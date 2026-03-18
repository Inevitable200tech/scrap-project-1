// main.ts
import express from 'express';
import pLimit from 'p-limit';
import path from 'path';
import { fileURLToPath } from 'url';
import { performScrape, ScrapeResult } from './modules/scraper.service.js';
import {
  processAndStoreVideo,
  getDb,
  s3Client,
  createJob,
  getJob,
  updateJob,
  listJobs,
} from './modules/storage.service.js';
import { GetObjectCommand } from "@aws-sdk/client-s3";
import { getSignedUrl } from "@aws-sdk/s3-request-presigner";
import { PORT, CONCURRENCY_LIMIT, R2_CONFIG } from './modules/config.js';

const app = express();
app.use(express.json());

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
app.use(express.static(path.join(__dirname, '../public')));

const limit = pLimit(CONCURRENCY_LIMIT);

// ── Background processor ───────────────────────────────────────────────────
async function processJob(jobId: string): Promise<void> {
  const job = await getJob(jobId);
  if (!job) return;

  const streamId = job.url.split('/v/')[1]?.split('/')[0] ||
    job.url.split('/e/')[1]?.split('/')[0] ||
    Math.random().toString(36).substring(7);

  try {
    // ── Step 1: Scrape ───────────────────────────────────────────────────
    await updateJob(jobId, { status: 'scraping' });
    let scrapeResult: ScrapeResult = await limit(() => performScrape(job.url, streamId));

    if (!scrapeResult?.videos?.length) {
      console.log(`[JOB:${jobId}] No videos on first attempt, retrying in 60s...`);
      await new Promise(r => setTimeout(r, 60000));
      scrapeResult = await limit(() => performScrape(job.url, streamId));
    }

    if (!scrapeResult?.videos?.length) {
      await updateJob(jobId, { status: 'failed', error: 'No videos found after retry' });
      return;
    }

    const videoUrl = scrapeResult.videos[0].url;
    const originalPageUrl = scrapeResult.originalUrl;

    // ── Step 2: Store ────────────────────────────────────────────────────
    await updateJob(jobId, { status: 'storing' });
    const storageResult = await processAndStoreVideo(
      videoUrl,
      job.title || scrapeResult.title,
      async () => {
        console.log(`[JOB:${jobId}] Token expired — re-scraping...`);
        const fresh = await performScrape(originalPageUrl, streamId);
        const freshUrl = fresh.videos[0]?.url;
        if (!freshUrl) throw new Error('Re-scrape returned no video URLs');
        return freshUrl;
      }
    );

    if (!storageResult.success) {
      await updateJob(jobId, { status: 'failed', error: storageResult.error });
      return;
    }

    // ── Step 3: Generate signed play URL ─────────────────────────────────
    let playUrl: string | undefined;
    if (storageResult.r2Key) {
      try {
        const command = new GetObjectCommand({
          Bucket: R2_CONFIG.bucket,
          Key: storageResult.r2Key,
        });
        playUrl = await getSignedUrl(s3Client, command, { expiresIn: 3600 });
      } catch (e: any) {
        console.error(`[JOB:${jobId}] Failed to generate play URL: ${e.message}`);
      }
    }

    await updateJob(jobId, {
      status: 'done',
      result: {
        title: job.title || scrapeResult.title,
        r2Key: storageResult.r2Key,
        hash: storageResult.hash,
        isDuplicate: storageResult.isDuplicate,
        playUrl,
      },
    });

    console.log(`[JOB:${jobId}] Completed successfully`);

  } catch (error: any) {
    console.error(`[JOB:${jobId}] Failed: ${error.message}`);
    await updateJob(jobId, { status: 'failed', error: error.message });
  }
}

// ── Routes ─────────────────────────────────────────────────────────────────

/**
 * POST /api/scrape
 * Submit a URL for processing. Returns jobId immediately.
 * Body: { url: string, title?: string }
 */
app.post('/api/scrape', async (req, res): Promise<any> => {
  const { url, title } = req.body;

  if (!url || /luluvdo|luluvid/i.test(url)) {
    return res.status(400).json({ error: 'URL required or provider not supported' });
  }

  try {
    const job = await createJob(url, title);

    // Fire and forget
    processJob(job.jobId).catch((err) => {
      console.error(`[JOB:${job.jobId}] Unhandled error: ${err.message}`);
    });

    return res.status(202).json({
      jobId: job.jobId,
      status: 'pending',
      pollUrl: `/api/scrape/status/${job.jobId}`,
    });
  } catch (error: any) {
    return res.status(500).json({ error: error.message });
  }
});

/**
 * GET /api/scrape/status/:jobId
 * Poll the status of a submitted job.
 */
app.get('/api/scrape/status/:jobId', async (req, res): Promise<any> => {
  try {
    const job = await getJob(req.params.jobId);

    if (!job) {
      return res.status(404).json({ error: 'Job not found' });
    }

    const response: Record<string, any> = {
      jobId: job.jobId,
      status: job.status,
      createdAt: job.createdAt,
      updatedAt: job.updatedAt,
    };

    if (job.status === 'done')   response.result = job.result;
    if (job.status === 'failed') response.error  = job.error;

    return res.json(response);
  } catch (error: any) {
    return res.status(500).json({ error: error.message });
  }
});

/**
 * GET /api/scrape/jobs
 * List recent jobs.
 */
app.get('/api/scrape/jobs', async (req, res): Promise<any> => {
  try {
    const allJobs = await listJobs();
    return res.json(allJobs.map(j => ({
      jobId: j.jobId,
      url: j.url,
      status: j.status,
      createdAt: j.createdAt,
      updatedAt: j.updatedAt,
      error: j.error,
    })));
  } catch (error: any) {
    return res.status(500).json({ error: error.message });
  }
});

// ── Auth ───────────────────────────────────────────────────────────────────
app.post('/api/login', (req, res): any => {
  const { pin } = req.body;
  const correctPin = process.env.DASHBOARD_PIN;

  console.log(`[AUTH] Received: "${pin}" | Expected: "${correctPin}"`);

  if (!correctPin) {
    console.error('[AUTH ERROR] DASHBOARD_PIN is not set in Environment Variables!');
    return res.status(500).json({ error: 'Server configuration error' });
  }

  if (String(pin).trim() === String(correctPin).trim()) {
    return res.json({ success: true });
  }
  return res.status(401).json({ error: 'Invalid PIN' });
});

// ── Videos gallery ─────────────────────────────────────────────────────────
app.get('/api/videos', async (req, res): Promise<any> => {
  const authHeader = req.headers['authorization'];

  if (authHeader !== process.env.DASHBOARD_PIN) {
    return res.status(403).json({ error: 'Unauthorized access' });
  }

  try {
    const db = await getDb();
    const videos = await db.collection('videos')
      .find()
      .sort({ processedAt: -1 })
      .limit(50)
      .toArray();

    if (!videos || videos.length === 0) return res.json([]);

    const playableVideos = await Promise.all(videos.map(async (video) => {
      try {
        const command = new GetObjectCommand({ Bucket: R2_CONFIG.bucket, Key: video.r2Key });
        const signedUrl = await getSignedUrl(s3Client, command, { expiresIn: 3600 });
        return { ...video, playUrl: signedUrl };
      } catch (e) {
        return { ...video, playUrl: null, error: 'Link failed' };
      }
    }));

    return res.json(playableVideos);
  } catch (error: any) {
    console.error('[API ERROR]', error);
    return res.status(500).json({ error: error.message });
  }
});

// ── Static ─────────────────────────────────────────────────────────────────
app.get('/gallery', (req, res) => {
  res.sendFile(path.join(__dirname, '../public/index.html'));
});

app.listen(PORT, '0.0.0.0', () =>
  console.log(`🚀 Server running at http://localhost:${PORT}`)
);