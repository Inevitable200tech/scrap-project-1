// main.ts
import express from 'express';
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
  cleanupIncompleteMultipartUploads,
  JobFailureReason,
} from './modules/storage.service.js';
import { GetObjectCommand } from "@aws-sdk/client-s3";
import { getSignedUrl } from "@aws-sdk/s3-request-presigner";
import { PORT, R2_CONFIG } from './modules/config.js';

const app = express();
app.use(express.json());

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
app.use(express.static(path.join(__dirname, '../public')));

// ── Serial job queue ───────────────────────────────────────────────────────
const jobQueue: string[] = [];
let isProcessing = false;

async function enqueueJob(jobId: string): Promise<void> {
  jobQueue.push(jobId);
  console.log(`[QUEUE] Job ${jobId} enqueued — position ${jobQueue.length}`);
  if (!isProcessing) {
    processNextJob();
  }
}

async function processNextJob(): Promise<void> {
  if (jobQueue.length === 0) {
    isProcessing = false;
    console.log('[QUEUE] Empty — processor idle');
    return;
  }

  isProcessing = true;
  const jobId = jobQueue.shift()!;
  console.log(`[QUEUE] Starting job ${jobId} — ${jobQueue.length} remaining`);

  try {
    await processJob(jobId);
  } catch (err: any) {
    console.error(`[QUEUE] Job ${jobId} threw unhandled error: ${err.message}`);
  }

  await processNextJob();
}

// ── Per-host cooldown ──────────────────────────────────────────────────────
const hostLastScraped = new Map<string, number>();
const HOST_COOLDOWN_MS = 10_000;

async function enforceHostCooldown(url: string): Promise<void> {
  try {
    const hostname = new URL(url).hostname;
    const last = hostLastScraped.get(hostname) ?? 0;
    const elapsed = Date.now() - last;
    if (elapsed < HOST_COOLDOWN_MS) {
      const wait = HOST_COOLDOWN_MS - elapsed;
      console.log(`[COOLDOWN] ${hostname} — waiting ${Math.round(wait / 1000)}s`);
      await new Promise(r => setTimeout(r, wait));
    }
    hostLastScraped.set(hostname, Date.now());
  } catch {}
}

// ── Job processor ──────────────────────────────────────────────────────────
async function processJob(jobId: string): Promise<void> {
  const job = await getJob(jobId);
  if (!job) return;

  const streamId = job.url.split('/v/')[1]?.split('/')[0] ||
    job.url.split('/e/')[1]?.split('/')[0] ||
    Math.random().toString(36).substring(7);

  try {
    // ── Step 1: Scrape ───────────────────────────────────────────────────
    await updateJob(jobId, { status: 'scraping' });
    await enforceHostCooldown(job.url);

    let scrapeResult: ScrapeResult;
    try {
      scrapeResult = await performScrape(job.url, streamId);
    } catch (scrapeErr: any) {
      await updateJob(jobId, {
        status: 'failed',
        failureReason: 'scrape_error',
        error: scrapeErr.message,
      });
      return;
    }

    // Dead video — confirmed gone at source
    if (scrapeResult.dead) {
      await updateJob(jobId, {
        status: 'failed',
        failureReason: 'dead_video',
        error: `Video has been removed (matched: ${scrapeResult.deadReason ?? 'unknown'})`,
      });
      return;
    }

    // No video URL found on first attempt — retry once
    if (!scrapeResult.videos?.length) {
      console.log(`[JOB:${jobId}] No videos on first attempt, retrying in 30s...`);
      await new Promise(r => setTimeout(r, 30_000));

      let retryResult: ScrapeResult;
      try {
        retryResult = await performScrape(job.url, streamId);
      } catch (retryErr: any) {
        await updateJob(jobId, {
          status: 'failed',
          failureReason: 'scrape_error',
          error: retryErr.message,
        });
        return;
      }

      if (retryResult.dead) {
        await updateJob(jobId, {
          status: 'failed',
          failureReason: 'dead_video',
          error: `Video has been removed (matched: ${retryResult.deadReason ?? 'unknown'})`,
        });
        return;
      }

      if (!retryResult.videos?.length) {
        await updateJob(jobId, {
          status: 'failed',
          failureReason: 'no_video_found',
          error: 'No video URL found after two scrape attempts',
        });
        return;
      }

      scrapeResult = retryResult;
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
        await enforceHostCooldown(originalPageUrl);
        const fresh = await performScrape(originalPageUrl, streamId);
        const freshUrl = fresh.videos[0]?.url;
        if (!freshUrl) throw new Error('Re-scrape returned no video URLs');
        return freshUrl;
      }
    );

    if (!storageResult.success) {
      const failureReason: JobFailureReason =
        storageResult.error?.includes('expired') ? 'expired_url' : 'ffmpeg_failed';

      await updateJob(jobId, {
        status: 'failed',
        failureReason,
        error: storageResult.error,
      });
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

    console.log(`[JOB:${jobId}] ✓ Completed — queue: ${jobQueue.length} remaining`);

  } catch (error: any) {
    console.error(`[JOB:${jobId}] Failed: ${error.message}`);
    await updateJob(jobId, {
      status: 'failed',
      failureReason: 'unknown',
      error: error.message,
    });
  }
}

// ── Routes ─────────────────────────────────────────────────────────────────
app.post('/api/scrape', async (req, res): Promise<any> => {
  const { url, title } = req.body;

  if (!url || /luluvdo|luluvid/i.test(url)) {
    return res.status(400).json({ error: 'URL required or provider not supported' });
  }

  try {
    const job = await createJob(url, title);
    await enqueueJob(job.jobId);

    return res.status(202).json({
      jobId: job.jobId,
      status: 'pending',
      queuePosition: jobQueue.length,
      pollUrl: `/api/scrape/status/${job.jobId}`,
    });
  } catch (error: any) {
    return res.status(500).json({ error: error.message });
  }
});

app.get('/api/scrape/status/:jobId', async (req, res): Promise<any> => {
  try {
    const job = await getJob(req.params.jobId);
    if (!job) return res.status(404).json({ error: 'Job not found' });

    const queuePosition = jobQueue.indexOf(req.params.jobId);

    const response: Record<string, any> = {
      jobId: job.jobId,
      status: job.status,
      createdAt: job.createdAt,
      updatedAt: job.updatedAt,
      queuePosition: queuePosition >= 0 ? queuePosition + 1 : null,
    };

    if (job.status === 'done') {
      response.result = job.result;
    }

    if (job.status === 'failed') {
      response.error         = job.error;
      response.failureReason = job.failureReason ?? 'unknown';
    }

    return res.json(response);
  } catch (error: any) {
    return res.status(500).json({ error: error.message });
  }
});

app.get('/api/scrape/jobs', async (req, res): Promise<any> => {
  try {
    const allJobs = await listJobs();
    return res.json(allJobs.map(j => ({
      jobId: j.jobId,
      url: j.url,
      status: j.status,
      failureReason: j.failureReason,
      createdAt: j.createdAt,
      updatedAt: j.updatedAt,
      error: j.error,
    })));
  } catch (error: any) {
    return res.status(500).json({ error: error.message });
  }
});

app.get('/api/queue', (req, res) => {
  res.json({
    isProcessing,
    pending: jobQueue.length,
    jobs: jobQueue.map((id, i) => ({ jobId: id, position: i + 1 })),
  });
});

// ── Auth ───────────────────────────────────────────────────────────────────
app.post('/api/login', (req, res): any => {
  const { pin } = req.body;
  const correctPin = process.env.DASHBOARD_PIN;

  console.log(`[AUTH] Received: "${pin}" | Expected: "${correctPin}"`);

  if (!correctPin) {
    console.error('[AUTH ERROR] DASHBOARD_PIN is not set');
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

app.listen(PORT, '0.0.0.0', async () => {
  console.log(`🚀 Server running at http://localhost:${PORT}`);

  try {
    await getDb();

    cleanupIncompleteMultipartUploads().catch(console.error);
    setInterval(() => {
      cleanupIncompleteMultipartUploads().catch(console.error);
    }, 6 * 60 * 60 * 1000);

  } catch (err) {
    console.error('FATAL: Could not initialize. Process exiting.');
    process.exit(1);
  }
});