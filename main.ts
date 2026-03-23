// main.ts - Updated with progress tracking + Main Instance integration
import express from 'express';
import path from 'path';
import { fileURLToPath } from 'url';
import { performScrape, ScrapeResult } from './modules/scraper.service.js';
import {
  processAndStoreVideo,
  getDb,
  createJob,
  getJob,
  updateJob,
  listJobs,
  JobFailureReason,
} from './modules/storage.service.js';
import { PORT, MAIN_INSTANCE } from './modules/config.js';

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

// ── Job processor with progress tracking ────────────────────────────────────
async function processJob(jobId: string): Promise<void> {
  const job = await getJob(jobId);
  if (!job) return;

  const streamId = job.url.split('/v/')[1]?.split('/')[0] ||
    job.url.split('/e/')[1]?.split('/')[0] ||
    Math.random().toString(36).substring(7);

  try {
    // ── Step 1: Scrape ───────────────────────────────────────────────────
    await updateJob(jobId, { 
      status: 'scraping',
      progress: { stage: 'downloading', percent: 5 }
    });
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
      await updateJob(jobId, { 
        progress: { stage: 'downloading', percent: 15 } 
      });
      
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

    // ── Step 2: Store with progress tracking ────────────────────────────
    await updateJob(jobId, { 
      status: 'storing',
      progress: { stage: 'downloading', percent: 25 }
    });

    const storageResult = await processAndStoreVideo(
      videoUrl,
      jobId,
      job.title || scrapeResult.title,
      async () => {
        console.log(`[JOB:${jobId}] Token expired — re-scraping...`);
        await enforceHostCooldown(originalPageUrl);
        const fresh = await performScrape(originalPageUrl, streamId);
        const freshUrl = fresh.videos[0]?.url;
        if (!freshUrl) throw new Error('Re-scrape returned no video URLs');
        return freshUrl;
      },
      // Progress callback
      async (progress) => {
        await updateJob(jobId, { progress });
      }
    );

    if (!storageResult.success) {
      const failureReason: JobFailureReason =
        storageResult.error?.includes('expired') ? 'expired_url' : 'upload_failed';

      await updateJob(jobId, {
        status: 'failed',
        failureReason,
        error: storageResult.error,
      });
      return;
    }

    // ── Step 3: Fetch video metadata from main instance ──────────────────
    let playUrl: string | undefined;
    if (storageResult.hash) {
      try {
        console.log(`[JOB:${jobId}] Fetching video info from main instance...`);
        const fileInfoUrl = `${MAIN_INSTANCE.url}/api/public/file/${storageResult.hash}`;
        const fileResponse = await fetch(fileInfoUrl, {
          headers: {
            'Authorization': `Bearer ${MAIN_INSTANCE.token}`,
          }
        });
        
        if (fileResponse.ok) {
          const fileData = await fileResponse.json();
          // ✅ Only set if we got a valid signed URL
          if (fileData.download?.url) {
            playUrl = fileData.download.url;
            console.log(`[JOB:${jobId}] ✓ Got signed URL from main instance`);
          } else {
            console.warn(`[JOB:${jobId}] No signed URL in response, will use API endpoint`);
          }
        } else {
          console.warn(`[JOB:${jobId}] Failed to get file info: ${fileResponse.status}`);
        }
      } catch (e: any) {
        console.error(`[JOB:${jobId}] Failed to fetch file info: ${e.message}`);
      }
    }

    await updateJob(jobId, {
      status: 'done',
      progress: { stage: 'complete', percent: 100 },
      result: {
        title: job.title || scrapeResult.title,
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
      progress: job.progress,
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
      progress: j.progress,
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

// ── Videos gallery (fetch from main instance) ──────────────────────────────
app.get('/api/videos', async (req, res): Promise<any> => {
  const authHeader = req.headers['authorization'];
  if (authHeader !== process.env.DASHBOARD_PIN) {
    return res.status(403).json({ error: 'Unauthorized access' });
  }

  try {
    // Check if token is configured
    if (!MAIN_INSTANCE.token) {
      console.error('[VIDEOS] MAIN_INSTANCE_TOKEN not configured');
      return res.status(500).json({ error: 'Main instance token not configured' });
    }

    // Fetch videos from main instance using PUBLIC API with token auth
    console.log(`[VIDEOS] Fetching from main instance: ${MAIN_INSTANCE.url}/api/public/files`);
    const mainResponse = await fetch(`${MAIN_INSTANCE.url}/api/public/files`, {
      headers: {
        'Authorization': `Bearer ${MAIN_INSTANCE.token}`,
      }
    });

    if (!mainResponse.ok) {
      console.error(`[VIDEOS] Failed to fetch from main instance: ${mainResponse.status}`);
      if (mainResponse.status === 401) {
        return res.status(500).json({ error: 'Main instance token invalid or expired' });
      }
      return res.status(500).json({ error: 'Failed to fetch videos from main instance' });
    }

    const mainData = await mainResponse.json();
    const files = mainData.files || [];

    if (!files || files.length === 0) {
      return res.json([]);
    }

    // Map main instance format to gallery format
    const videoList = files.map((file: any) => ({
      _id: file.hash,
      hash: file.hash,
      filename: file.filename,
      title: file.title || file.filename,
      fileSize: file.size,
      processedAt: file.created_at,
      type: 'mp4',
      playUrl: `${MAIN_INSTANCE.url}/api/public/file/${file.hash}`, // Will fetch signed URL when needed
    }));

    // Enhance with signed URLs from main instance
    const videoListWithUrls = await Promise.all(videoList.map(async (video: any) => {
      try {
        const fileResponse = await fetch(
          `${MAIN_INSTANCE.url}/api/public/file/${video.hash}`,
          {
            headers: {
              'Authorization': `Bearer ${MAIN_INSTANCE.token}`,
            }
          }
        );
        if (fileResponse.ok) {
          const fileData = await fileResponse.json();
          // ✅ Only update if we got a valid signed URL
          if (fileData.download?.url) {
            video.playUrl = fileData.download.url;
            console.log(`[VIDEOS] Got signed URL for ${video.hash.substring(0, 8)}...`);
          } else {
            console.warn(`[VIDEOS] No signed URL in response for ${video.hash.substring(0, 8)}... using API endpoint`);
          }
        } else {
          console.warn(`[VIDEOS] Failed to fetch file info for ${video.hash.substring(0, 8)}...: ${fileResponse.status}`);
        }
      } catch (e: any) {
        console.warn(`[VIDEOS] Error fetching signed URL for ${video.hash.substring(0, 8)}...: ${e.message}`);
      }
      return video;
    }));

    return res.json(videoListWithUrls);
  } catch (error: any) {
    console.error('[API ERROR]', error);
    return res.status(500).json({ error: error.message });
  }
});

// ── Static ─────────────────────────────────────────────────────────────────
app.get('/gallery', (req, res) => {
  res.sendFile(path.join(__dirname, '../public/index.html'));
});

app.get('/health', (req, res) => {
  res.json({ status: 'ok', timestamp: new Date() });
});

app.listen(PORT, '0.0.0.0', async () => {
  console.log(`🚀 Server running at http://localhost:${PORT}`);
  console.log(`🎥 Gallery: http://localhost:${PORT}/gallery`);
  console.log(`📡 Main Instance: ${MAIN_INSTANCE.url}`);

  try {
    await getDb();
    console.log(`✅ Connected to MongoDB`);
  } catch (err) {
    console.error('FATAL: Could not initialize. Process exiting.');
    process.exit(1);
  }
});