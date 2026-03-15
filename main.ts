import express from 'express';
import pLimit from 'p-limit';
import path from 'path';
import { fileURLToPath } from 'url';
import { performScrape } from './modules/scraper.service.js';
import { processAndStoreVideo, getDb, s3Client } from './modules/storage.service.js';
import { GetObjectCommand } from "@aws-sdk/client-s3";
import { getSignedUrl } from "@aws-sdk/s3-request-presigner";
import { PORT, CONCURRENCY_LIMIT, R2_CONFIG } from './modules/config.js';

const app = express();
app.use(express.json());

// --- FRONTEND SETUP ---
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Serve static files from the 'public' folder
app.use(express.static(path.join(__dirname, '../public')));

const limit = pLimit(CONCURRENCY_LIMIT);

// API to list videos with temporary playable links
// Add this near your other routes in main.ts

// Main Scraper Endpoint
app.post('/api/scrape', async (req, res) => {
  const { url, title } = req.body;
  if (!url || /luluvdo|luluvid/i.test(url)) {
    return res.status(400).json({ error: 'URL required or provider not supported' });
  }

  const streamId = url.split('/v/')[1]?.split('/')[0] || Math.random().toString(36).substring(7);

  try {
    let result = await limit(() => performScrape(url, streamId));

    if (result.videos.length === 0) {
      console.log(`[RETRY] No videos found for ${streamId}, waiting 60s...`);
      await new Promise(r => setTimeout(r, 60000));
      result = await limit(() => performScrape(url, streamId));
    }

    if (result.videos.length === 0) {
      return res.status(202).json({ error: 'No videos found', retryAfter: 300 });
    }

    const storageResult = await processAndStoreVideo(result.videos[0].url, title || result.title);

    res.status(storageResult.success ? 200 : 304).json({
      ...result,
      storage: storageResult
    });
  } catch (error: any) {
    res.status(500).json({ error: error.message });
  }
});

// Login endpoint to verify PIN
app.post('/api/login', (req, res) => {
  const { pin } = req.body;
  const correctPin = process.env.DASHBOARD_PIN;

  // Debugging: This will show in Render logs what the server is comparing
  // Delete these logs once you confirm it works!
  console.log(`[AUTH] Received: "${pin}" | Expected: "${correctPin}"`);

  if (!correctPin) {
    console.error("[AUTH ERROR] DASHBOARD_PIN is not set in Environment Variables!");
    return res.status(500).json({ error: "Server configuration error" });
  }

  if (String(pin).trim() === String(correctPin).trim()) {
    res.json({ success: true });
  } else {
    res.status(401).json({ error: "Invalid PIN" });
  }
});

// Update your /api/videos route to check for a header
app.get('/api/videos', async (req, res) => {
  const authHeader = req.headers['authorization'];
  
  if (authHeader !== process.env.DASHBOARD_PIN) {
    return res.status(403).json({ error: "Unauthorized access" });
  }

  try {
    const db = await getDb();
    const videos = await db.collection('videos')
      .find()
      .sort({ processedAt: -1 })
      .limit(50)
      .toArray();

    // If no videos, return empty array immediately
    if (!videos || videos.length === 0) {
      return res.json([]);
    }

    const playableVideos = await Promise.all(videos.map(async (video) => {
      try {
        const command = new GetObjectCommand({ Bucket: R2_CONFIG.bucket, Key: video.r2Key });
        const signedUrl = await getSignedUrl(s3Client, command, { expiresIn: 3600 });
        return { ...video, playUrl: signedUrl };
      } catch (e) {
        return { ...video, playUrl: null, error: "Link failed" };
      }
    }));

    res.json(playableVideos);
  } catch (error: any) {
    console.error("[API ERROR]", error);
    res.status(500).json({ error: error.message }); // This returns an OBJECT, which causes the .forEach error
  }
});

// Serve the dashboard HTML for any other route
app.get('/gallery', (req, res) => {
  res.sendFile(path.join(__dirname, '../public/index.html'));
});

app.listen(PORT, '0.0.0.0', () => console.log(`🚀 Server running at http://localhost:${PORT}`));