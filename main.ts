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

// Login endpoint to verify PIN
app.post('/api/login', (req, res) => {
  const { pin } = req.body;
  const correctPin = process.env.DASHBOARD_PIN;

  if (!correctPin) {
    return res.status(500).json({ error: "Server PIN not configured" });
  }

  if (pin === correctPin) {
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
    const videos = await db.collection('videos').find().sort({ processedAt: -1 }).limit(50).toArray();

    const playableVideos = await Promise.all(videos.map(async (video) => {
      const command = new GetObjectCommand({ Bucket: R2_CONFIG.bucket, Key: video.r2Key });
      const signedUrl = await getSignedUrl(s3Client, command, { expiresIn: 3600 });
      return { ...video, playUrl: signedUrl };
    }));

    res.json(playableVideos);
  } catch (error: any) {
    res.status(500).json({ error: error.message });
  }
});

// Serve the dashboard HTML for any other route
app.get('/gallery', (req, res) => {
  res.sendFile(path.join(__dirname, '../public/index.html'));
});

app.listen(PORT, '0.0.0.0', () => console.log(`🚀 Server running at http://localhost:${PORT}`));