import express from 'express';
import pLimit from 'p-limit';
import { performScrape } from './modules/scraper.service.js';
import { processAndStoreVideo, getDb, s3Client } from './modules/storage.service.js'; 
import { GetObjectCommand } from "@aws-sdk/client-s3";
import { getSignedUrl } from "@aws-sdk/s3-request-presigner";
import { PORT, CONCURRENCY_LIMIT, R2_CONFIG } from './modules/config.js';

const app = express();
app.use(express.json());
const limit = pLimit(CONCURRENCY_LIMIT);

/**
 * NEW: Endpoint to see all videos and get playable links
 */
app.get('/api/videos', async (req, res) => {
  try {
    const db = await getDb();
    const videos = await db.collection('videos').find().sort({ processedAt: -1 }).toArray();

    // Generate temporary playable links for each video
    const playableVideos = await Promise.all(videos.map(async (video) => {
      const command = new GetObjectCommand({
        Bucket: R2_CONFIG.bucket,
        Key: video.r2Key,
      });

      // URL expires in 1 hour
      const signedUrl = await getSignedUrl(s3Client, command, { expiresIn: 3600 });

      return {
        ...video,
        playUrl: signedUrl
      };
    }));

    res.json(playableVideos);
  } catch (error: any) {
    res.status(500).json({ error: error.message });
  }
});

// Existing Scrape Endpoint
app.post('/api/scrape', async (req, res) => {
  const { url, title } = req.body;
  const streamId = Math.random().toString(36).substring(7);

  if (!url) return res.status(400).json({ error: 'URL required' });

  try {
    let scrapeResult = await limit(() => performScrape(url, streamId));

    if (scrapeResult.videos.length === 0) {
      await new Promise(r => setTimeout(r, 5000));
      scrapeResult = await limit(() => performScrape(url, streamId));
    }

    if (scrapeResult.videos.length === 0) {
      return res.status(202).json({ error: 'No videos found', retryAfter: 300 });
    }

    const targetVideoUrl = scrapeResult.videos[0].url;
    const storageResult = await processAndStoreVideo(targetVideoUrl, title || `Scraped_${streamId}`);

    if (storageResult.success) {
      res.status(200).json({
        message: 'Successfully scraped and uploaded',
        hash: storageResult.hash,
        r2Key: storageResult.r2Key,
        ...scrapeResult
      });
    } else {
      res.status(304).json({ error: 'Upload failed', details: storageResult.error });
    }
  } catch (error: any) {
    res.status(500).json({ error: error.message });
  }
});

app.listen(PORT, '0.0.0.0', () => console.log(`🚀 Scraper ready on port ${PORT}`));