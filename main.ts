import express from 'express';
import pLimit from 'p-limit';
import { performScrape } from './modules/scraper.service.js';
import { processAndStoreVideo } from './modules/storage.service.js'; // Import your storage service
import { PORT, CONCURRENCY_LIMIT } from './modules/config.js';

const app = express();
app.use(express.json());
const limit = pLimit(CONCURRENCY_LIMIT);

app.post('/api/scrape', async (req, res) => {
  const { url, title } = req.body;
  const streamId = Math.random().toString(36).substring(7);

  if (!url) return res.status(400).json({ error: 'URL required' });

  try {
    // 1. Perform Scraping (wrapped in pLimit)
    let scrapeResult = await limit(() => performScrape(url, streamId));

    // Retry Logic if no videos found
    if (scrapeResult.videos.length === 0) {
      console.log(`[RETRY] ID: ${streamId}`);
      await new Promise(r => setTimeout(r, 5000));
      scrapeResult = await limit(() => performScrape(url, streamId));
    }

    // If still no videos found after retry, return 202
    if (scrapeResult.videos.length === 0) {
      return res.status(202).json({ 
        error: 'No videos found after retries', 
        retryAfter: 300 
      });
    }

    // 2. Video found, now try to Download and Upload to R2
    // We take the first video URL found by the scraper
    const targetVideoUrl = scrapeResult.videos[0].url;
    const storageResult = await processAndStoreVideo(targetVideoUrl, title || `Scraped_${streamId}`);

    if (storageResult.success) {
      // Return 200 if both scrape and upload succeeded
      return res.status(200).json({
        message: 'Successfully scraped and uploaded',
        hash: storageResult.hash,
        r2Key: storageResult.r2Key,
        ...scrapeResult
      });
    } else {
      // Return 304 if scraped but upload failed
      // Note: 304 usually has no body, but Express allows sending JSON with it
      return res.status(304).json({
        error: 'Video scraped but failed to upload to storage',
        details: storageResult.error,
        scrapeResult
      });
    }

  } catch (error: any) {
    res.status(500).json({ error: error.message });
  }
});

app.listen(PORT, '0.0.0.0', () => console.log(`🚀 Scraper ready on port ${PORT}`));