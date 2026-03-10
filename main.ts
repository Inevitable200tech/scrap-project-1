import express, { Request, Response } from 'express';
import { exec } from 'child_process';
import { promisify } from 'util';
import * as cheerio from 'cheerio';
import playwrightExtra from 'playwright-extra';
import StealthPlugin from 'puppeteer-extra-plugin-stealth';
import pLimit from 'p-limit'; // Install this: npm install p-limit
import * as os from 'os';
import * as path from 'path';
import fs from 'fs';

const execPromise = promisify(exec);
playwrightExtra.chromium.use(StealthPlugin());

const app = express();
app.use(express.json());

// IMPORTANT: Limit concurrency for Docker/Render resources
// This ensures we only run 5 yt-dlp checks at a time, preventing CPU/RAM spikes.
const limit = pLimit(5);

async function getMediaInfo(url: string) {
  try {
    // --flat-playlist: extremely important for speed (doesn't scrape every video in a list)
    // --no-warnings: keeps the output clean for JSON parsing
    const { stdout } = await execPromise(`yt-dlp -j --no-warnings --flat-playlist "${url}"`);
    return JSON.parse(stdout);
  } catch (e) {
    return null;
  }
}

app.post('/api/scrape', async (req: Request, res: Response) => {
  const { url } = req.body;
  if (!url) return res.status(400).json({ error: 'URL is required' });

  // Extract ID for logging (e.g., vpqzjKKXaPu4ea0)
  const streamId = url.split('/v/')[1]?.split('/')[0] || 'unknown';
  
  let browser;
  try {
    console.log(`[START] [ID: ${streamId}] Scrape Request for: ${url}`);
    const userDataDir = path.join(os.tmpdir(), 'dropmms-api-profile');

    if (fs.existsSync(path.join(userDataDir, 'SingletonLock'))) {
      try { 
        fs.unlinkSync(path.join(userDataDir, 'SingletonLock')); 
        console.log(`[DEBUG] [ID: ${streamId}] Cleaned up SingletonLock file.`);
      } catch (e) { console.log(`[WARN] [ID: ${streamId}] Could not remove lock file.`); }
    }

    browser = await playwrightExtra.chromium.launchPersistentContext(
      userDataDir,
      {
        headless: true,
        args: [
          '--no-sandbox',
          '--disable-dev-shm-usage',
          '--no-zygote',
          '--disable-setuid-sandbox',
          '--disable-infobars',
          '--window-size=1280,900',
          '--disable-blink-features=AutomationControlled',
        ],
        viewport: { width: 1280, height: 900 },
        userAgent: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        locale: 'en-US',
        timezoneId: 'Asia/Kolkata',
        bypassCSP: true,
        javaScriptEnabled: true,
        ignoreHTTPSErrors: true,
      }
    );

    const page = await browser.newPage();
    const interceptedVideos: any[] = [];

    // Close any pop-up ads/tabs immediately (The "Anti-Ad" Monitor)
    browser.on('page', async popup => {
      console.log(`[ID: ${streamId}] [AD-BLOCK] Closing popup: ${popup.url()}`);
      await popup.close().catch(() => {});
    });

    // --- REFINED NETWORK INTERCEPTOR ---
    page.on('request', request => {
      const reqUrl = request.url();
      const isTracker = /yandex|mc\.ru|analytics|pixel|doubleclick|google/i.test(reqUrl);
      
      if (!isTracker && (reqUrl.includes('get_video') || reqUrl.includes('.m3u8') || reqUrl.includes('.mp4'))) {
        // Filter out the original page URL if it mimics a video file extension
        if (reqUrl !== url && !interceptedVideos.some(v => v.url === reqUrl)) {
          console.log(`[ID: ${streamId}] [CATCH] Valid stream: ${reqUrl.substring(0, 60)}...`);
          interceptedVideos.push({ site: 'Network-Stream', title: 'Captured Media', url: reqUrl });
        }
      }
    });

    console.log(`[ID: ${streamId}] [DEBUG] Navigating to page...`);
    try {
      await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 30000 });
    } catch (e: any) {
      console.error(`[ID: ${streamId}] [TIMEOUT] Goto timed out, proceeding to interaction.`);
    }

    // --- INTERACTION LOGIC (The Double-Click Fix) ---
    console.log(`[ID: ${streamId}] [DEBUG] Attempting to trigger video player...`);
    try {
      const playBtn = '.plyr__control--overlaid, .play-overlay, button[data-plyr="play"]';
      await page.waitForSelector(playBtn, { timeout: 10000 });
      
      // First click often triggers a pop-under on Streamtape
      await page.click(playBtn, { force: true });
      console.log(`[ID: ${streamId}] [DEBUG] First click sent.`);
      await page.waitForTimeout(1500);

      // Second click/JS dispatch to ensure video actually starts
      await page.evaluate((sel) => {
        const btn = document.querySelector(sel) as HTMLElement;
        if (btn) btn.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      }, playBtn);
      console.log(`[ID: ${streamId}] [DEBUG] JS click dispatched.`);

      // Wait for source to appear in DOM
      await page.waitForFunction(() => {
        const v = document.querySelector('#mainvideo') as HTMLVideoElement;
        return v && v.src && v.src.includes('get_video');
      }, { timeout: 15000 }).catch(() => console.log(`[ID: ${streamId}] [DEBUG] DOM src wait timed out.`));
    } catch (err) {
      console.log(`[ID: ${streamId}] [WARN] Interaction failed.`);
    }

    // --- PLYR DOM EXTRACTION ---
    const plyrVideo = await page.evaluate(() => {
      const v = document.querySelector('#mainvideo') as HTMLVideoElement;
      if (!v) return null;
      const absoluteUrl = v.currentSrc || v.src;
      return absoluteUrl ? { url: absoluteUrl, title: document.title } : null;
    });

    if (plyrVideo && plyrVideo.url && plyrVideo.url !== url) {
        console.log(`[ID: ${streamId}] [SUCCESS] Found in DOM: ${plyrVideo.url.substring(0, 50)}...`);
        interceptedVideos.push({ site: 'Plyr-DOM', title: plyrVideo.title, url: plyrVideo.url });
    }

    const html = await page.content();
    const $ = cheerio.load(html);
    const title = await page.title();

    const videoLinks: any[] = [...interceptedVideos];
    const images: string[] = [];
    const linksFound = new Set<string>();

    $('a[href^="http"]').each((_, el) => {
      const href = $(el).attr('href');
      if (href && href !== url) linksFound.add(href);
    });

    // Probing links
    console.log(`[ID: ${streamId}] [INFO] Probing ${linksFound.size} secondary links...`);
    /**
     * Creates an array of rate-limited async tasks that fetch media information for discovered links.
     * Each task retrieves metadata (title, thumbnail, extractor info) for a given link and,
     * if successful, adds the formatted video details to the videoLinks collection.
     * Failed requests are silently ignored and do not interrupt the overall process.
     */
    const checkTasks = Array.from(linksFound).map((link) =>
      limit(async () => {
        const info = await getMediaInfo(link).catch(() => null);
        if (info) {
          videoLinks.push({
            site: info.extractor_key,
            title: info.title,
            url: info.url || info.webpage_url,
            thumbnail: info.thumbnail
          });
        }
      })
    );
    await Promise.all(checkTasks);

    // Image Extraction
    $('img').each((_, el) => {
      const src = $(el).attr('src') || $(el).attr('data-src');
      if (src) {
        try {
          const absolute = new URL(src, url).href;
          if (/\.(jpg|jpeg|png|gif|webp|avif)($|\?)/i.test(absolute)) {
            if (!images.includes(absolute)) images.push(absolute);
          }
        } catch (e) { }
      }
    });

    await browser.close();
    
    // Final Filtering
    const finalVideos = Array.from(new Map(
        videoLinks
          .filter(v => !/yandex|mc\.ru|analytics/i.test(v.url) && v.url !== url)
          .map(v => [v.url, v])
      ).values());

    console.log(`[ID: ${streamId}] [COMPLETE] Returned ${finalVideos.length} videos.`);
    res.json({ title, videos: finalVideos, images });

  } catch (error: any) {
    if (browser) await browser.close();
    console.error(`[ID: ${streamId}] [CRITICAL ERROR] ${error.message}`);
    res.status(500).json({ error: error.message });
  }
});

app.get('/health', (req: Request, res: Response) => {
  res.json({ status: 'ok', timestamp: new Date().toISOString() })
});

const PORT = parseInt(process.env.PORT || '3000', 10);
app.listen(PORT, '0.0.0.0', () => {
  console.log(`\n🚀 Server running in Docker on port ${PORT}`);
  console.log(`Using yt-dlp for universal video extraction\n`);
});