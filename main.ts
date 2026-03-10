import express, { Request, Response } from 'express';
import { exec } from 'child_process';
import { promisify } from 'util';
import * as cheerio from 'cheerio';
import playwrightExtra from 'playwright-extra';
import StealthPlugin from 'puppeteer-extra-plugin-stealth';
import pLimit from 'p-limit'; // Install this: npm install p-limit
import * as os from 'os';
import * as path from 'path';

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

  let browser;
  try {
    console.log(`[DOCKER] Starting scrape for: ${url}`);
    const userDataDir = path.join(os.tmpdir(), 'dropmms-api-profile');
    console.log(`Persistent profile: ${userDataDir}`);

    browser = await playwrightExtra.chromium.launchPersistentContext(
      userDataDir,
      {
        headless: true,
        args: [
          '--no-sandbox',
          '--disable-setuid-sandbox',
          '--disable-infobars',
          '--window-size=1280,900',
          '--disable-blink-features=AutomationControlled',
        ],
        viewport: { width: 1280, height: 900 },
        userAgent:
          'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        locale: 'en-US',
        timezoneId: 'Asia/Kolkata',
        bypassCSP: true,
        javaScriptEnabled: true,
        ignoreHTTPSErrors: true,
      }
    );

    const page = await browser.newPage();
    await page.goto(url, { waitUntil: 'networkidle', timeout: 60000 });

    // Smooth scroll to bottom to load lazy images
    await page.evaluate(async () => {
      await new Promise((resolve) => {
        let totalHeight = 0;
        const distance = 100;
        const timer = setInterval(() => {
          const scrollHeight = document.body.scrollHeight;
          window.scrollBy(0, distance);
          totalHeight += distance;
          if (totalHeight >= scrollHeight) {
            clearInterval(timer);
            resolve(true);
          }
        }, 100);
      });
    });

    const html = await page.content();
    const $ = cheerio.load(html);
    const title = await page.title();

    const videoLinks: any[] = [];
    const images: string[] = [];
    const linksFound = new Set<string>();

    // Collect all unique links
    $('a[href^="http"]').each((_, el) => {
      const href = $(el).attr('href');
      if (href) linksFound.add(href);
    });

    console.log(`[INFO] Found ${linksFound.size} potential links. Checking with yt-dlp...`);

    // Use p-limit to check links without crashing the container
    const checkTasks = Array.from(linksFound).map((link) =>
      limit(async () => {
        const info = await getMediaInfo(link);
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

    // Image extraction (regex avoids non-image links)
    $('img, a').each((_, el) => {
      const src = $(el).attr('src') || $(el).attr('data-src') || $(el).attr('href');
      if (src && /\.(jpg|jpeg|png|gif|webp|avif)($|\?)/i.test(src)) {
        try {
          const absolute = new URL(src, url).href;
          if (!images.includes(absolute)) images.push(absolute);
        } catch (e) { /* skip invalid urls */ }
      }
    });

    await browser.close();
    console.log(`[SUCCESS] Scrape complete. Found ${videoLinks.length} videos.`);

    res.json({ title, videos: videoLinks, images });
  } catch (error: any) {
    if (browser) await browser.close();
    console.error(`[ERROR] ${error.message}`);
    res.status(500).json({ error: error.message });
  }
});

app.get('/api/health', (req: Request, res: Response) => {
  res.json({ status: 'ok', timestamp: new Date().toISOString() })
});

const PORT = parseInt(process.env.PORT || '3000', 10);
app.listen(PORT, '0.0.0.0', () => {
  console.log(`\n🚀 Server running in Docker on port ${PORT}`);
  console.log(`Using yt-dlp for universal video extraction\n`);
});