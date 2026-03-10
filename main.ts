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

  const isLulu = /luluvdo|luluvid/i.test(url);

  if (!url || isLulu) return res.status(400).json({ error: 'URL is required or Lulu URLs are not supported' });

  const isVidara = /vidara\./i.test(url);

  const isVidsonic = /vidsonic\./i.test(url);

  const isVidnest = /vidnest\./i.test(url);

  const streamId = url.split('/v/')[1]?.split('/')[0] || Math.random().toString(36).substring(7);


  const scrapeAttempt = async () => {
    let browser;
    try {
      console.log(`[START] [ID: ${streamId}] Scrape Target: ${url}`);

      const userDataDir = path.join(os.tmpdir(), 'dropmms-api-profile');

      if (fs.existsSync(path.join(userDataDir, 'SingletonLock'))) {
        try { fs.unlinkSync(path.join(userDataDir, 'SingletonLock')); } catch { }
      }

      browser = await playwrightExtra.chromium.launchPersistentContext(userDataDir, {
        headless: true,
        args: [
          '--no-sandbox', '--disable-dev-shm-usage', '--no-zygote',
          '--disable-setuid-sandbox', '--disable-infobars', '--window-size=1280,900',
          '--disable-blink-features=AutomationControlled',
        ],
        viewport: { width: 1280, height: 900 },
        userAgent: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
      });

      const page = await browser.newPage();
      const interceptedVideos: any[] = [];

      // --- FIXED POPUP KILLER ---
      browser.on('page', async popup => {
        try {
          // Give stealth plugin 1-2 seconds to finish initializing before closing
          await new Promise(resolve => setTimeout(resolve, 8000));
          if (!popup.isClosed()) {
            await popup.close().catch(() => { });
          }
        } catch (e) {
          // Ignore errors if the popup was already closed
        }
      });

      page.on('request', request => {
        const reqUrl = request.url();
        const isBlacklisted = /yandex|mc\.ru|analytics|pixel|google|\.ts($|\?)/i.test(reqUrl);

        if (!isBlacklisted && (reqUrl.includes('get_video') || reqUrl.includes('.m3u8') || reqUrl.includes('.mp4'))) {
          if (reqUrl !== url && !interceptedVideos.some(v => v.url === reqUrl)) {
            console.log(`[CATCH] [ID: ${streamId}] Found: ${reqUrl.substring(0, 60)}...`);
            interceptedVideos.push({ site: 'Network-Sniffer', url: reqUrl });
          }
        }
      });

      await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 30000 }).catch(() => { });

      if (isVidara || isVidnest) {
        console.log(`[DEBUG] [ID: ${streamId}] Strategy: VIDARA (JWPlayer)`);
        const vidaraPlay = '.jw-video, .jw-display-icon-container, video';
        await page.click(vidaraPlay, { force: true }).catch(() => { });
        await page.waitForTimeout(2000);

        const jwSource = await page.evaluate(() => {
          try {
            const player = (window as any).jwplayer?.();
            return player ? player.getPlaylist()?.[0]?.file : null;
          } catch { return null; }
        });
        if (jwSource) interceptedVideos.push({ site: 'JW-Internal', url: jwSource });

      } else if (isVidsonic) {
        console.log(`[DEBUG] [ID: ${streamId}] Strategy: VIDSONIC (Video.js)`);
        await page.evaluate(() => {
          document.querySelector('.vjs-vast-label')?.remove();
        });
        const vjsPlay = '.vjs-big-play-button, .vjs-play-control';
        await page.click(vjsPlay, { force: true }).catch(() => { });
        await page.waitForTimeout(2000);

        const vjsSource = await page.evaluate(() => {
          try {
            const playerEl = document.querySelector('.video-js');
            if (playerEl && (playerEl as any).player) return (playerEl as any).player.src();
            if ((window as any).videojs) {
              const players = (window as any).videojs.getPlayers();
              return Object.values(players)[0] ? (Object.values(players)[0] as any).src() : null;
            }
          } catch { return null; }
        });
        if (vjsSource && !vjsSource.startsWith('blob:')) interceptedVideos.push({ site: 'VJS-Internal', url: vjsSource });

      } else {
        console.log(`[DEBUG] [ID: ${streamId}] Strategy: GENERAL/STREAMTAPE`);
        try {
          const playBtn = '.plyr__control--overlaid, .play-overlay, button[data-plyr="play"]';
          await page.waitForSelector(playBtn, { timeout: 10000 });
          await page.click(playBtn, { force: true });
          await page.waitForTimeout(1500);
          await page.evaluate((sel) => {
            const btn = document.querySelector(sel) as HTMLElement;
            if (btn) btn.dispatchEvent(new MouseEvent('click', { bubbles: true }));
          }, playBtn);
        } catch (err) { }
      }

      const domMedia = await page.evaluate(() => {
        const v = document.querySelector('video') as HTMLVideoElement;
        if (!v) return null;
        const src = v.currentSrc || v.src;
        return (src && !src.startsWith('blob:')) ? src : null;
      });

      if (domMedia && !interceptedVideos.some(v => v.url === domMedia)) {
        interceptedVideos.push({ site: 'DOM-Extraction', url: domMedia });
      }

      const title = await page.title();
      await browser.close();

      return {
        title,
        videos: Array.from(new Map(
          interceptedVideos
            .filter(v => v.url !== url && !v.url.startsWith('blob:'))
            .map(v => [v.url, v])
        ).values())
      };

    } catch (error: any) {
      if (browser) await browser.close();
      throw error;
    }
  };

  try {
    let result = await scrapeAttempt();

    if (result.videos.length === 0) {
      console.log(`[RETRY] [ID: ${streamId}] No videos found. Retrying in 60 seconds...`);
      await new Promise(resolve => setTimeout(resolve, 60000));
      result = await scrapeAttempt();
    }

    if (result.videos.length === 0) {
      console.log(`[FAILED] [ID: ${streamId}] No videos found after retry.`);
      return res.status(202).json({ 
        error: 'No videos found. Please try again later.',
        retryAfter: 300 
      });
    }

    console.log(`[COMPLETE] [ID: ${streamId}] Found ${result.videos.length} videos.`);
    res.json(result);

  } catch (error: any) {
    console.error(`[CRITICAL] [ID: ${streamId}] ${error.message}`);
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