import playwrightExtra from 'playwright-extra';
import StealthPlugin from 'puppeteer-extra-plugin-stealth';
import fs from 'fs';
import path from 'path';
import { BROWSER_ARGS, USER_DATA_DIR } from './config.js';
import { filterVideoUrls } from './utils.js';

playwrightExtra.chromium.use(StealthPlugin());

export async function performScrape(url: string, streamId: string) {
  // Fix SingletonLock issue
  const lockFile = path.join(USER_DATA_DIR, 'SingletonLock');
  if (fs.existsSync(lockFile)) { try { fs.unlinkSync(lockFile); } catch {} }

  const browser = await playwrightExtra.chromium.launchPersistentContext(USER_DATA_DIR, {
    headless: true,
    args: BROWSER_ARGS,
    viewport: { width: 1280, height: 900 },
    userAgent: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/131.0.0.0 Safari/537.36',
  });

  try {
    const page = await browser.newPage();
    const interceptedVideos: any[] = [];

    // Popup killer
    browser.on('page', async popup => {
      await new Promise(r => setTimeout(r, 8000));
      if (!popup.isClosed()) await popup.close().catch(() => {});
    });

    // Network Sniffer
    page.on('request', request => {
      const reqUrl = request.url();
      if ((reqUrl.includes('get_video') || reqUrl.includes('.m3u8') || reqUrl.includes('.mp4')) && reqUrl !== url) {
        if (!interceptedVideos.some(v => v.url === reqUrl)) {
          interceptedVideos.push({ site: 'Sniffer', url: reqUrl });
        }
      }
    });

    await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 30000 }).catch(() => {});

    // --- Site Strategies ---
    if (/vidara\.|vidnest\./i.test(url)) {
      await page.click('.jw-video, video', { force: true }).catch(() => {});
    } else if (/vidsonic\./i.test(url)) {
      await page.click('.vjs-big-play-button', { force: true }).catch(() => {});
    } else {
      await page.click('.plyr__control--overlaid, .play-overlay', { force: true }).catch(() => {});
    }

    await page.waitForTimeout(3000);
    const title = await page.title().catch(() => "Unknown");
    
    return {
      title,
      videos: filterVideoUrls(interceptedVideos, url)
    };
  } finally {
    await browser.close().catch(() => {});
  }
}