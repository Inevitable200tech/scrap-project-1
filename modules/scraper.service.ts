import playwrightExtra from 'playwright-extra';
import StealthPlugin from 'puppeteer-extra-plugin-stealth';
import fs from 'fs';
import path from 'path';
import os from 'os';
import { filterVideoUrls } from './utils.js';

playwrightExtra.chromium.use(StealthPlugin());

// Define the interface so other files know what this returns
export interface ScrapeResult {
  title: string;
  videos: { site: string; url: string }[];
}


// Exporting with explicit return type to fix 'void' errors in main.ts
export async function performScrape(url: string, streamId: string): Promise<ScrapeResult> {

  const isVidara = /vidara\./i.test(url);
  const isVidsonic = /vidsonic\./i.test(url);
  const isVidnest = /vidnest\./i.test(url);

  // Logical fix: We must actually call and return the inner function
  const scrapeAttempt = async (): Promise<ScrapeResult> => {
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
        userAgent: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, Gecko) Chrome/131.0.0.0 Safari/537.36',
      });

      const page = await browser.newPage();
      const interceptedVideos: any[] = [];

      browser.on('page', async popup => {
        try {
          await new Promise(resolve => setTimeout(resolve, 8000));
          if (!popup.isClosed()) {
            await popup.close().catch(() => { });
          }
        } catch (e) { }
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
        await page.evaluate(() => {
          (document.querySelector('.vjs-vast-label') as any)?.remove();
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

      const title = await page.title().catch(() => "Unknown Title");
      if (browser) await browser.close().catch(() => { });

      const filteredVideos = filterVideoUrls(interceptedVideos, url);
      return { title, videos: filteredVideos };

    } catch (error: any) {
      if (browser) await browser.close();
      throw error;
    }
  };

  // Fix: Execute the attempt and return the Promise
  return scrapeAttempt();
}