import playwrightExtra from 'playwright-extra';
import StealthPlugin from 'puppeteer-extra-plugin-stealth';
import fs from 'fs';
import path from 'path';
import os from 'os';
import { filterVideoUrls } from './utils.js';

playwrightExtra.chromium.use(StealthPlugin());

export async function performScrape(url: string, streamId: string) {
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

    // Network Sniffer
    // Updated Network Sniffer in scraper.service.ts
    page.on('request', request => {
      const reqUrl = request.url();

      // 1. Target specific ad domains like t.dtscdn.com and tracking parameters
      const isAdOrTracker = /yandex|mc\.ru|analytics|dtscdn|pixel|google|dtscout|ad-delivery|popads|doubleclick|securepubads/i.test(reqUrl);

      // 2. Block .ts segments which are often mistaken for video files but are just stream parts
      const isStreamSegment = /\.ts($|\?)/i.test(reqUrl);

      if (isAdOrTracker || isStreamSegment) {
        // Abort or ignore the request immediately to save CPU and avoid false positives
        return;
      }

      // 3. Capture legitimate video signals
      const isVideo = reqUrl.includes('get_video') || reqUrl.includes('.m3u8') || reqUrl.includes('.mp4');

      if (isVideo && reqUrl !== url && !interceptedVideos.some(v => v.url === reqUrl)) {
        // Priority check: Always prioritize the direct Streamtape 'get_video' link
        if (reqUrl.includes('get_video')) {
          console.log(`[CATCH-PRIORITY] [ID: ${streamId}] Found Direct Stream: ${reqUrl.substring(0, 60)}...`);
          interceptedVideos.unshift({ site: 'Network-Sniffer-Direct', url: reqUrl });
        } else {
          console.log(`[CATCH] [ID: ${streamId}] Found: ${reqUrl.substring(0, 60)}...`);
          interceptedVideos.push({ site: 'Network-Sniffer', url: reqUrl });
        }
      }
    });

    await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 30000 }).catch(() => { });

    const isVidara = /vidara\./i.test(url);
    const isVidnest = /vidnest\./i.test(url);
    // Updated to catch both vidsonic.xxx and vsonic.xxx
    const isVidsonic = /vidsonic\.|vsonic\./i.test(url);
    // --- Site Specific Strategies ---
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

    }
    else {
      console.log(`[DEBUG] [ID: ${streamId}] Strategy: GENERAL/STREAMTAPE`);

      // 1. Initial wait to see if 'get_video' pops up automatically on load
      await page.waitForTimeout(3000);

      // 2. Check if we already have a direct streamtape link in our intercepted list
      const hasDirectLink = interceptedVideos.some(v => v.url.includes('get_video?id='));

      if (hasDirectLink) {
        console.log(`[DEBUG] [ID: ${streamId}] Direct link already captured. Skipping clicks.`);
      } else {
        // 3. Fallback: Perform the manual click if no link was found yet
        try {
          // Remove overlays that might block the button
          await page.evaluate(() => {
            const ads = document.querySelectorAll('div[style*="z-index: 2147483647"]');
            ads.forEach(el => el.remove());
          });

          const playBtn = '.plyr__control--overlaid, .play-overlay, button[data-plyr="play"], #videooverlay';
          await page.waitForSelector(playBtn, { timeout: 5000 }).catch(() => { });

          // Force a click on the play button
          await page.click(playBtn, { force: true }).catch(() => { });

          // Wait extra time for Streamtape's dynamic link to generate
          await page.waitForTimeout(3000);
        } catch (err) {
          console.warn(`[DEBUG] [ID: ${streamId}] Click strategy failed or timed out.`);
        }
      }
    }

    // Fallback: DOM Extraction
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
    return {
      title,
      videos: filteredVideos
    };

  } catch (error: any) {
    if (browser) await browser.close().catch(() => { });
    throw error;
  }
}