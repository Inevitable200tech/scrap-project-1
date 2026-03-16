import playwrightExtra from 'playwright-extra';
import StealthPlugin from 'puppeteer-extra-plugin-stealth';
import { filterVideoUrls } from './utils.js';

playwrightExtra.chromium.use(StealthPlugin());

export interface ScrapeResult {
  title: string;
  videos: { site: string; url: string }[];
}

function log(streamId: string, message: string, level: 'INFO' | 'WARN' | 'ERROR' = 'INFO') {
  const ts = new Date().toISOString();
  const prefix = `[${ts}] [ID: ${streamId}] [${level}]`;
  console[level.toLowerCase() as 'log' | 'warn' | 'error'](`${prefix} ${message}`);
}

export async function performScrape(url: string, streamId: string): Promise<ScrapeResult> {
  const isVidara  = /vidara\./i.test(url);
  const isVidsonic = /vidsonic\./i.test(url);
  const isVidnest  = /vidnest\./i.test(url);

  log(streamId, `Starting scrape for URL: ${url}`);
  log(streamId, `Site detection → Vidara: ${isVidara}, Vidsonic: ${isVidsonic}, Vidnest: ${isVidnest}`);

  const scrapeAttempt = async (): Promise<ScrapeResult> => {
    let browser: any = null;
    let context: any = null;
    let page: any = null;

    try {
      log(streamId, 'Launching browser...');
      browser = await playwrightExtra.chromium.launch({
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
      });
      log(streamId, 'Browser launched successfully');

      log(streamId, 'Creating new browser context...');
      context = await browser.newContext({
        viewport: { width: 1280, height: 900 },
        userAgent:
          'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, Gecko) Chrome/131.0.0.0 Safari/537.36',
      });
      log(streamId, 'Browser context created');

      // Popup / new page handler
      context.on('page', async (popup: any) => {
        const popupUrl = popup.url?.() || '(unknown)';
        log(streamId, `New page/popup detected → ${popupUrl}`);
        try {
          log(streamId, 'Waiting 8 seconds before auto-closing popup...');
          await new Promise(resolve => setTimeout(resolve, 2000));

          if (!popup.isClosed()) {
            log(streamId, 'Closing detected popup');
            await popup.close().catch((e: any) => {
              log(streamId, `Failed to close popup: ${e.message}`, 'WARN');
            });
          } else {
            log(streamId, 'Popup already closed');
          }
        } catch (e: any) {
          log(streamId, `Popup handler error: ${e.message}`, 'ERROR');
        }
      });

      log(streamId, 'Opening primary page...');
      page = await context.newPage();
      log(streamId, 'Primary page opened');

      const interceptedVideos: { site: string; url: string }[] = [];

      log(streamId, 'Attaching network request interceptor...');
      page.on('request', (request: { url: () => string }) => {
        const reqUrl = request.url();
        const isBlacklisted = /yandex|mc\.ru|analytics|pixel|google|\.ts($|\?)/i.test(reqUrl);

        if (!isBlacklisted && (reqUrl.includes('get_video') || reqUrl.includes('.m3u8') || reqUrl.includes('.mp4'))) {
          if (reqUrl !== url && !interceptedVideos.some(v => v.url === reqUrl)) {
            log(streamId, `VIDEO URL INTERCEPTED → ${reqUrl.substring(0, 100)}...`);
            interceptedVideos.push({ site: 'Network-Sniffer', url: reqUrl });
          }
        }
      });

      log(streamId, `Navigating to: ${url}`);
      await page.goto(url, { waitUntil: 'networkidle', timeout: 45000 }).catch((e: any) => {
        log(streamId, `Navigation error (non-fatal): ${e.message}`, 'WARN');
      });
      log(streamId, 'Navigation phase completed');

      // ────────────────────────────────────────────────
      // Site/player-specific extraction logic
      // ────────────────────────────────────────────────
      if (isVidara || isVidnest) {
        log(streamId, 'Vidara / Vidnest detected → JW Player path');
        const playSelector = '.jw-video, .jw-display-icon-container, video';
        log(streamId, `Attempting click: ${playSelector}`);
        await page.click(playSelector, { force: true, timeout: 10000 }).catch((e: { message: any; }) =>
          log(streamId, `Play click failed: ${e.message}`, 'WARN')
        );
        await page.waitForTimeout(2200);

        log(streamId, 'Extracting JW Player source...');
        const jwSource = await page.evaluate(() => {
          try {
            const player = (window as any).jwplayer?.();
            return player?.getPlaylist?.()?.[0]?.file ?? null;
          } catch {
            return null;
          }
        });

        if (jwSource) {
          log(streamId, `JW source extracted: ${jwSource.substring(0, 90)}...`);
          interceptedVideos.push({ site: 'JW-Internal', url: jwSource });
        } else {
          log(streamId, 'No JW Player source found');
        }

      } else if (isVidsonic) {
        log(streamId, 'Vidsonic detected → Video.js path');
        await page.evaluate(() => {
          document.querySelector('.vjs-vast-label')?.remove();
        }).catch(() => {});

        const playSelector = '.vjs-big-play-button, .vjs-play-control';
        log(streamId, `Attempting click: ${playSelector}`);
        await page.click(playSelector, { force: true, timeout: 10000 }).catch((e: { message: any; }) =>
          log(streamId, `Video.js click failed: ${e.message}`, 'WARN')
        );
        await page.waitForTimeout(2200);

        log(streamId, 'Extracting Video.js source...');
        const vjsSource = await page.evaluate(() => {
          try {
            const playerEl = document.querySelector('.video-js');
            if (playerEl && (playerEl as any).player) return (playerEl as any).player.src();
            if ((window as any).videojs) {
              const players = (window as any).videojs.getPlayers();
              const firstPlayer = Object.values(players)[0];
              return firstPlayer ? (firstPlayer as any).src() : null;
            }
            return null;
          } catch {
            return null;
          }
        });

        if (vjsSource && !vjsSource.startsWith('blob:')) {
          log(streamId, `Video.js source: ${vjsSource.substring(0, 90)}...`);
          interceptedVideos.push({ site: 'VJS-Internal', url: vjsSource });
        } else {
          log(streamId, 'No valid Video.js source found');
        }

      } else {
        log(streamId, 'Generic player path (Plyr / overlay attempt)');
        try {
          const playSelector = '.plyr__control--overlaid, .play-overlay, button[data-plyr="play"]';
          log(streamId, `Waiting for selector: ${playSelector}`);
          await page.waitForSelector(playSelector, { timeout: 12000 });
          log(streamId, 'Play element found → clicking');
          await page.click(playSelector, { force: true });

          await page.waitForTimeout(1800);

          await page.evaluate((sel: string) => {
            const btn = document.querySelector(sel) as HTMLElement | null;
            btn?.dispatchEvent(new MouseEvent('click', { bubbles: true, cancelable: true }));
          }, playSelector);
        } catch (err: any) {
          log(streamId, `Generic play attempt failed: ${err.message}`, 'WARN');
        }
      }

      // ────────────────────────────────────────────────
      // Final DOM <video> fallback
      // ────────────────────────────────────────────────
      log(streamId, 'Checking <video> element source...');
      const domMedia = await page.evaluate(() => {
        const video = document.querySelector('video') as HTMLVideoElement | null;
        if (!video) return null;
        const src = video.currentSrc || video.src || '';
        return src && !src.startsWith('blob:') ? src : null;
      });

      if (domMedia) {
        log(streamId, `DOM video src: ${domMedia.substring(0, 90)}...`);
        if (!interceptedVideos.some(v => v.url === domMedia)) {
          interceptedVideos.push({ site: 'DOM-Extraction', url: domMedia });
        }
      } else {
        log(streamId, 'No usable <video> source found in DOM');
      }

      const title = await page.title().catch(() => 'Unknown Title');
      log(streamId, `Page title: ${title}`);

      log(streamId, `Collected ${interceptedVideos.length} candidate video URLs`);

      // ────────────────────────────────────────────────
      // Cleanup
      // ────────────────────────────────────────────────
      log(streamId, 'Initiating cleanup...');
      if (context) {
        log(streamId, 'Closing context...');
        await context.close().catch((e: { message: any; }) => log(streamId, `Context close failed: ${e.message}`, 'WARN'));
      }
      if (browser) {
        log(streamId, 'Closing browser...');
        await browser.close().catch((e: { message: any; }) => log(streamId, `Browser close failed: ${e.message}`, 'WARN'));
      }

      const filteredVideos = filterVideoUrls(interceptedVideos, url);
      log(streamId, `Final result: ${filteredVideos.length} valid video URLs after filtering`);

      log(streamId, 'Scrape completed successfully');
      return { title, videos: filteredVideos };

    } catch (error: any) {
      log(streamId, `SCRAPE FAILED: ${error.message || 'Unknown error'}`, 'ERROR');
      if (error?.stack) {
        log(streamId, `Stack:\n${error.stack}`, 'ERROR');
      }

      // Emergency cleanup
      try {
        if (context) await context.close().catch(() => {});
        if (browser) await browser.close().catch(() => {});
      } catch (cleanupErr: any) {
        log(streamId, `Cleanup during error handling failed: ${cleanupErr.message}`, 'ERROR');
      }

      throw error;
    }
  };

  return scrapeAttempt();
}