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

function isPageAlive(p: any): boolean {
  try {
    return p && !p.isClosed();
  } catch {
    return false;
  }
}

/**
 * Vidara loads its player inside a cross-origin iframe at vidara.so/e/<filecode>.
 * We can't read that iframe from the parent page, so we rewrite the URL to
 * navigate directly to the embed page where JW Player is accessible.
 *
 * Input:  https://vidara.to/v/BYs7T9uu9UdFP/some-title
 * Output: https://vidara.so/e/BYs7T9uu9UdFP
 */
function resolveVidaraEmbedUrl(url: string): string {
  try {
    const parsed = new URL(url);

    // Already an embed URL — use as-is
    if (parsed.hostname.includes('vidara.so') && parsed.pathname.startsWith('/e/')) {
      return url;
    }

    // Extract filecode from path — first non-empty segment after skipping 'v'
    // e.g. /v/BYs7T9uu9UdFP/some-title → BYs7T9uu9UdFP
    const segments = parsed.pathname.split('/').filter(Boolean);
    const filecode = segments[0] === 'v' ? segments[1] : segments[0];

    return `https://vidara.so/e/${filecode}`;
  } catch {
    return url;
  }
}

export async function performScrape(url: string, streamId: string): Promise<ScrapeResult> {
  const isVidara     = /vidara\./i.test(url);
  const isVidsonic   = /vidsonic\./i.test(url);
  const isVidnest    = /vidnest\./i.test(url);
  const isStreamtape = /streamtape\./i.test(url);

  // Vidara: navigate directly to the embed page instead of the main page
  const targetUrl = isVidara ? resolveVidaraEmbedUrl(url) : url;

  log(streamId, `Starting scrape for URL: ${url}`);
  if (isVidara) log(streamId, `Vidara embed URL resolved → ${targetUrl}`);
  log(streamId, `Site detection → Vidara: ${isVidara}, Vidsonic: ${isVidsonic}, Vidnest: ${isVidnest}, Streamtape: ${isStreamtape}`);

  const scrapeAttempt = async (): Promise<ScrapeResult> => {
    let browser: any = null;
    let context: any = null;
    let page: any = null;

    try {
      // ── Browser launch ─────────────────────────────────────────────────────
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

      // ── Browser context ────────────────────────────────────────────────────
      log(streamId, 'Creating new browser context...');
      context = await browser.newContext({
        viewport: { width: 1280, height: 900 },
        userAgent:
          'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
      });
      log(streamId, 'Browser context created');

      // ── Primary page ───────────────────────────────────────────────────────
      // Open BEFORE registering context.on('page') so the handler never sees
      // our own tab and accidentally closes it.
      log(streamId, 'Opening primary page...');
      page = await context.newPage();
      log(streamId, 'Primary page opened');

      page.on('close', () => log(streamId, 'PAGE CLOSED EVENT FIRED', 'ERROR'));
      page.on('console', (msg: any) => {
        if (msg.type() === 'error') {
          log(streamId, `PAGE CONSOLE ERROR: ${msg.text()}`, 'ERROR');
        }
      });

      // ── Popup handler ──────────────────────────────────────────────────────
      // Registered AFTER primary page so it never matches our own tab.
      context.on('page', async (popup: any) => {
        if (popup === page) return;

        const popupUrl = popup.url?.() || '(unknown)';
        log(streamId, `New page/popup detected → ${popupUrl}`);

        try {
          await new Promise(resolve => setTimeout(resolve, 800));

          if (!popup.isClosed()) {
            log(streamId, 'Closing popup');
            await popup.close({ runBeforeUnload: false }).catch((e: any) =>
              log(streamId, `Popup close failed: ${e.message}`, 'WARN')
            );
          } else {
            log(streamId, 'Popup already closed');
          }
        } catch (e: any) {
          log(streamId, `Popup handler error: ${e.message}`, 'ERROR');
        }
      });

      // ── Network interception ───────────────────────────────────────────────
      const interceptedVideos: { site: string; url: string }[] = [];

      log(streamId, 'Attaching network request interceptor...');
      page.on('request', (request: { url: () => string }) => {
        const reqUrl = request.url();

        // Extended blacklist — ad trackers, analytics, CDN ad networks
        const isBlacklisted = /yandex|mc\.ru|analytics|pixel|google|\.ts($|\?)|dtscout|dtscdn|doubleclick|adnxs|adsystem|googlesyndication|amazon-adsystem|outbrain|taboola|adsbygoogle/i.test(reqUrl);

        if (
          !isBlacklisted &&
          (reqUrl.includes('get_video') || reqUrl.includes('.m3u8') || reqUrl.includes('.mp4'))
        ) {
          if (reqUrl !== url && !interceptedVideos.some(v => v.url === reqUrl)) {
            log(streamId, `VIDEO URL INTERCEPTED → ${reqUrl.substring(0, 100)}...`);
            interceptedVideos.push({ site: 'Network-Sniffer', url: reqUrl });
          }
        }
      });

      // ── Navigation ─────────────────────────────────────────────────────────
      // Use targetUrl — embed URL for Vidara, original URL for everything else
      log(streamId, `Navigating to: ${targetUrl}`);
      await page.goto(targetUrl, { waitUntil: 'domcontentloaded', timeout: 45000 }).catch((e: any) => {
        log(streamId, `Navigation error (non-fatal): ${e.message}`, 'WARN');
      });

      if (!isPageAlive(page)) {
        throw new Error('Primary page was closed unexpectedly after navigation');
      }

      // Streamtape and Vidara have their data available immediately after load —
      // skip the 3.5s warm-up wait for both.
      if (!isStreamtape && !isVidara) {
        await page.waitForTimeout(3500).catch(() => {});
      }
      log(streamId, 'Navigation phase completed');

      // ── Player-specific extraction ─────────────────────────────────────────
      if (isVidara) {
        log(streamId, 'Vidara detected → JW Player extraction from embed page');

        // The embed page fetches /api/stream?filecode=... immediately on load and
        // passes the result to JW Player. A short wait is enough for that fetch
        // to complete and the playlist to be populated.
        await page.waitForTimeout(3000).catch(() => {});

        const vidaraUrl = await page.evaluate(() => {
          try {
            const p = (window as any).jwplayer?.();
            if (!p) return null;

            // Primary: playlist file (HLS m3u8 or direct mp4)
            const file = p.getPlaylist?.()?.[0]?.file;
            if (file && !file.startsWith('blob:')) return file;

            // Fallback: first non-blob source in sources array
            const sources: any[] = p.getPlaylist?.()?.[0]?.sources ?? [];
            const valid = sources.find((s: any) => s.file && !s.file.startsWith('blob:'));
            return valid?.file ?? null;
          } catch {
            return null;
          }
        }).catch(() => null);

        if (vidaraUrl) {
          log(streamId, `Vidara JW source extracted: ${String(vidaraUrl).substring(0, 90)}...`);
          interceptedVideos.push({ site: 'Vidara-JW', url: vidaraUrl });
        } else {
          log(streamId, 'Vidara JW extraction failed — will rely on network sniffer', 'WARN');
        }

      } else if (isVidnest) {
        log(streamId, 'Vidnest detected → JW Player path');
        const playSelector = '.jw-video, .jw-display-icon-container, video';
        log(streamId, `Attempting click: ${playSelector}`);
        await page.click(playSelector, { force: true, timeout: 10000 }).catch((e: any) =>
          log(streamId, `Play click failed: ${e.message}`, 'WARN')
        );
        await page.waitForTimeout(2200).catch(() => {});

        log(streamId, 'Extracting JW Player source...');
        const jwSource = await page.evaluate(() => {
          try {
            const player = (window as any).jwplayer?.();
            return player?.getPlaylist?.()?.[0]?.file ?? null;
          } catch {
            return null;
          }
        }).catch(() => null);

        if (jwSource) {
          log(streamId, `Vidnest JW source extracted: ${String(jwSource).substring(0, 90)}...`);
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
        await page.click(playSelector, { force: true, timeout: 10000 }).catch((e: any) =>
          log(streamId, `Video.js click failed: ${e.message}`, 'WARN')
        );
        await page.waitForTimeout(2200).catch(() => {});

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
        }).catch(() => null);

        if (vjsSource && !String(vjsSource).startsWith('blob:')) {
          log(streamId, `Video.js source: ${String(vjsSource).substring(0, 90)}...`);
          interceptedVideos.push({ site: 'VJS-Internal', url: vjsSource });
        } else {
          log(streamId, 'No valid Video.js source found');
        }

      } else if (isStreamtape) {
        log(streamId, 'Streamtape detected → norobotlink / video src extraction');

        // The URL is embedded in the initial HTML — no click or extra wait needed.
        // #norobotlink is a <div> whose textContent is set by an inline <script>
        // immediately on page load. The <video> src attribute is also in the HTML.
        const streamtapeUrl = await page.evaluate(() => {
          try {
            // Method 1: #norobotlink — most reliable, always present after load
            const norobotEl = document.getElementById('norobotlink');
            if (norobotEl) {
              let href = norobotEl.textContent?.trim() || '';
              if (href.startsWith('//')) return 'https:' + href;
              if (href.startsWith('https://')) return href;
            }

            // Method 2: raw src attribute on <video> (already in HTML, no play needed)
            const video = document.querySelector('video');
            if (video) {
              let src = video.getAttribute('src') || '';
              if (src.startsWith('//')) return 'https:' + src;
              if (src.startsWith('https://')) return src;
            }

            return null;
          } catch {
            return null;
          }
        }).catch(() => null);

        if (streamtapeUrl) {
          log(streamId, `Streamtape URL extracted: ${String(streamtapeUrl).substring(0, 90)}...`);
          interceptedVideos.push({ site: 'Streamtape-DOM', url: streamtapeUrl });
        } else {
          log(streamId, 'Streamtape extraction failed — will rely on network sniffer', 'WARN');
        }

      } else {
        log(streamId, 'Generic player path (Plyr / overlay attempt)');
        try {
          const playSelector = '.plyr__control--overlaid, .play-overlay, button[data-plyr="play"]';
          log(streamId, `Waiting for selector: ${playSelector}`);
          await page.waitForSelector(playSelector, { timeout: 12000 });
          log(streamId, 'Play element found → clicking');
          await page.click(playSelector, { force: true });
          await page.waitForTimeout(1800).catch(() => {});

          await page.evaluate((sel: string) => {
            const btn = document.querySelector(sel) as HTMLElement | null;
            btn?.dispatchEvent(new MouseEvent('click', { bubbles: true, cancelable: true }));
          }, playSelector).catch(() => {});
        } catch (err: any) {
          log(streamId, `Generic play attempt failed: ${err.message}`, 'WARN');
        }
      }

      // ── DOM <video> fallback ───────────────────────────────────────────────
      // Skip for Streamtape and Vidara — both already handled above.
      // Vidara's <video> only ever has a blob: src which is useless.
      if (!isStreamtape && !isVidara) {
        log(streamId, 'Checking <video> element source...');
        const domMedia = isPageAlive(page)
          ? await page.evaluate(() => {
              const video = document.querySelector('video') as HTMLVideoElement | null;
              if (!video) return null;
              const src = video.currentSrc || video.src || '';
              return src && !src.startsWith('blob:') ? src : null;
            }).catch(() => null)
          : null;

        if (domMedia) {
          log(streamId, `DOM video src: ${String(domMedia).substring(0, 90)}...`);
          if (!interceptedVideos.some(v => v.url === domMedia)) {
            interceptedVideos.push({ site: 'DOM-Extraction', url: domMedia });
          }
        } else {
          log(streamId, 'No usable <video> source found in DOM');
        }
      }

      const title = isPageAlive(page)
        ? await page.title().catch(() => 'Unknown Title')
        : 'Unknown Title';

      log(streamId, `Page title: ${title}`);
      log(streamId, `Collected ${interceptedVideos.length} candidate video URLs`);

      // ── Cleanup ────────────────────────────────────────────────────────────
      log(streamId, 'Initiating cleanup...');
      await context?.close().catch((e: any) =>
        log(streamId, `Context close failed: ${e.message}`, 'WARN')
      );
      await browser?.close().catch((e: any) =>
        log(streamId, `Browser close failed: ${e.message}`, 'WARN')
      );

      const filteredVideos = filterVideoUrls(interceptedVideos, url);
      log(streamId, `Final result: ${filteredVideos.length} valid video URLs after filtering`);
      log(streamId, 'Scrape completed successfully');

      return { title, videos: filteredVideos };

    } catch (error: any) {
      log(streamId, `SCRAPE FAILED: ${error.message || 'Unknown error'}`, 'ERROR');
      if (error?.stack) log(streamId, `Stack:\n${error.stack}`, 'ERROR');

      await context?.close().catch(() => {});
      await browser?.close().catch(() => {});

      throw error;
    }
  };

  return scrapeAttempt();
}