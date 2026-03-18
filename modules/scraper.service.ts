import playwrightExtra from 'playwright-extra';
import StealthPlugin from 'puppeteer-extra-plugin-stealth';
import { filterVideoUrls } from './utils.js';

playwrightExtra.chromium.use(StealthPlugin());

export interface ScrapeResult {
  title: string;
  originalUrl: string;
  videos: { site: string; url: string }[];
  dead?: boolean;
  // Structured failure reason — consumed by main.ts to set job.failureReason
  // without fragile error string matching.
  deadReason?: string;
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

// ── Universal dead video detector ──────────────────────────────────────────
const DEAD_VIDEO_SIGNALS = [
  /video (has been|was) (removed|deleted)/i,
  /removed by the uploader/i,
  /this video is (no longer|not) available/i,
  /video (not found|unavailable|expired)/i,
  /file (not found|has been deleted|no longer exists)/i,
  /content (has been|was) (removed|deleted|taken down)/i,
  /page not found/i,
  /404 not found/i,
  /maybe it got deleted by the creator/i,
  /video-empty/i,
  /img_loading_error/i,
  /this file (no longer exists|has been removed)/i,
  /file (was|has been) (deleted|removed)/i,
  /This video has been removed due to term violence./i,
];

function detectDeadVideo(httpStatus: number, html: string): string | null {
  if (httpStatus === 404 || httpStatus === 410) return `HTTP ${httpStatus}`;

  const bodyOnly = html
    .replace(/<script[\s\S]*?<\/script>/gi, '')
    .replace(/<style[\s\S]*?<\/style>/gi, '');

  for (const pattern of DEAD_VIDEO_SIGNALS) {
    if (pattern.test(bodyOnly)) return pattern.source;
  }
  return null;
}

/**
 * Dumps visible page text and any clues about why no video was found.
 * Only called when 0 video URLs are collected — purely for debugging.
 */
async function dumpPageDebugInfo(
  page: any,
  streamId: string,
  interceptedCount: number
): Promise<void> {
  if (!isPageAlive(page)) {
    log(streamId, '[DEBUG] Page not alive — cannot dump debug info', 'WARN');
    return;
  }

  try {
    const debugInfo = await page.evaluate(() => {
      const clone = document.body.cloneNode(true) as HTMLElement;
      clone.querySelectorAll('script, style, noscript').forEach(el => el.remove());
      const visibleText = (clone.innerText || clone.textContent || '')
        .replace(/\s+/g, ' ')
        .trim()
        .substring(0, 1000);

      const allLinks = Array.from(document.querySelectorAll('a[href]'))
        .map(a => (a as HTMLAnchorElement).href)
        .filter(h => h.startsWith('http'))
        .slice(0, 20);

      const iframes = Array.from(document.querySelectorAll('iframe')).map(f => ({
        src: f.src || f.getAttribute('src'),
        id: f.id,
      }));

      const videos = Array.from(document.querySelectorAll('video')).map(v => ({
        src: v.src,
        currentSrc: v.currentSrc,
        srcAttribute: v.getAttribute('src'),
        readyState: v.readyState,
      }));

      const errorSelectors = [
        '.error', '.message', '.alert', '.notice',
        '[class*="error"]', '[class*="empty"]', '[class*="not-found"]',
        '[class*="unavailable"]', '[class*="deleted"]', '[class*="removed"]',
        'h1', 'h2', '.title',
      ];
      const errorTexts: Record<string, string> = {};
      for (const sel of errorSelectors) {
        const el = document.querySelector(sel);
        if (el) {
          const text = el.textContent?.trim().substring(0, 100);
          if (text) errorTexts[sel] = text;
        }
      }

      return { visibleText, allLinks, iframes, videos, errorTexts };
    });

    log(streamId, `[DEBUG] ── No video found — page dump ──`, 'WARN');
    log(streamId, `[DEBUG] Intercepted network requests: ${interceptedCount}`, 'WARN');
    log(streamId, `[DEBUG] Page title: ${await page.title().catch(() => '?')}`, 'WARN');
    log(streamId, `[DEBUG] Visible text (first 1000 chars):\n${debugInfo.visibleText}`, 'WARN');

    if (debugInfo.videos.length > 0) {
      log(streamId, `[DEBUG] Video elements found: ${JSON.stringify(debugInfo.videos, null, 2)}`, 'WARN');
    } else {
      log(streamId, `[DEBUG] No <video> elements on page`, 'WARN');
    }

    if (debugInfo.iframes.length > 0) {
      log(streamId, `[DEBUG] iframes: ${JSON.stringify(debugInfo.iframes)}`, 'WARN');
    }

    if (Object.keys(debugInfo.errorTexts).length > 0) {
      log(streamId, `[DEBUG] Error/message elements: ${JSON.stringify(debugInfo.errorTexts, null, 2)}`, 'WARN');
    }

    if (debugInfo.allLinks.length > 0) {
      log(streamId, `[DEBUG] All links on page:\n${debugInfo.allLinks.join('\n')}`, 'WARN');
    }

  } catch (e: any) {
    log(streamId, `[DEBUG] Failed to dump page info: ${e.message}`, 'WARN');
  }
}

function resolveVidaraEmbedUrl(url: string): string {
  try {
    const parsed = new URL(url);
    if (parsed.hostname.includes('vidara.so') && parsed.pathname.startsWith('/e/')) {
      return url;
    }
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

  const targetUrl = isVidara ? resolveVidaraEmbedUrl(url) : url;

  log(streamId, `Starting scrape for URL: ${url}`);
  if (isVidara) log(streamId, `Vidara embed URL resolved → ${targetUrl}`);
  log(streamId, `Site detection → Vidara: ${isVidara}, Vidsonic: ${isVidsonic}, Vidnest: ${isVidnest}, Streamtape: ${isStreamtape}`);

  const scrapeAttempt = async (): Promise<ScrapeResult> => {
    let browser: any = null;
    let context: any = null;
    let page: any = null;

    try {
     // log(streamId, 'Launching browser...');
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
    //  log(streamId, 'Browser launched successfully');

    //  log(streamId, 'Creating new browser context...');
      context = await browser.newContext({
        viewport: { width: 1280, height: 900 },
        userAgent:
          'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
      });
      //log(streamId, 'Browser context created');

     // log(streamId, 'Opening primary page...');
      page = await context.newPage();
      log(streamId, 'Primary page opened');

      page.on('close', () => log(streamId, 'PAGE CLOSED EVENT FIRED', 'ERROR'));
      page.on('console', (msg: any) => {
        if (msg.type() === 'error') {
          log(streamId, `PAGE CONSOLE ERROR: ${msg.text()}`, 'ERROR');
        }
      });

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

      const interceptedVideos: { site: string; url: string }[] = [];

      log(streamId, 'Attaching network request interceptor...');
      page.on('request', (request: { url: () => string }) => {
        const reqUrl = request.url();
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
      let httpStatus = 200;
      log(streamId, `Navigating to: ${targetUrl}`);
      await page.goto(targetUrl, { waitUntil: 'domcontentloaded', timeout: 45000 })
        .then((response: any) => {
          httpStatus = response?.status() ?? 200;
          log(streamId, `Navigation response: HTTP ${httpStatus}`);
        })
        .catch((e: any) => {
          log(streamId, `Navigation error (non-fatal): ${e.message}`, 'WARN');
        });

      if (!isPageAlive(page)) {
        throw new Error('Primary page was closed unexpectedly after navigation');
      }

      if (!isStreamtape && !isVidara) {
        await page.waitForTimeout(3500).catch(() => {});
      }
     // log(streamId, 'Navigation phase completed');

      // ── Universal dead video check ─────────────────────────────────────────
      const pageHtml = isPageAlive(page)
        ? await page.content().catch(() => '')
        : '';

      const deadReason = detectDeadVideo(httpStatus, pageHtml);
      if (deadReason) {
        log(streamId, `Dead video detected (matched: ${deadReason}) — aborting`, 'WARN');
        await context?.close().catch(() => {});
        await browser?.close().catch(() => {});
        return {
          title: 'Dead Video',
          originalUrl: url,
          videos: [],
          dead: true,
          deadReason,
        };
      }

      // ── Player-specific extraction ─────────────────────────────────────────
      if (isVidara) {
        log(streamId, 'Vidara detected → JW Player extraction from embed page');
        await page.waitForTimeout(3000).catch(() => {});

        const vidaraUrl = await page.evaluate(() => {
          try {
            const p = (window as any).jwplayer?.();
            if (!p) return null;
            const file = p.getPlaylist?.()?.[0]?.file;
            if (file && !file.startsWith('blob:')) return file;
            const sources: any[] = p.getPlaylist?.()?.[0]?.sources ?? [];
            const valid = sources.find((s: any) => s.file && !s.file.startsWith('blob:'));
            return valid?.file ?? null;
          } catch { return null; }
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
        await page.click(playSelector, { force: true, timeout: 10000 }).catch((e: any) =>
          log(streamId, `Play click failed: ${e.message}`, 'WARN')
        );
        await page.waitForTimeout(2200).catch(() => {});

        const jwSource = await page.evaluate(() => {
          try {
            const player = (window as any).jwplayer?.();
            return player?.getPlaylist?.()?.[0]?.file ?? null;
          } catch { return null; }
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
        await page.click(playSelector, { force: true, timeout: 10000 }).catch((e: any) =>
          log(streamId, `Video.js click failed: ${e.message}`, 'WARN')
        );
        await page.waitForTimeout(2200).catch(() => {});

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
          } catch { return null; }
        }).catch(() => null);

        if (vjsSource && !String(vjsSource).startsWith('blob:')) {
          log(streamId, `Video.js source: ${String(vjsSource).substring(0, 90)}...`);
          interceptedVideos.push({ site: 'VJS-Internal', url: vjsSource });
        } else {
          log(streamId, 'No valid Video.js source found');
        }

      } else if (isStreamtape) {
        log(streamId, 'Streamtape detected → norobotlink / video src extraction');

        const streamtapeUrl = await page.evaluate(() => {
          try {
            const norobotEl = document.getElementById('norobotlink');
            if (norobotEl) {
              let href = norobotEl.textContent?.trim() || '';
              if (href.startsWith('//')) return 'https:' + href;
              if (href.startsWith('https://')) return href;
            }
            const video = document.querySelector('video');
            if (video) {
              let src = video.getAttribute('src') || '';
              if (src.startsWith('//')) return 'https:' + src;
              if (src.startsWith('https://')) return src;
            }
            return null;
          } catch { return null; }
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

      // ── Debug dump if nothing found ────────────────────────────────────────
      if (interceptedVideos.length === 0) {
        await dumpPageDebugInfo(page, streamId, interceptedVideos.length);
      }

     // log(streamId, 'Initiating cleanup...');
      await context?.close().catch((e: any) =>
        log(streamId, `Context close failed: ${e.message}`, 'WARN')
      );
      await browser?.close().catch((e: any) =>
        log(streamId, `Browser close failed: ${e.message}`, 'WARN')
      );

      const filteredVideos = filterVideoUrls(interceptedVideos, url);
      // log(streamId, `Final result: ${filteredVideos.length} valid video URLs after filtering`);
      log(streamId, 'Scrape completed successfully');

      return { title, originalUrl: url, videos: filteredVideos };

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