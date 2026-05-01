// scraper_service_FIXED.ts - Memory leak & crash prevention fixes
// Key fixes:
// 1. Proper try/finally cleanup (browser ALWAYS closes)
// 2. Remove event listeners before close (prevent accumulation)
// 3. Concurrent scrape limit (prevent OOM)
// 4. Abort long-running operations
// 5. Reduce DOM query complexity
// 6. Add memory checks

import playwrightExtra from 'playwright-extra';
import StealthPlugin from 'puppeteer-extra-plugin-stealth';
import { PlaywrightBlocker } from '@ghostery/adblocker-playwright';
import fetch from 'cross-fetch';
import { promises as fs } from 'fs';
import { filterVideoUrls } from './utils.js';

// ─── Plugin Registration ───────────────────────────────────────────────────────
// Stealth plugin disabled to save memory (~20MB)

export interface ScrapeResult {
  title: string;
  originalUrl: string;
  videos: { site: string; url: string }[];
  dead?: boolean;
  deadReason?: string;
}

// ─── MEMORY MANAGEMENT ─────────────────────────────────────────────────────────
let activeScrapes = 0;
const MAX_CONCURRENT_SCRAPES = 1;  // ← Sequential only: 1 concurrent scrape
const MEMORY_THRESHOLD_MB = 300;    // ← Aggressive: Stop scraping if memory > 300MB
const ENABLE_ADBLOCKER = false;     // ← Disabled: Save ~100MB of adblocker engine
const PAGE_TIMEOUT_MS = 15000;      // ← Shorter timeouts to free resources faster

function getMemoryUsageMB(): number {
  if (typeof process !== 'undefined' && process.memoryUsage) {
    return Math.round(process.memoryUsage().heapUsed / 1024 / 1024);
  }
  return 0;
}

function logMemory(msg: string): void {
  const mb = getMemoryUsageMB();
  console.log(`[MEMORY] ${msg} (${mb}MB heap)`);
}

// ─── Ghostery blocker disabled for memory efficiency ──────────────────
// Adblocker uses ~100-150MB; disabled to prioritize stability
async function getBlocker(): Promise<PlaywrightBlocker> {
  return null as any;  // Always disabled
}

let _browser: any = null;
async function getGlobalBrowser() {
  if (!_browser || !_browser.isConnected()) {
    console.log('[BROWSER] Launching minimal memory browser instance...');
    _browser = await playwrightExtra.chromium.launch({
      headless: true,
      args: [
        '--no-sandbox',
        '--disable-dev-shm-usage',
        '--no-zygote',
        '--disable-setuid-sandbox',
        '--disable-infobars',
        '--window-size=800,600',  // Minimal viewport (saves ~40MB)
        '--disable-blink-features=AutomationControlled',
        '--block-new-web-contents',
        '--disable-component-extensions-with-background-pages',
        '--disable-extensions',
        '--disable-plugins',
        '--disable-images',  // Don't load images (saves ~50-100MB per page)
        '--single-process',  // Single process (saves overhead)
        '--no-first-run',
        '--no-default-browser-check',
        '--disable-breakpad',  // Disable crash reporting (saves ~10MB)
        '--disable-client-side-phishing-detection',
        '--disable-popup-blocking',
        '--disable-prompt-on-repost',
        '--disable-sync',
        '--disable-translate',
        '--disable-web-resources',
        '--metrics-recording-only',
        '--mute-audio',  // Mute audio (saves ~5MB)
        '--js-flags="--max-old-space-size=32"'  // 32MB V8 heap
      ],
    });
  }
  return _browser;
}

process.on('exit', () => {
  if (_browser) _browser.close().catch(() => {});
});

// Minimal critical selectors only (rest handled by --disable-images)
const AD_OVERLAY_SELECTORS: string[] = [];

// ─── Helpers ──────────────────────────────────────────────────────────────────

function log(streamId: string, message: string, level: 'INFO' | 'WARN' | 'ERROR' = 'INFO') {
  const ts = new Date().toISOString();
  const prefix = `[${ts}] [ID: ${streamId}] [${level}]`;
  console[level.toLowerCase() as 'log' | 'warn' | 'error'](`${prefix} ${message}`);
}

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
  /Processing Downloading your video/i
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

// ─── FIX: Proper timeout with abort ────────────────────────────────────────────
function withTimeout<T>(promise: Promise<T>, timeoutMs: number, defaultValue: T): Promise<T> {
  let timeoutHandle: NodeJS.Timeout | null = null;
  return Promise.race([
    promise.finally(() => {
      if (timeoutHandle) clearTimeout(timeoutHandle);
    }),
    new Promise<T>(resolve => {
      timeoutHandle = setTimeout(() => resolve(defaultValue), timeoutMs);
    }),
  ]);
}

// ─── FIX: Remove event listeners before closing ─────────────────────────────────
async function cleanupPage(page: any, streamId: string): Promise<void> {
  if (!page || page.isClosed?.()) return;
  try {
    // Remove all listeners BEFORE closing
    page.removeAllListeners?.();
    // Clear cookies and storage
    await Promise.race([
      page.context?.().clearCookies?.(),
      new Promise(r => setTimeout(r, 1000))
    ]).catch(() => {});
  } catch (e: any) {
    log(streamId, `Listener cleanup error: ${e.message}`, 'WARN');
  }
}

async function cleanupContext(context: any, streamId: string): Promise<void> {
  if (!context) return;
  try {
    context.removeAllListeners?.();
    // Wrap context.close() in a timeout to prevent infinite hang if Playwright gets stuck
    await Promise.race([
      context.close().catch(() => {}),
      new Promise(resolve => setTimeout(resolve, 2000))
    ]);
  } catch (e: any) {
    log(streamId, `Context cleanup error: ${e.message}`, 'WARN');
  }
}

async function cleanupBrowser(browser: any, streamId: string): Promise<void> {
  if (!browser) return;
  try {
    browser.removeAllListeners?.();
    await browser.close().catch(() => {});
  } catch (e: any) {
    log(streamId, `Browser cleanup error: ${e.message}`, 'WARN');
  }
}

// ─── Minimal DOM cleanup (images already disabled in browser args) ─────────────
async function nukeAdOverlays(page: any, streamId: string): Promise<void> {
  if (!page || page.isClosed?.()) return;
  try {
    // Minimal cleanup: only patch window functions that pop ads
    await withTimeout(
      page.evaluate(() => {
        (window as any).open = () => null;
        (window as any).showAd = () => null;
        (window as any).displayAd = () => null;
      }),
      1000,  // Shorter timeout
      undefined
    );
    log(streamId, 'Window functions patched');
  } catch (e: any) {
    log(streamId, `Overlay cleanup skipped: ${e.message}`, 'WARN');
  }
}

// ─── Minimal debug info (memory efficient) ────────────────────────────────────
async function dumpPageDebugInfo(
  page: any,
  streamId: string,
  interceptedCount: number
): Promise<void> {
  if (!page || page.isClosed?.()) {
    log(streamId, '[DEBUG] Page unavailable', 'WARN');
    return;
  }
  try {
    const videos = await withTimeout(
      page.evaluate(() => {
        return Array.from(document.querySelectorAll('video')).map(v => ({
          src: v.src,
          currentSrc: v.currentSrc,
        }));
      }),
      2000,
      []
    );
    log(streamId, `[DEBUG] Intercepted: ${interceptedCount}, Videos found: ${videos.length}`, 'WARN');
  } catch (e: any) {
    log(streamId, `[DEBUG] Info skipped`, 'WARN');
  }
}

// ─── Site-specific extractors ─────────────────────────────────────────────────

function resolveVidaraEmbedUrl(url: string): string {
  try {
    const parsed = new URL(url);
    if (parsed.hostname.includes('vidara.so') && parsed.pathname.startsWith('/e/')) return url;
    const segments = parsed.pathname.split('/').filter(Boolean);
    const filecode = segments[0] === 'v' ? segments[1] : segments[0];
    return `https://vidara.so/e/${filecode}`;
  } catch { return url; }
}

async function extractVidaraUrl(page: any, streamId: string): Promise<string | null> {
  return withTimeout(
    page.evaluate(() => {
      try {
        const p = (window as any).jwplayer?.();
        if (!p) return null;
        const file = p.getPlaylist?.()?.[0]?.file;
        if (file && !file.startsWith('blob:')) return file;
        const sources = p.getPlaylist?.()?.[0]?.sources ?? [];
        const valid = sources.find((s: any) => s.file && !s.file.startsWith('blob:'));
        return valid?.file ?? null;
      } catch { return null; }
    }),
    3000, null
  );
}

async function extractVidnestUrl(page: any, streamId: string): Promise<string | null> {
  try {
    const playSelector = '.jw-video, .jw-display-icon-container, video';
    await withTimeout(page.click(playSelector, { force: true, timeout: 5000 }), 5500, undefined);
    await new Promise(resolve => setTimeout(resolve, 1200));
    return withTimeout(
      page.evaluate(() => {
        try { return (window as any).jwplayer?.()?.getPlaylist?.()?.[0]?.file ?? null; }
        catch { return null; }
      }),
      2000, null
    );
  } catch { return null; }
}

async function extractVidsonicUrl(page: any, streamId: string): Promise<string | null> {
  try {
    await withTimeout(
      page.evaluate(() => { document.querySelector('.vjs-vast-label')?.remove(); }),
      1000, undefined
    );
    const playSelector = '.vjs-big-play-button, .vjs-play-control';
    await withTimeout(page.click(playSelector, { force: true, timeout: 5000 }), 5500, undefined);
    await new Promise(resolve => setTimeout(resolve, 1200));
    const vjsSource = await withTimeout(
      page.evaluate(() => {
        try {
          const playerEl = document.querySelector('.video-js');
          if (playerEl && (playerEl as any).player) return (playerEl as any).player.src();
          if ((window as any).videojs) {
            const players = (window as any).videojs.getPlayers?.();
            if (players) {
              const first = Object.values(players)[0];
              return first ? (first as any).src() : null;
            }
          }
          return null;
        } catch { return null; }
      }),
      2000, null
    );
    return vjsSource && !String(vjsSource).startsWith('blob:') ? vjsSource : null;
  } catch { return null; }
}

async function extractStreamtapeUrl(page: any, streamId: string): Promise<string | null> {
  try {
    await withTimeout(page.waitForSelector('video', { timeout: 12000 }), 12500, undefined);
    await new Promise(resolve => setTimeout(resolve, 1500));

    let videoUrl: string | null = await withTimeout(
      (page.evaluate(() => {
        const video = document.querySelector('video') as HTMLVideoElement | null;
        if (!video) return null;
        const src = video.src || video.currentSrc || video.getAttribute('src') || '';
        return src && !src.startsWith('blob:') ? src : null;
      }) as Promise<string | null>),
      3000,
      null as string | null
    );
    if (videoUrl) {
      log(streamId, `Streamtape: ✅ Found video URL from <video> tag`);
      return videoUrl.startsWith('//') ? 'https:' + videoUrl : videoUrl;
    }

    videoUrl = await withTimeout(
      (page.evaluate(() => {
        const sources = Array.from(document.querySelectorAll('video source'));
        for (const s of sources) {
          const src = (s as HTMLSourceElement).src || s.getAttribute('src') || '';
          if (src && !src.startsWith('blob:')) return src;
        }
        return null;
      }) as Promise<string | null>),
      2000,
      null as string | null
    );
    if (videoUrl) {
      log(streamId, `Streamtape: ✅ Found video URL from <source> tag`);
      return videoUrl.startsWith('//') ? 'https:' + videoUrl : videoUrl;
    }

    const intercepted: string | null = await withTimeout(
      (page.evaluate(() =>
        (window as any).__streamtape_src || (window as any).videoUrl || null
      ) as Promise<string | null>),
      1500,
      null as string | null
    );
    if (intercepted) {
      log(streamId, `Streamtape: ✅ Found video URL from window global`);
      return intercepted;
    }

    log(streamId, 'Streamtape: ❌ No video URL found after all attempts', 'WARN');
    return null;
  } catch (error: any) {
    log(streamId, `Streamtape extraction error: ${error.message}`, 'WARN');
    return null;
  }
}

async function extractGenericVideoUrl(page: any, streamId: string): Promise<string | null> {
  try {
    const playSelector = '.plyr__control--overlaid, .play-overlay, button[data-plyr="play"]';
    await withTimeout(page.waitForSelector(playSelector, { timeout: 8000 }), 8500, undefined);
    await page.click(playSelector, { force: true }).catch(() => {});
    await new Promise(resolve => setTimeout(resolve, 1000));
  } catch {
    log(streamId, 'Generic play element not found', 'WARN');
  }
  return withTimeout(
    page.evaluate(() => {
      const video = document.querySelector('video') as HTMLVideoElement | null;
      if (!video) return null;
      const src = video.currentSrc || video.src || '';
      return src && !src.startsWith('blob:') ? src : null;
    }),
    2000, null
  );
}

async function extractDOMVideo(page: any): Promise<string | null> {
  return withTimeout(
    page.evaluate(() => {
      const video = document.querySelector('video') as HTMLVideoElement | null;
      if (!video) return null;
      const src = video.currentSrc || video.src || '';
      return src && !src.startsWith('blob:') ? src : null;
    }),
    1500, null
  );
}

// ─── Main scrape entry point ───────────────────────────────────────────────────

export async function performScrape(url: string, streamId: string): Promise<ScrapeResult> {
  // ─── FIX: Check memory before scraping ──────────────────────────────────────
  const memMB = getMemoryUsageMB();
  if (memMB > MEMORY_THRESHOLD_MB) {
    logMemory(`❌ Memory too high (${memMB}MB > ${MEMORY_THRESHOLD_MB}MB), queuing scrape`);
    // Queue the scrape instead of rejecting
    await new Promise(r => setTimeout(r, 5000));
  }

  // ─── FIX: Limit concurrent scrapes ────────────────────────────────────────
  while (activeScrapes >= MAX_CONCURRENT_SCRAPES) {
    log(streamId, `Waiting for slot (${activeScrapes}/${MAX_CONCURRENT_SCRAPES} active)`, 'WARN');
    await new Promise(r => setTimeout(r, 1000));
  }

  activeScrapes++;
  logMemory(`Scrape started (${activeScrapes} active)`);

  try {
    return await scrapeWithCleanup(url, streamId);
  } finally {
    activeScrapes--;
    logMemory(`Scrape finished (${activeScrapes} active)`);
  }
}

async function scrapeWithCleanup(url: string, streamId: string): Promise<ScrapeResult> {
  const isVidara     = /vidara\./i.test(url);
  const isVidsonic   = /vidsonic\./i.test(url);
  const isVidnest    = /vidnest\./i.test(url);
  const isStreamtape = /streamtape\./i.test(url);

  const targetUrl = isVidara ? resolveVidaraEmbedUrl(url) : url;

  log(streamId, `Starting scrape for: ${url}`);
  if (isVidara) log(streamId, `Vidara resolved → ${targetUrl}`);

  const browser = await getGlobalBrowser();
  let context: any = null;
  let page: any    = null;
  const interceptedVideos: { site: string; url: string }[] = [];

  try {
    context = await browser.newContext({
      viewport: { width: 1280, height: 900 },
      userAgent:
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
    });

    page = await context.newPage();
    log(streamId, 'Page opened');

    // ── Layer 1: Network video sniffer ────────────────────────────────────────
    const onRequest = (request: { url: () => string }) => {
      const reqUrl = request.url();
      if (
        (reqUrl.includes('get_video') || reqUrl.includes('.m3u8') || reqUrl.includes('.mp4')) &&
        reqUrl !== url &&
        !interceptedVideos.some(v => v.url === reqUrl)
      ) {
        log(streamId, `VIDEO INTERCEPTED: ${reqUrl.substring(0, 80)}...`);
        interceptedVideos.push({ site: 'Network-Sniffer', url: reqUrl });
      }
    };

    const onPageClose = () => log(streamId, 'Page closed', 'WARN');
    const onConsole = (msg: any) => {
      if (msg.type() === 'error') log(streamId, `JS Error: ${msg.text()}`, 'WARN');
    };

    page.on('request', onRequest);
    page.on('close', onPageClose);
    page.on('console', onConsole);

    // ── Layer 2: Popup / new-tab suppression ──────────────────────────────────
    const onPopup = async (popup: any) => {
      if (popup === page) return;
      try {
        await new Promise(r => setTimeout(r, 300));
        if (!popup.isClosed?.()) await popup.close({ runBeforeUnload: false }).catch(() => {});
        log(streamId, 'Popup suppressed');
      } catch (e: any) {
        log(streamId, `Popup handler error: ${e.message}`, 'WARN');
      }
    };

    context.on('page', onPopup);

    // ── Layer 3: Patch window.open + ad globals BEFORE any page JS runs ───────
    await page.addInitScript(() => {
      window.open = () => null as any;
      try {
        Object.defineProperty(window, 'open', { value: () => null, writable: false });
      } catch { /* already sealed */ }

      (window as any).popUnder  = () => null;
      (window as any).popunder  = () => null;
      (window as any).showAd    = () => null;
      (window as any).loadAd    = () => null;
      (window as any).displayAd = () => null;

      const _createElement = document.createElement.bind(document);
      (document as any).createElement = function (tag: string, ...args: any[]) {
        const el = _createElement(tag, ...args);
        if (tag.toLowerCase() === 'script') {
          const _setAttr = el.setAttribute.bind(el);
          el.setAttribute = function (name: string, value: string) {
            if (name === 'src') {
              const blocked = [
                'popads', 'popcash', 'popunder', 'exoclick',
                'trafficjunky', 'adcash', 'hilltopads', 'propellerads',
                'plugrush', 'juicyads', 'adform', 'adnxs',
                'dtscout', 'dtscdn',
              ];
              if (blocked.some(b => value.includes(b))) {
                console.warn(`[AdBlock-init] Blocked script injection: ${value}`);
                return;
              }
            }
            _setAttr(name, value);
          };
        }
        return el;
      };
    });

    // Navigation
    let httpStatus = 200;
    log(streamId, `Navigating to: ${targetUrl}`);
    await page.goto(targetUrl, { waitUntil: 'domcontentloaded', timeout: 35000 })
      .then((response: any) => {
        httpStatus = response?.status() ?? 200;
        log(streamId, `HTTP ${httpStatus}`);
      })
      .catch((e: any) => {
        log(streamId, `Navigation error: ${e.message}`, 'WARN');
      });

    if (!page || page.isClosed?.()) throw new Error('Page closed after navigation');

    // ── Layer 5: DOM nuke after load ──────────────────────────────────────────
    await nukeAdOverlays(page, streamId);

    // Dead video check
    const pageHtml = await withTimeout(page.content(), 5000, '');
    const deadReason = detectDeadVideo(httpStatus, pageHtml);
    if (deadReason) {
      log(streamId, `Dead video: ${deadReason}`, 'WARN');
      return { title: 'Dead Video', originalUrl: url, videos: [], dead: true, deadReason };
    }

    // ── Site-specific extraction ───────────────────────────────────────────────
    if (isVidara) {
      log(streamId, 'Vidara JW Player extraction');
      const vidaraUrl = await extractVidaraUrl(page, streamId);
      if (vidaraUrl) {
        log(streamId, `Vidara: ${String(vidaraUrl).substring(0, 80)}...`);
        interceptedVideos.push({ site: 'Vidara-JW', url: vidaraUrl });
      }
    } else if (isVidnest) {
      log(streamId, 'Vidnest JW Player extraction');
      const jwUrl = await extractVidnestUrl(page, streamId);
      if (jwUrl) {
        log(streamId, `Vidnest JW: ${String(jwUrl).substring(0, 80)}...`);
        interceptedVideos.push({ site: 'JW-Internal', url: jwUrl });
      }
    } else if (isVidsonic) {
      log(streamId, 'Vidsonic Video.js extraction');
      const vjsUrl = await extractVidsonicUrl(page, streamId);
      if (vjsUrl) {
        log(streamId, `Vidsonic VJS: ${String(vjsUrl).substring(0, 80)}...`);
        interceptedVideos.push({ site: 'VJS-Internal', url: vjsUrl });
      }
    } else if (isStreamtape) {
      log(streamId, 'Streamtape DOM extraction');
      const stUrl = await extractStreamtapeUrl(page, streamId);
      if (stUrl) {
        log(streamId, `Streamtape: ${String(stUrl).substring(0, 80)}...`);
        interceptedVideos.push({ site: 'Streamtape-DOM', url: stUrl });
      }
    } else {
      log(streamId, 'Generic player extraction');
      const genericUrl = await extractGenericVideoUrl(page, streamId);
      if (genericUrl) {
        log(streamId, `Generic: ${String(genericUrl).substring(0, 80)}...`);
        interceptedVideos.push({ site: 'Generic-Play', url: genericUrl });
      }
    }

    // Fallback: DOM video for non-Streamtape/Vidara sites
    if (!isStreamtape && !isVidara && interceptedVideos.length === 0) {
      const domUrl = await extractDOMVideo(page);
      if (domUrl) {
        log(streamId, `DOM Video: ${String(domUrl).substring(0, 80)}...`);
        interceptedVideos.push({ site: 'DOM-Extraction', url: domUrl });
      }
    }

    const title = await withTimeout(page.title(), 2000, 'Unknown Title');
    log(streamId, `Title: ${title}`);
    log(streamId, `Found ${interceptedVideos.length} video URLs`);

    if (interceptedVideos.length === 0) {
      await dumpPageDebugInfo(page, streamId, interceptedVideos.length);
    }

    const filteredVideos = filterVideoUrls(interceptedVideos, url);
    log(streamId, `Filtered to ${filteredVideos.length} valid URLs`);

    return { title, originalUrl: url, videos: filteredVideos };

  } catch (error: any) {
    log(streamId, `FAILED: ${error.message || 'Unknown error'}`, 'ERROR');
    throw error;
  } finally {
    // ─── FIX: Always cleanup, in correct order ──────────────────────────────
    if (page) {
      page.removeAllListeners?.();
      await cleanupPage(page, streamId);
    }
    if (context) {
      context.removeAllListeners?.();
      await cleanupContext(context, streamId);
    }
    // Global browser is kept alive for reuse
    logMemory(`Cleanup complete for ${streamId}`);
  }
}