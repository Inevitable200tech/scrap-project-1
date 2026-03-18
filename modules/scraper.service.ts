import playwrightExtra from 'playwright-extra';
import StealthPlugin from 'puppeteer-extra-plugin-stealth';
import { filterVideoUrls } from './utils.js';

playwrightExtra.chromium.use(StealthPlugin());

export interface ScrapeResult {
  title: string;
  originalUrl: string;
  videos: { site: string; url: string }[];
  dead?: boolean;
  deadReason?: string;
}

function log(streamId: string, message: string, level: 'INFO' | 'WARN' | 'ERROR' = 'INFO') {
  const ts = new Date().toISOString();
  const prefix = `[${ts}] [ID: ${streamId}] [${level}]`;
  console[level.toLowerCase() as 'log' | 'warn' | 'error'](`${prefix} ${message}`);
}

class PageAliveChecker {
  private lastCheck = 0;
  private cached = true;

  check(p: any): boolean {
    try {
      return p && !p.isClosed?.();
    } catch {
      return false;
    }
  }
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

// Timeout wrapper for cleaner async operations
function withTimeout<T>(promise: Promise<T>, timeoutMs: number, defaultValue: T): Promise<T> {
  return Promise.race([
    promise,
    new Promise<T>(resolve => setTimeout(() => resolve(defaultValue), timeoutMs))
  ]);
}

async function dumpPageDebugInfo(
  page: any,
  streamId: string,
  interceptedCount: number
): Promise<void> {
  if (!page || page.isClosed?.()) {
    log(streamId, '[DEBUG] Page unavailable for debug info', 'WARN');
    return;
  }

  try {
    const debugInfo = await withTimeout(
      page.evaluate(() => {
        const clone = document.body.cloneNode(true) as HTMLElement;
        clone.querySelectorAll('script, style, noscript').forEach(el => el.remove());
        const visibleText = (clone.innerText || clone.textContent || '')
          .replace(/\s+/g, ' ')
          .trim()
          .substring(0, 10000);

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
      }),
      5000,
      { visibleText: '', allLinks: [], iframes: [], videos: [], errorTexts: {} }
    );

    log(streamId, `[DEBUG] ── No video found – page dump ──`, 'WARN');
    log(streamId, `[DEBUG] Intercepted requests: ${interceptedCount}`, 'WARN');
    if (debugInfo.visibleText) {
      log(streamId, `[DEBUG] Visible text: ${debugInfo.visibleText.substring(0, 200)}`, 'WARN');
    }
    if (debugInfo.videos.length > 0) {
      log(streamId, `[DEBUG] Video elements: ${JSON.stringify(debugInfo.videos)}`, 'WARN');
    }
    if (Object.keys(debugInfo.errorTexts).length > 0) {
      log(streamId, `[DEBUG] Error texts: ${JSON.stringify(debugInfo.errorTexts)}`, 'WARN');
    }
  } catch (e: any) {
    log(streamId, `[DEBUG] Dump failed: ${e.message}`, 'WARN');
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
      } catch {
        return null;
      }
    }),
    3000,
    null
  );
}

async function extractVidnestUrl(page: any, streamId: string): Promise<string | null> {
  try {
    const playSelector = '.jw-video, .jw-display-icon-container, video';
    await withTimeout(
      page.click(playSelector, { force: true, timeout: 5000 }),
      5500,
      undefined
    );
    await new Promise(resolve => setTimeout(resolve, 1200));

    return withTimeout(
      page.evaluate(() => {
        try {
          return (window as any).jwplayer?.()?.getPlaylist?.()?.[0]?.file ?? null;
        } catch {
          return null;
        }
      }),
      2000,
      null
    );
  } catch {
    return null;
  }
}

async function extractVidsonicUrl(page: any, streamId: string): Promise<string | null> {
  try {
    await withTimeout(
      page.evaluate(() => {
        document.querySelector('.vjs-vast-label')?.remove();
      }),
      1000,
      undefined
    );

    const playSelector = '.vjs-big-play-button, .vjs-play-control';
    await withTimeout(
      page.click(playSelector, { force: true, timeout: 5000 }),
      5500,
      undefined
    );
    await new Promise(resolve => setTimeout(resolve, 1200));

    const vjsSource = await withTimeout(
      page.evaluate(() => {
        try {
          const playerEl = document.querySelector('.video-js');
          if (playerEl && (playerEl as any).player) return (playerEl as any).player.src();
          if ((window as any).videojs) {
            const players = (window as any).videojs.getPlayers?.();
            if (players) {
              const firstPlayer = Object.values(players)[0];
              return firstPlayer ? (firstPlayer as any).src() : null;
            }
          }
          return null;
        } catch {
          return null;
        }
      }),
      2000,
      null
    );

    return vjsSource && !String(vjsSource).startsWith('blob:') ? vjsSource : null;
  } catch {
    return null;
  }
}

// ✅ FIXED: Wait for Streamtape lazy-loading
async function extractStreamtapeUrl(page: any, streamId: string): Promise<string | null> {
  try {
    const videoUrl = await page.evaluate(() => {
      const video = document.querySelector('video');
      if (!video) return null;
      
      const src = video.src || video.currentSrc || video.getAttribute('src');
      if (src && src.length > 0 && !src.startsWith('blob:')) {
        return src.startsWith('//') ? 'https:' + src : src;
      }
      return null;
    });

    if (videoUrl) {
      log(streamId, `Streamtape: ✅ Found video URL from <video> tag`);
      return videoUrl;
    }

    log(streamId, 'Streamtape: ❌ No video URL found');
    return null;

  } catch (error: any) {
    log(streamId, `Streamtape extraction error: ${error.message}`, 'WARN');
    return null;
  }
}

async function extractGenericVideoUrl(page: any, streamId: string): Promise<string | null> {
  try {
    const playSelector = '.plyr__control--overlaid, .play-overlay, button[data-plyr="play"]';
    await withTimeout(
      page.waitForSelector(playSelector, { timeout: 8000 }),
      8500,
      undefined
    );
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
    2000,
    null
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
    1500,
    null
  );
}

export async function performScrape(url: string, streamId: string): Promise<ScrapeResult> {
  const isVidara = /vidara\./i.test(url);
  const isVidsonic = /vidsonic\./i.test(url);
  const isVidnest = /vidnest\./i.test(url);
  const isStreamtape = /streamtape\./i.test(url);

  const targetUrl = isVidara ? resolveVidaraEmbedUrl(url) : url;

  log(streamId, `Starting scrape for: ${url}`);
  if (isVidara) log(streamId, `Vidara resolved → ${targetUrl}`);

  const scrapeAttempt = async (): Promise<ScrapeResult> => {
    let browser: any = null;
    let context: any = null;
    let page: any = null;
    const interceptedVideos: { site: string; url: string }[] = [];

    try {
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

      context = await browser.newContext({
        viewport: { width: 1280, height: 900 },
        userAgent:
          'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
      });

      page = await context.newPage();
      log(streamId, 'Page opened');

      // Network interceptor - catch videos early
      page.on('request', (request: { url: () => string }) => {
        const reqUrl = request.url();
        const isBlacklisted = /yandex|mc\.ru|analytics|pixel|google|\.ts($|\?)|dtscout|dtscdn|doubleclick|adnxs|adsystem|googlesyndication|amazon-adsystem|outbrain|taboola|adsbygoogle/i.test(reqUrl);

        if (
          !isBlacklisted &&
          (reqUrl.includes('get_video') || reqUrl.includes('.m3u8') || reqUrl.includes('.mp4'))
        ) {
          if (reqUrl !== url && !interceptedVideos.some(v => v.url === reqUrl)) {
            log(streamId, `VIDEO: ${reqUrl.substring(0, 80)}...`);
            interceptedVideos.push({ site: 'Network-Sniffer', url: reqUrl });
          }
        }
      });

      page.on('close', () => log(streamId, 'Page closed', 'WARN'));
      page.on('console', (msg: any) => {
        if (msg.type() === 'error') {
          log(streamId, `JS Error: ${msg.text()}`, 'WARN');
        }
      });

      // Popup handler
      context.on('page', async (popup: any) => {
        if (popup === page) return;
        try {
          await new Promise(r => setTimeout(r, 500));
          if (!popup.isClosed?.()) await popup.close({ runBeforeUnload: false }).catch(() => {});
        } catch (e: any) {
          log(streamId, `Popup handler error: ${e.message}`, 'WARN');
        }
      });

      // Navigation with early dead video detection
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

      if (!page || page.isClosed?.()) {
        throw new Error('Page closed after navigation');
      }

      // Check for dead video immediately after navigation
      const pageHtml = await withTimeout(
        page.content(),
        5000,
        ''
      );

      const deadReason = detectDeadVideo(httpStatus, pageHtml);
      if (deadReason) {
        log(streamId, `Dead video: ${deadReason}`, 'WARN');
        return {
          title: 'Dead Video',
          originalUrl: url,
          videos: [],
          dead: true,
          deadReason,
        };
      }

      // Parallel extraction based on site type
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

      const title = await withTimeout(
        page.title(),
        2000,
        'Unknown Title'
      );

      log(streamId, `Title: ${title}`);
      log(streamId, `Found ${interceptedVideos.length} video URLs`);

      if (interceptedVideos.length === 0) {
        await dumpPageDebugInfo(page, streamId, interceptedVideos.length);
      }

      await context?.close().catch(() => {});
      await browser?.close().catch(() => {});

      const filteredVideos = filterVideoUrls(interceptedVideos, url);
      log(streamId, `Filtered to ${filteredVideos.length} valid URLs`);

      return { title, originalUrl: url, videos: filteredVideos };

    } catch (error: any) {
      log(streamId, `FAILED: ${error.message || 'Unknown error'}`, 'ERROR');

      await context?.close().catch(() => {});
      await browser?.close().catch(() => {});

      throw error;
    }
  };

  return scrapeAttempt();
}