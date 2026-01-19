// server.ts — With comprehensive request & flow debugging

import express, { Request, Response, NextFunction } from 'express';
import playwrightExtra from 'playwright-extra';
import StealthPlugin from 'puppeteer-extra-plugin-stealth';
import * as cheerio from 'cheerio';
import rateLimit from 'express-rate-limit';
import * as os from 'os';
import * as path from 'path';

playwrightExtra.chromium.use(StealthPlugin());

// Persistent context
let persistentContext: any = null;

async function getPersistentContext() {
  if (!persistentContext) {
    const userDataDir = path.join(os.tmpdir(), 'dropmms-api-profile');
    console.log(`[DEBUG] Creating persistent profile: ${userDataDir}`);

    persistentContext = await playwrightExtra.chromium.launchPersistentContext(
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

    await persistentContext.addInitScript(() => {
      Object.defineProperty(navigator, 'webdriver', { get: () => false });
      delete (window as any).cdc_adoQpoasnfa76pfcZLmcfl_Array;
      delete (window as any).cdc_adoQpoasnfa76pfcZLmcfl_Promise;
      delete (window as any).cdc_adoQpoasnfa76pfcZLmcfl_Symbol;
    });
  }
  return persistentContext;
}

// Rate limiting (60/min — should be safe now)
const limiter = rateLimit({
  windowMs: 60 * 1000,
  max: 60,
  message: { error: 'Rate limit exceeded. Please wait.' },
  standardHeaders: true,
  legacyHeaders: false,
});

const app = express();
const PORT = process.env.PORT || 3000;

app.use(express.json({ limit: '10mb' }));
app.use(limiter);

// ── GLOBAL REQUEST LOGGER ───────────────────────────────────────────────────
app.use((req: Request, res: Response, next: NextFunction) => {
  const start = Date.now();

  console.log(`\n[INCOMING] ${req.method} ${req.originalUrl} | IP: ${req.ip}`);
  console.log(`[HEADERS] ${JSON.stringify(req.headers, null, 2)}`);

  if (req.body && Object.keys(req.body).length > 0) {
    console.log(`[BODY] ${JSON.stringify(req.body, null, 2)}`);
  } else {
    console.log('[BODY] Empty or no JSON body');
  }

  res.on('finish', () => {
    const duration = Date.now() - start;
    console.log(`[OUTGOING] ${req.method} ${req.originalUrl} → ${res.statusCode} (${duration}ms)`);
  });

  next();
});

// Allowed domains
const ALLOWED_PREFIXES = [
  'http://dropmms.co',
  'https://dropmms.co',
  'https://videmms24.com'
];

const zipDomains = ['upfiles.com', 'file-upload.org', 'zapupload.top', 'frdl.io'];
const videoDomains = ['strmup.cc', 'luluvid.com', 'vidnest.io', 'vidoza.net', 'streamtape.com', 'vinovo.to', 'up4fun.top'];
const imageExtensions = ['.jpg', '.jpeg', '.png', '.gif', '.webp'];

// ── SCRAPE ENDPOINT ─────────────────────────────────────────────────────────
app.post('/api/scrape', async (req: Request, res: Response) => {
  console.log('[ROUTE] /api/scrape handler STARTED');

  const { url } = req.body;

  if (!url || typeof url !== 'string') {
    console.log('[VALIDATION] Failed: Missing or invalid "url"');
    return res.status(400).json({ error: 'Missing or invalid "url"' });
  }

  console.log(`[VALIDATION] URL received: ${url}`);

  if (!ALLOWED_PREFIXES.some(prefix => url.startsWith(prefix))) {
    console.log(`[VALIDATION] Failed: Disallowed domain → ${url}`);
    return res.status(403).json({ error: 'Only dropmms.co / videmms24.com URLs allowed' });
  }

  console.log('[VALIDATION] Passed — proceeding to scrape');

  try {
    const context = await getPersistentContext();
    const page = await context.newPage();

    console.log(`[PLAYWRIGHT] Navigating to: ${url}`);
    await page.goto(url, { waitUntil: 'networkidle', timeout: 60000 });

    const pageTitle = await page.title();
    console.log(`[PLAYWRIGHT] Loaded page title: "${pageTitle}"`);

    if (pageTitle.includes('Just a moment') || pageTitle.includes('Attention Required')) {
      console.log('[PLAYWRIGHT] Cloudflare challenge → waiting 15–25s');
      await page.waitForTimeout(15000 + Math.random() * 10000);
    }

    console.log('[PLAYWRIGHT] Applying scroll & delay');
    await page.waitForTimeout(5000 + Math.random() * 3000);
    await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
    await page.waitForTimeout(3000);

    const html = await page.content();
    await page.close();
    console.log('[PLAYWRIGHT] Page content fetched — length:', html.length);

    const $ = cheerio.load(html);

    let title = $('h1.ipsType_pageTitle span.ipsContained span').first().text().trim();
    if (!title) {
      title = $('h1.ipsType_pageTitle').first().text().trim() || 'Untitled Thread';
    }
    console.log(`[EXTRACTION] Title: "${title}"`);

    const contentWraps = $('.cPost_contentWrap');
    console.log(`[EXTRACTION] Found ${contentWraps.length} .cPost_contentWrap blocks`);

    const videos: string[] = [];
    const images: string[] = [];
    const zips: string[] = [];

    contentWraps.each((i, wrap) => {
      const $wrap = $(wrap);
      console.log(`\n[Block ${i + 1}] ───────────────────────────────`);

      const textPreview = $wrap.text().trim().replace(/\s+/g, ' ').slice(0, 300);
      console.log(`Text preview: ${textPreview}...`);

      // Links
      const links: string[] = [];
      $wrap.find('a[href^="http"]').each((_, a) => {
        const href = $(a).attr('href')?.trim() || '';
        if (href) {
          links.push(href);
          console.log(`  Link: ${href}`);
        }
      });

      links.forEach(href => {
        const hostname = new URL(href).hostname.toLowerCase();
        if (zipDomains.some(d => hostname.includes(d))) {
          if (!zips.includes(href)) zips.push(href);
          console.log(`    → Classified as ZIP`);
        } else if (videoDomains.some(d => hostname.includes(d))) {
          if (!videos.includes(href)) videos.push(href);
          console.log(`    → Classified as VIDEO`);
        }
      });

      // Images
      $wrap.find('img').each((_, img) => {
        let src = $(img).attr('src') || $(img).attr('data-src') || '';
        if (!src.startsWith('http')) return;

        const ext = src.toLowerCase().split('?')[0].slice(-5);
        if (imageExtensions.some(e => ext.endsWith(e))) {
          const $parentA = $(img).closest('a[href]');
          let finalSrc = src;
          if ($parentA.length) {
            const parentHref = $parentA.attr('href')?.trim() || '';
            const parentExt = parentHref.toLowerCase().split('?')[0].slice(-5);
            if (imageExtensions.some(e => parentExt.endsWith(e))) {
              finalSrc = parentHref;
            }
          }
          if (!images.includes(finalSrc)) {
            images.push(finalSrc);
            console.log(`    → IMAGE captured: ${finalSrc} (class: ${$(img).attr('class') || 'none'})`);
          }
        }
      });
    });

    console.log(`\n[SUMMARY] Videos: ${videos.length} | Images: ${images.length} | Zips: ${zips.length}`);

    return res.status(200).json({
      title,
      videos,
      images,
      zips
    });
  } catch (error: any) {
    console.error(`[ERROR] Scrape failed for ${req.body?.url || 'unknown'}:`);
    console.error(error.stack || error.message);
    return res.status(500).json({
      error: 'Failed to scrape page',
      details: error.message || 'Internal error'
    });
  }
});

// Health check with debug
app.get('/health', (req, res) => {
  console.log('[HEALTH] Check requested');
  res.status(200).json({ status: 'ok', uptime: process.uptime() });
});

// ── Catch-all for unmatched routes (helps diagnose 404) ──────────────────────
app.use((req: Request, res: Response) => {
  console.log(`[404 NOT FOUND] ${req.method} ${req.originalUrl} | IP: ${req.ip}`);
  console.log(`[404] Body was: ${JSON.stringify(req.body, null, 2)}`);
  res.status(404).json({ error: 'Endpoint not found' });
});

app.listen(PORT, () => {
  console.log(`\nServer running on http://localhost:${PORT}`);
  console.log('Debug mode enabled — detailed request & extraction logs active');
  console.log('POST /api/scrape → { "url": "https://..." }\n');
});