// server.ts

import express, { Request, Response } from 'express';
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
    console.log(`Persistent profile: ${userDataDir}`);

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

// Rate limiting: 10 requests per IP per minute
const limiter = rateLimit({
  windowMs: 60 * 1000,   // still 1 minute
  max: 60,               // ← allow 1 request per second (60/min)
  message: { error: 'Rate limit exceeded. Please wait.' },
  standardHeaders: true,
  legacyHeaders: false,
});

const app = express();
const PORT = process.env.PORT || 3000;

app.use(express.json());
app.use(limiter);

// Allowed domains
const ALLOWED_PREFIXES = [
  'http://dropmms.co',
  'https://dropmms.co',
  'https://videmms24.com'
];

// Domain lists for classification
const zipDomains = [
  'upfiles.com',
  'file-upload.org',
  'zapupload.top',
  'frdl.io'
  // ← add more file hosters here
];

const videoDomains = [
  'strmup.cc',
  'luluvid.com',
  'vidnest.io',
  'vidoza.net',
  'streamtape.com',
  'vinovo.to',
  'up4fun.top'
  // ← add more video hosts here
];

const imageExtensions = ['.jpg', '.jpeg', '.png', '.gif', '.webp'];

app.post('/api/scrape', async (req: Request, res: Response) => {
  const { url } = req.body;

  if (!url || typeof url !== 'string') {
    return res.status(400).json({ error: 'Missing or invalid "url"' });
  }

  if (!ALLOWED_PREFIXES.some(prefix => url.startsWith(prefix))) {
    return res.status(403).json({ error: 'Only dropmms.co / videmms24.com URLs allowed' });
  }

  try {
    const context = await getPersistentContext();
    const page = await context.newPage();

    console.log(`\nScraping: ${url}`);

    await page.goto(url, { waitUntil: 'networkidle', timeout: 60000 });

    // Cloudflare handling
    try {
      const title = await page.title();
      if (title.includes('Just a moment') || title.includes('Attention Required')) {
        console.log('Cloudflare challenge detected — waiting...');
        await page.waitForTimeout(15000 + Math.random() * 10000);
      }
    } catch {}

    await page.waitForTimeout(5000 + Math.random() * 3000);
    await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
    await page.waitForTimeout(3000);

    const html = await page.content();
    await page.close();

    const $ = cheerio.load(html);

    // ── Extract title ────────────────────────────────────────────────────────
    let title = $('h1.ipsType_pageTitle span.ipsContained span').first().text().trim();
    if (!title) {
      title = $('h1.ipsType_pageTitle').first().text().trim() || 'Untitled Thread';
    }
    console.log(`Title: ${title}`);

    // ── Focus only on .cPost_contentWrap blocks ─────────────────────────────
    const contentWraps = $('.cPost_contentWrap');

    console.log(`Found ${contentWraps.length} .cPost_contentWrap blocks`);

    const videos: string[] = [];
    const images: string[] = [];
    const zips: string[] = [];

    contentWraps.each((i, wrap) => {
      const $wrap = $(wrap);

      console.log(`\n--- Content block ${i + 1} ---`);

      // Text preview (first 400 chars)
      const textPreview = $wrap.text().trim().replace(/\s+/g, ' ').slice(0, 400);
      console.log(`Text preview: ${textPreview}...`);

      // ── All <a href="http..."> links inside this block ───────────────────
      const links: string[] = [];
      $wrap.find('a[href^="http"]').each((_, a) => {
        const href = $(a).attr('href')?.trim() || '';
        if (href) {
          links.push(href);
          console.log(`Link: ${href}`);
        }
      });

      // Classify links
      links.forEach(href => {
        const hostname = new URL(href).hostname.toLowerCase();

        // Zip / file download
        if (zipDomains.some(d => hostname.includes(d))) {
          if (!zips.includes(href)) zips.push(href);
          return;
        }

        // Video / stream
        if (videoDomains.some(d => hostname.includes(d))) {
          if (!videos.includes(href)) videos.push(href);
          return;
        }
      });

      // ── Images: only inside .cPost_contentWrap ───────────────────────────
      $wrap.find('img').each((_, img) => {
        let src = $(img).attr('src') || $(img).attr('data-src') || '';
        if (!src.startsWith('http')) return;

        const ext = src.toLowerCase().split('?')[0].slice(-5);
        if (imageExtensions.some(e => ext.endsWith(e))) {
          // Prefer parent <a> href if it looks like full-res image
          const $parentA = $(img).closest('a[href]');
          if ($parentA.length) {
            const parentHref = $parentA.attr('href')?.trim() || '';
            const parentExt = parentHref.toLowerCase().split('?')[0].slice(-5);
            if (imageExtensions.some(e => parentExt.endsWith(e))) {
              src = parentHref;
            }
          }

          if (!images.includes(src)) {
            images.push(src);
            console.log(`Captured image: ${src} (class: ${$(img).attr('class') || 'none'})`);
          }
        }
      });
    });

    // ── Final response ───────────────────────────────────────────────────────
    return res.status(200).json({
      title,
      videos,
      images,
      zips
    });
  } catch (error: any) {
    console.error(`Scrape failed for ${req.body.url}:`, error.message);
    return res.status(500).json({
      error: 'Failed to scrape page',
      details: error.message || 'Internal error'
    });
  }
});

// Health check
app.get('/health', (req, res) => {
  res.status(200).json({ status: 'ok', uptime: process.uptime() });
});

app.listen(PORT, () => {
  console.log(`Server running on http://localhost:${PORT}`);
  console.log(`POST /api/scrape → { "url": "https://..." }`);
});