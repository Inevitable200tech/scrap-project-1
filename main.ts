// server.ts

import express, { Request, Response } from 'express';
import playwrightExtra from 'playwright-extra';
import StealthPlugin from 'puppeteer-extra-plugin-stealth';
import * as cheerio from 'cheerio';
import rateLimit from 'express-rate-limit';
import * as path from 'path';
import * as os from 'os';

playwrightExtra.chromium.use(StealthPlugin());

// ── Persistent Playwright context (reused across requests) ──────────────────
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

// ── Rate limiting: 10 requests per minute per IP ─────────────────────────────
const limiter = rateLimit({
  windowMs: 60 * 1000, // 1 minute
  max: 10,             // limit each IP to 10 requests per windowMs
  message: {
    error: 'Too many requests from this IP. Please try again in 60 seconds.',
  },
  standardHeaders: true,
  legacyHeaders: false,
});

// ── Express app ──────────────────────────────────────────────────────────────
const app = express();
const PORT = process.env.PORT || 3000;

app.use(express.json());
app.use(limiter);

// Only allow dropmms.co URLs
const ALLOWED_PREFIXES = ['http://dropmms.co', 'https://dropmms.co'];

app.post('/api/scrape', async (req: Request, res: Response) => {
  const { url } = req.body;

  if (!url || typeof url !== 'string') {
    return res.status(400).json({ error: 'Missing or invalid "url" parameter' });
  }

  // Validate URL prefix
  if (!ALLOWED_PREFIXES.some(prefix => url.startsWith(prefix))) {
    return res.status(403).json({ error: 'Only dropmms.co URLs are allowed' });
  }

  try {
    const context = await getPersistentContext();
    const page = await context.newPage();

    await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 60000 });

    // Cloudflare wait (if present)
    try {
      const title = await page.title();
      if (title.includes('Just a moment') || title.includes('Attention Required')) {
        await page.waitForTimeout(15000 + Math.random() * 10000);
      }
    } catch {}

    // Wait for main content
    await page.waitForSelector('.ipsType_pageTitle, [data-role="commentContent"]', { timeout: 30000 });

    const html = await page.content();

    await page.close();

    const $ = cheerio.load(html);

    // ── Extract title ────────────────────────────────────────────────────────
    const title = $('h1.ipsType_pageTitle span.ipsContained span').first().text().trim() ||
                  $('h1.ipsType_pageTitle').first().text().trim() ||
                  'Untitled Thread';

    // ── Extract videos (▶️ prefixed links) ───────────────────────────────────
    const videos: string[] = [];
    const streamRegex = /▶️\s*(https?:\/\/[^\s<]+)/gi;
    const fullText = $('[data-role="commentContent"]').text().trim();
    let match;
    while ((match = streamRegex.exec(fullText)) !== null) {
      videos.push(match[1].trim());
    }

    // ── Extract images ───────────────────────────────────────────────────────
    const images: string[] = [];
    $('img.ipsImage').each((_, img) => {
      let src = $(img).attr('src') || $(img).attr('data-src') || '';
      if (src && (src.includes('imagetwist.com') || src.includes('img202.'))) {
        images.push(src);
      }
    });

    // ── Response ─────────────────────────────────────────────────────────────
    return res.json({
      title,
      videos,
      images,
    });
  } catch (error: any) {
    console.error('Scrape error:', error);
    return res.status(500).json({
      error: 'Failed to scrape the page',
      details: error.message || 'Unknown error',
    });
  }
});

// Health check endpoint
app.get('/health', (req, res) => {
  res.json({ status: 'ok', uptime: process.uptime() });
});

app.listen(PORT, () => {
  console.log(`Server running on http://localhost:${PORT}`);
  console.log(`POST to /api/scrape with JSON body: { "url": "https://dropmms.co/topic/..." }`);
});