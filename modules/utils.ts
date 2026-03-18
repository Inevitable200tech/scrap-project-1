export const filterVideoUrls = (videos: any[], originalUrl: string) => {
  const urlObj = new URL(originalUrl);
  const mainDomain = urlObj.hostname.split('.').slice(-2).join('.');
  const isStreamtape = /streamtape\./i.test(originalUrl);

  // ── Step 1: Filter out junk ───────────────────────────────────────────────
  const filtered = videos.filter(v => {
    const vUrl = v.url.toLowerCase();
    const isOriginal = v.url === originalUrl;
    const isBlob = vUrl.startsWith('blob:');
    const isAdDomain = /afcdn\.net|adsystem|clck\.ru|doubleclick|popads|exoclick|dtscout|dtscdn/i.test(vUrl);
    const isTracker = /yandex|mc\.ru|analytics|pixel|google|facebook|amazon|\.ts($|\?)/i.test(vUrl);
    const isLibraryAd = vUrl.includes('/library/') || vUrl.includes('/ads/');

    return !isOriginal && !isBlob && !isAdDomain && !isTracker && !isLibraryAd;
  });

  // ── Step 2: Deduplicate by URL ────────────────────────────────────────────
  const uniqueMap = new Map();
  filtered.forEach(v => uniqueMap.set(v.url, v));
  let uniqueVideos = Array.from(uniqueMap.values());

  // ── Step 3: Streamtape — return only the best single URL ──────────────────
  // Network sniffer captures both the get_video redirect AND the tapecontent.net
  // CDN stream. We only need one: prefer the direct CDN URL, fall back to redirect.
  if (isStreamtape) {
    const cdn      = uniqueVideos.find(v => /tapecontent\.net/i.test(v.url));
    const redirect = uniqueVideos.find(v => v.url.includes('get_video'));
    if (cdn)      return [cdn];
    if (redirect) return [redirect];
    // Nothing matched the known patterns — return whatever we have (1 item max)
    return uniqueVideos.slice(0, 1);
  }

  // ── Step 4: HLS deduplication — prefer master playlist ───────────────────
  const hasMaster = uniqueVideos.some(v => v.url.includes('master.m3u8'));
  if (hasMaster) {
    uniqueVideos = uniqueVideos.filter(v =>
      v.url.includes('master.m3u8') || !v.url.includes('.m3u8')
    );
  }

  // ── Step 5: Emergency fallback ────────────────────────────────────────────
  if (uniqueVideos.length === 0 && videos.length > 0) {
    const emergencyBackup = videos.find(v => !/google|analytics|yandex/i.test(v.url));
    return emergencyBackup ? [emergencyBackup] : [];
  }

  return uniqueVideos;
};