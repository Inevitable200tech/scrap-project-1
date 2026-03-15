import { URL } from 'url';

export const filterVideoUrls = (videos: any[], originalUrl: string) => {
  // 1. Extract the main domain from the original URL (e.g., mmsvibe.com)
  const urlObj = new URL(originalUrl);
  const mainDomain = urlObj.hostname.split('.').slice(-2).join('.'); 

  const filtered = videos.filter(v => {
    const vUrl = v.url.toLowerCase();
    
    // --- BLACKLISTS ---
    const isOriginal = v.url === originalUrl;
    const isBlob = vUrl.startsWith('blob:');
    const isAdDomain = /afcdn\.net|adsystem|clck\.ru|doubleclick|popads|exoclick/i.test(vUrl);
    const isTracker = /yandex|mc\.ru|analytics|pixel|google|facebook|amazon|\.ts($|\?)/i.test(vUrl);
    
    // --- WHITELIST LOGIC ---
    const isLibraryAd = vUrl.includes('/library/') || vUrl.includes('/ads/');

    return !isOriginal && !isBlob && !isAdDomain && !isTracker && !isLibraryAd;
  });

  // 2. De-duplicate by URL
  const uniqueMap = new Map();
  filtered.forEach(v => uniqueMap.set(v.url, v));
  let uniqueVideos = Array.from(uniqueMap.values());

  // 3. Smart HLS Filtering (Master vs Index)
  const hasMaster = uniqueVideos.some(v => v.url.includes('master.m3u8'));
  if (hasMaster) {
    uniqueVideos = uniqueVideos.filter(v => 
      v.url.includes('master.m3u8') || !v.url.includes('.m3u8')
    );
  }

  // 4. Final fallback
  if (uniqueVideos.length === 0 && videos.length > 0) {
      const emergencyBackup = videos.find(v => !/google|analytics|yandex/i.test(v.url));
      return emergencyBackup ? [emergencyBackup] : [];
  }

  return uniqueVideos;
};