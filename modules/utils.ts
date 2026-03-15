export const filterVideoUrls = (videos: any[], originalUrl: string) => {
  const urlObj = new URL(originalUrl);
  const mainDomain = urlObj.hostname.split('.').slice(-2).join('.');

  const filtered = videos.filter(v => {
    const vUrl = v.url.toLowerCase();
    const isOriginal = v.url === originalUrl;
    const isBlob = vUrl.startsWith('blob:');
    const isAdDomain = /afcdn\.net|adsystem|clck\.ru|doubleclick|popads|exoclick/i.test(vUrl);
    const isTracker = /yandex|mc\.ru|analytics|pixel|google|facebook|amazon|\.ts($|\?)/i.test(vUrl);
    const isLibraryAd = vUrl.includes('/library/') || vUrl.includes('/ads/');

    return !isOriginal && !isBlob && !isAdDomain && !isTracker && !isLibraryAd;
  });

  const uniqueMap = new Map();
  filtered.forEach(v => uniqueMap.set(v.url, v));
  let uniqueVideos = Array.from(uniqueMap.values());

  const hasMaster = uniqueVideos.some(v => v.url.includes('master.m3u8'));
  if (hasMaster) {
    uniqueVideos = uniqueVideos.filter(v =>
      v.url.includes('master.m3u8') || !v.url.includes('.m3u8')
    );
  }

  if (uniqueVideos.length === 0 && videos.length > 0) {
    const emergencyBackup = videos.find(v => !/google|analytics|yandex/i.test(v.url));
    return emergencyBackup ? [emergencyBackup] : [];
  }

  return uniqueVideos;
};