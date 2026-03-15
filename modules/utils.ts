import { exec } from 'child_process';
import { promisify } from 'util';
const execPromise = promisify(exec);

export async function getMediaInfo(url: string) {
  try {
    const { stdout } = await execPromise(`yt-dlp -j --no-warnings --flat-playlist "${url}"`);
    return JSON.parse(stdout);
  } catch { return null; }
}

export const filterVideoUrls = (videos: any[], originalUrl: string) => {
  const filtered = videos.filter(v => {
    const vUrl = v.url.toLowerCase();
    const isOriginal = v.url === originalUrl;
    const isBlob = vUrl.startsWith('blob:');
    const isAdDomain = /afcdn\.net|adsystem|clck\.ru|doubleclick|popads|exoclick/i.test(vUrl);
    const isTracker = /yandex|mc\.ru|analytics|pixel|google|facebook|amazon|\.ts($|\?)/i.test(vUrl);
    
    return !isOriginal && !isBlob && !isAdDomain && !isTracker;
  });

  const uniqueMap = new Map();
  filtered.forEach(v => uniqueMap.set(v.url, v));
  let uniqueVideos = Array.from(uniqueMap.values());

  const hasMaster = uniqueVideos.some(v => v.url.includes('master.m3u8'));
  if (hasMaster) {
    uniqueVideos = uniqueVideos.filter(v => v.url.includes('master.m3u8') || !v.url.includes('.m3u8'));
  }

  return uniqueVideos;
};