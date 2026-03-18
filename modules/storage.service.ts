// storage.service.ts
import { S3Client, DeleteObjectCommand } from "@aws-sdk/client-s3";
import { Upload } from "@aws-sdk/lib-storage";
import { MongoClient, Db, ObjectId } from "mongodb";
import crypto from "crypto";
import { PassThrough, Readable } from "stream";
import { spawn } from "child_process";
import { R2_CONFIG, MONGO_URI, MONGO_DB } from "./config.js";

export const s3Client = new S3Client({
  region: "auto",
  endpoint: R2_CONFIG.endpoint,
  credentials: R2_CONFIG.credentials,
});

let db: Db;
const mongoClient = new MongoClient(MONGO_URI);

export async function getDb() {
  if (!db) {
    await mongoClient.connect();
    db = mongoClient.db(MONGO_DB);

    // ── Indexes ──────────────────────────────────────────────────────────
    // TTL index: auto-delete jobs older than 2 hours
    await db.collection('jobs').createIndex(
      { createdAt: 1 },
      { expireAfterSeconds: 7200 }
    );
    await db.collection('jobs').createIndex({ status: 1 });
    await db.collection('videos').createIndex({ hash: 1 }, { unique: true });
  }
  return db;
}

// ── Job types ──────────────────────────────────────────────────────────────
export type JobStatus = 'pending' | 'scraping' | 'storing' | 'done' | 'failed';

export interface Job {
  jobId: string;
  url: string;
  title?: string;
  status: JobStatus;
  createdAt: Date;
  updatedAt: Date;
  result?: {
    title: string;
    r2Key?: string;
    hash?: string;
    isDuplicate?: boolean;
    playUrl?: string;
  };
  error?: string;
}

export async function createJob(url: string, title?: string): Promise<Job> {
  const db = await getDb();
  const job: Job = {
    jobId: crypto.randomBytes(8).toString('hex'),
    url,
    title,
    status: 'pending',
    createdAt: new Date(),
    updatedAt: new Date(),
  };
  await db.collection('jobs').insertOne({ ...job });
  return job;
}

export async function getJob(jobId: string): Promise<Job | null> {
  const db = await getDb();
  const doc = await db.collection('jobs').findOne({ jobId });
  if (!doc) return null;
  const { _id, ...job } = doc;
  return job as Job;
}

export async function updateJob(jobId: string, patch: Partial<Job>): Promise<void> {
  const db = await getDb();
  await db.collection('jobs').updateOne(
    { jobId },
    { $set: { ...patch, updatedAt: new Date() } }
  );
}

export async function listJobs(): Promise<Job[]> {
  const db = await getDb();
  const docs = await db.collection('jobs')
    .find()
    .sort({ createdAt: -1 })
    .limit(100)
    .toArray();
  return docs.map(({ _id, ...job }) => job as Job);
}

// ── Storage helpers ────────────────────────────────────────────────────────
export interface ProcessingResult {
  success: boolean;
  hash?: string;
  r2Key?: string;
  error?: string;
  isDuplicate?: boolean;
}

async function deleteFromR2(key: string): Promise<void> {
  try {
    await s3Client.send(new DeleteObjectCommand({
      Bucket: R2_CONFIG.bucket,
      Key: key,
    }));
    console.log(`[STORAGE] Deleted from R2: ${key}`);
  } catch (err: any) {
    console.error(`[STORAGE] Failed to delete from R2 (${key}): ${err.message}`);
  }
}

function getUrlSecondsRemaining(videoUrl: string): number {
  try {
    const urlObj = new URL(videoUrl);
    const expires =
      urlObj.searchParams.get('expires') ||
      urlObj.searchParams.get('exp') ||
      urlObj.searchParams.get('e');
    if (!expires) return -1;
    const secondsLeft = parseInt(expires) - Math.floor(Date.now() / 1000);
    return Math.max(0, secondsLeft);
  } catch {
    return -1;
  }
}

/**
 * Returns the Referer and Origin headers for a given video CDN URL.
 * FFmpeg needs these to pass CDN access checks.
 */
function getSiteOrigin(videoUrl: string): { referer: string; origin: string } {
  if (/vidsonic/i.test(videoUrl))   return { referer: 'https://vidsonic.net/',        origin: 'https://vidsonic.net' };
  if (/vidara/i.test(videoUrl))     return { referer: 'https://vidara.so/',            origin: 'https://vidara.so' };
  if (/vidnest/i.test(videoUrl))    return { referer: 'https://vidnest.to/',           origin: 'https://vidnest.to' };
  if (/streamtape/i.test(videoUrl)) return { referer: 'https://streamtape.com/',       origin: 'https://streamtape.com' };
  if (/boodstream/i.test(videoUrl)) return { referer: 'https://share.boodstream.cc/', origin: 'https://share.boodstream.cc' };
  try {
    const u = new URL(videoUrl);
    return { referer: u.origin + '/', origin: u.origin };
  } catch {
    return { referer: '', origin: '' };
  }
}

export async function processAndStoreVideo(
  videoUrl: string,
  title: string,
  refreshUrl?: () => Promise<string>
): Promise<ProcessingResult> {
  let ffmpegProcess: any = null;

  try {
    // ── Expiry check ───────────────────────────────────────────────────────
    const secondsLeft = getUrlSecondsRemaining(videoUrl);
    if (secondsLeft !== -1) {
      console.log(`[STORAGE] URL token expires in ${secondsLeft}s`);
      if (secondsLeft < 60) {
        if (refreshUrl) {
          console.log(`[STORAGE] URL expiring soon — re-scraping...`);
          try {
            videoUrl = await refreshUrl();
            console.log(`[STORAGE] Fresh URL: ${videoUrl.substring(0, 80)}...`);
          } catch (e: any) {
            return { success: false, error: `URL expired and re-scrape failed: ${e.message}` };
          }
        } else {
          return { success: false, error: `Video URL expired (${secondsLeft}s remaining)` };
        }
      }
    }

    const isM3U8 = videoUrl.includes('.m3u8');
    const uploadStream = new PassThrough();
    const hash = crypto.createHash('sha256');
    const r2Key = `videos/${Date.now()}-${crypto.randomBytes(4).toString('hex')}.mp4`;

    // ── Build browser-like headers for FFmpeg ──────────────────────────────
    // Passed via -headers for both HLS and direct MP4.
    // This bypasses TLS fingerprint checks on CDNs like boodstream and vidsonic
    // that reject non-browser http clients (including Node's built-in http).
    const { referer, origin } = getSiteOrigin(videoUrl);
    const headerString = [
      'User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
      'Accept: */*',
      'Accept-Language: en-US,en;q=0.9',
      ...(referer ? [`Referer: ${referer}`] : []),
      ...(origin  ? [`Origin: ${origin}`]   : []),
    ].join('\r\n') + '\r\n';

    console.log(`[STORAGE] ${isM3U8 ? 'HLS' : 'Direct MP4'} — FFmpeg fetching with browser headers: ${videoUrl.substring(0, 80)}...`);

    // ── FFmpeg — unified approach for HLS and direct MP4 ──────────────────
    // Both pass the URL directly to FFmpeg with -headers + -tls_verify 0.
    // The old pipe:0 approach is removed — it broke for any CDN doing TLS
    // fingerprinting (boodstream, vidsonic) because Node's http client
    // doesn't look like a browser at the TLS layer.
    const ffmpegArgs = [
      '-headers', headerString,
      '-tls_verify', '0',
      '-i', videoUrl,
      '-vf', 'scale=-2:480',
      '-r', '20',
      '-c:v', 'libx264',
      '-crf', '32',
      '-preset', 'ultrafast',
      '-b:v', '500k',
      // HLS-only bitrate caps
      ...(isM3U8 ? ['-maxrate', '600k', '-bufsize', '1000k'] : []),
      '-c:a', 'aac',
      '-b:a', '64k',
      '-ac', '1',
      '-movflags', 'frag_keyframe+empty_moov+default_base_moof',
      '-f', 'mp4',
      'pipe:1',
    ];

    ffmpegProcess = spawn('ffmpeg', ffmpegArgs);

    const videoSource: Readable = ffmpegProcess.stdout;

    ffmpegProcess.on('error', (err: any) => console.error('[FFMPEG SPAWN ERROR]:', err));
    ffmpegProcess.stderr.on('data', (data: any) => {
      const msg = data.toString();
      if (msg.includes('Error')) console.error(`[FFMPEG]: ${msg}`);
    });

    // Track bytes received to detect empty output before wasting R2 storage
    let bytesReceived = 0;
    videoSource.on('data', (chunk: Buffer) => {
      hash.update(chunk);
      bytesReceived += chunk.length;
    });

    const upload = new Upload({
      client: s3Client,
      params: {
        Bucket: R2_CONFIG.bucket,
        Key: r2Key,
        Body: videoSource.pipe(uploadStream),
        ContentType: 'video/mp4',
      },
      queueSize: 4,
      partSize: 1024 * 1024 * 5,
      leavePartsOnError: false,
    });

    await upload.done();

    // ── Empty output guard ─────────────────────────────────────────────────
    // If FFmpeg produced 0 bytes the CDN rejected the request (TLS or auth).
    // Delete the empty R2 object and return a failure so the job can retry.
    if (bytesReceived === 0) {
      console.error(`[STORAGE] FFmpeg produced 0 bytes — CDN likely rejected the request`);
      await deleteFromR2(r2Key);
      return { success: false, error: 'FFmpeg produced empty output — CDN rejected request' };
    }

    const finalHash = hash.digest('hex');

    const database = await getDb();
    const existing = await database.collection('videos').findOne({ hash: finalHash });

    if (existing) {
      console.log(`[STORAGE] Duplicate detected (hash: ${finalHash}). Removing freshly uploaded copy...`);
      await deleteFromR2(r2Key);
      return { success: true, hash: finalHash, r2Key: existing.r2Key, isDuplicate: true };
    }

    await database.collection('videos').insertOne({
      title,
      hash: finalHash,
      r2Key,
      originalUrl: videoUrl,
      type: isM3U8 ? 'hls_converted' : 'direct_mp4_optimized',
      processedAt: new Date(),
    });

    console.log(`[STORAGE] Stored successfully → R2: ${r2Key} | Hash: ${finalHash}`);
    return { success: true, hash: finalHash, r2Key };

  } catch (error: any) {
    if (ffmpegProcess) ffmpegProcess.kill('SIGKILL');
    console.error(`[STORAGE ERROR]: ${error.message}`);
    return { success: false, error: error.message };
  }
}