// storage.service.ts - MAXIMUM SPEED MODE
// Speed over everything. No quality concerns.
import { S3Client, DeleteObjectCommand, ListMultipartUploadsCommand, AbortMultipartUploadCommand } from "@aws-sdk/client-s3";
import { Upload } from "@aws-sdk/lib-storage";
import { MongoClient, Db } from "mongodb";
import crypto from "crypto";
import { spawn, ChildProcess, execSync } from "child_process";
import { R2_CONFIG, MONGO_URI, MONGO_DB } from "./config.js";

export const s3Client = new S3Client({
  region: "auto",
  endpoint: R2_CONFIG.endpoint,
  credentials: R2_CONFIG.credentials,
  maxAttempts: 5,
  requestHandler: { requestTimeout: 300000 },
});

let db: Db;
const mongoClient = new MongoClient(MONGO_URI, {
  maxPoolSize: 10,
  minPoolSize: 2,
  maxIdleTimeMS: 60000,
});

// ── Speed-First Encoding Profiles ──────────────────────────────────────────

export type SpeedProfile = 'instant' | 'ultra-speed' | 'extreme-speed';

interface EncodingConfig {
  mode: 'copy' | 'encode';      // copy = no encoding (instant), encode = fast encoding
  resolution?: string;
  fps?: number;
  crf?: number;
  bitrate?: string;
  format: 'mp4' | 'webm';         // webm is faster than mp4
}

const SPEED_PROFILES: Record<SpeedProfile, EncodingConfig> = {
  // MODE 1: INSTANT - No encoding at all (10-20x faster)
  // Detects if video is already H.264 MP4 and copies it directly
  'instant': {
    mode: 'copy',
    format: 'mp4',
    // No encoding = instant output
  },

  // MODE 2: ULTRA-SPEED - Extreme quality loss for 5-10x speed
  // 144p (phone screen size), 10fps, terrible quality but fast
  'ultra-speed': {
    mode: 'encode',
    format: 'webm',              // WebM VP8 is faster than H.264
    resolution: 'scale=-2:144',  // Extreme compression: 144p (smallest usable)
    fps: 8,                       // 8fps (slide show)
    bitrate: '150k',             // Extremely low bitrate
    crf: 45,                      // Worst quality (0-51 scale)
  },

  // MODE 3: EXTREME-SPEED - Absolute minimum
  // 96p (tiny), 6fps, unplayable quality but instant
  'extreme-speed': {
    mode: 'encode',
    format: 'webm',
    resolution: 'scale=-2:96',   // 96p (thumbnail size)
    fps: 6,                       // 6fps (barely animated)
    bitrate: '100k',             // Minimum bitrate
    crf: 51,                      // Absolute worst quality
  },
};

const SPEED_PROFILE: SpeedProfile = 
  (process.env.SPEED_PROFILE as SpeedProfile) || 'instant';

console.log(`[SPEED] Profile: ${SPEED_PROFILE}`);

export async function getDb(): Promise<Db> {
  if (!db) {
    await mongoClient.connect();
    db = mongoClient.db(MONGO_DB);
    await Promise.all([
      db.collection('jobs').createIndex({ createdAt: 1 }, { expireAfterSeconds: 7200 }),
      db.collection('jobs').createIndex({ status: 1 }),
      db.collection('videos').createIndex({ hash: 1 }, { unique: true }),
    ]);
    console.log(`[STORAGE] Speed mode: ${SPEED_PROFILE}`);
  }
  return db;
}

export type JobStatus = 'pending' | 'scraping' | 'storing' | 'done' | 'failed';
export type JobFailureReason = 'dead_video' | 'no_video_found' | 'ffmpeg_failed' | 'expired_url' | 'scrape_error' | 'upload_failed' | 'unknown';

export interface Job {
  jobId: string;
  url: string;
  title?: string;
  status: JobStatus;
  failureReason?: JobFailureReason;
  createdAt: Date;
  updatedAt: Date;
  progress?: {
    stage: 'downloading' | 'encoding' | 'uploading' | 'complete';
    percent: number;
    bytesProcessed?: number;
  };
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

export async function recoverInterruptedJobs(): Promise<Job[]> {
  const db = await getDb();
  const docs = await db.collection('jobs')
    .find({ status: { $in: ['pending', 'scraping', 'storing'] } })
    .sort({ createdAt: 1 })
    .toArray();
  return docs.map(({ _id, ...job }) => job as Job);
}

export async function resetJobToPending(jobId: string): Promise<void> {
  const db = await getDb();
  await db.collection('jobs').updateOne(
    { jobId },
    { $set: { status: 'pending', updatedAt: new Date() }, $unset: { error: '', failureReason: '' } }
  );
}

export interface ProcessingResult {
  success: boolean;
  hash?: string;
  r2Key?: string;
  error?: string;
  isDuplicate?: boolean;
  bytesProcessed?: number;
  encodingMode?: string;
}

async function deleteFromR2(key: string, retries = 2): Promise<void> {
  let lastError;
  for (let i = 0; i < retries; i++) {
    try {
      await s3Client.send(new DeleteObjectCommand({ Bucket: R2_CONFIG.bucket, Key: key }));
      return;
    } catch (err: any) {
      lastError = err;
      if (i < retries - 1) await new Promise(r => setTimeout(r, 100 * Math.pow(2, i)));
    }
  }
}

function getUrlSecondsRemaining(videoUrl: string): number {
  try {
    const u = new URL(videoUrl);
    const exp = u.searchParams.get('expires') || u.searchParams.get('exp') || u.searchParams.get('e');
    if (!exp) return -1;
    return Math.max(0, parseInt(exp) - Math.floor(Date.now() / 1000));
  } catch {
    return -1;
  }
}

const ORIGIN_HEADERS_CACHE: Record<string, any> = {};

function getSiteOrigin(videoUrl: string): { referer: string; origin: string } {
  if (videoUrl in ORIGIN_HEADERS_CACHE) return ORIGIN_HEADERS_CACHE[videoUrl];
  let result;
  if (/vidsonic/i.test(videoUrl)) result = { referer: 'https://vidsonic.net/', origin: 'https://vidsonic.net' };
  else if (/vidara/i.test(videoUrl)) result = { referer: 'https://vidara.so/', origin: 'https://vidara.so' };
  else if (/vidnest/i.test(videoUrl)) result = { referer: 'https://vidnest.to/', origin: 'https://vidnest.to' };
  else if (/streamtape/i.test(videoUrl)) result = { referer: 'https://streamtape.com/', origin: 'https://streamtape.com' };
  else if (/boodstream/i.test(videoUrl)) result = { referer: 'https://share.boodstream.cc/', origin: 'https://share.boodstream.cc' };
  else {
    try {
      const u = new URL(videoUrl);
      result = { referer: u.origin + '/', origin: u.origin };
    } catch {
      result = { referer: '', origin: '' };
    }
  }
  ORIGIN_HEADERS_CACHE[videoUrl] = result;
  return result;
}

function buildHeaders(referer: string, origin: string): string {
  return [
    'User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
    'Accept: */*',
    'Accept-Language: en-US,en;q=0.9',
    ...(referer ? [`Referer: ${referer}`] : []),
    ...(origin ? [`Origin: ${origin}`] : []),
  ].join('\r\n') + '\r\n';
}

// Detect if video is already H.264 MP4 (can use copy mode)
function detectVideoCodec(videoUrl: string): Promise<string | null> {
  return new Promise((resolve) => {
    try {
      const timeout = setTimeout(() => resolve(null), 5000);
      const proc = spawn('ffprobe', [
        '-v', 'error',
        '-select_streams', 'v:0',
        '-show_entries', 'stream=codec_name',
        '-of', 'default=noprint_wrappers=1:nokey=1',
        videoUrl,
      ]);

      let output = '';
      proc.stdout.on('data', (data) => { output += data.toString(); });
      proc.on('close', () => {
        clearTimeout(timeout);
        const codec = output.trim();
        resolve(codec === 'h264' ? 'h264' : null);
      });
    } catch {
      resolve(null);
    }
  });
}

function buildFFmpegArgs(headers: string, videoUrl: string, isM3U8: boolean, profile: SpeedProfile): string[] {
  const config = SPEED_PROFILES[profile];

  // INSTANT MODE: Direct copy (no encoding)
  if (config.mode === 'copy') {
    return [
      '-headers', headers,
      '-tls_verify', '0',
      '-i', videoUrl,
      '-c:v', 'copy',           // Copy video stream (NO ENCODING)
      '-c:a', 'copy',           // Copy audio stream (NO ENCODING)
      '-movflags', 'frag_keyframe+empty_moov+default_base_moof',
      '-f', 'mp4',
      'pipe:1',
    ];
  }

  // ULTRA/EXTREME SPEED: WebM VP8 (fastest encoding codec)
  if (config.format === 'webm') {
    return [
      '-headers', headers,
      '-tls_verify', '0',
      '-i', videoUrl,
      '-vf', config.resolution!,
      '-r', String(config.fps!),
      '-c:v', 'libvpx',         // VP8 is faster than H.264
      '-cpu-used', '5',         // Maximum speed (0-5, 5 is fastest)
      '-b:v', config.bitrate!,
      '-quality', 'realtime',   // Realtime encoding (fast)
      '-c:a', 'libopus',
      '-b:a', '32k',            // Ultra low audio bitrate
      '-ac', '1',               // Mono (faster)
      '-progress', 'pipe:2',
      '-f', 'webm',
      'pipe:1',
    ];
  }

  // Fallback to ultra-speed if unknown
  return buildFFmpegArgs(headers, videoUrl, isM3U8, 'ultra-speed');
}

function spawnFFmpeg(args: string[]): { process: ChildProcess; promise: Promise<void> } {
  const process = spawn('ffmpeg', args);
  let resolved = false;

  const promise = new Promise<void>((resolve, reject) => {
    process.stderr.on('data', (data: any) => {
      const msg = data.toString();
      if (msg.includes('Error')) console.error(`[FFMPEG]: ${msg.trim()}`);
    });
    process.on('error', (err: any) => {
      if (!resolved) { resolved = true; reject(new Error(`FFmpeg spawn: ${err.message}`)); }
    });
    process.on('exit', (code: number | null) => {
      if (!resolved) { resolved = true; if (code !== 0) reject(new Error(`FFmpeg code ${code}`)); else resolve(); }
    });
    process.on('close', (code: number | null) => {
      if (!resolved) { resolved = true; if (code !== 0) reject(new Error(`FFmpeg code ${code}`)); else resolve(); }
    });
  });

  return { process, promise };
}

export async function cleanupIncompleteMultipartUploads(olderThanMs = 60 * 60 * 1000): Promise<{ aborted: number; errors: number }> {
  let aborted = 0;
  let errors = 0;
  let isTruncated = true;
  let keyMarker: string | undefined;
  let uploadIdMarker: string | undefined;

  while (isTruncated) {
    let response;
    try {
      response = await s3Client.send(new ListMultipartUploadsCommand({
        Bucket: R2_CONFIG.bucket,
        KeyMarker: keyMarker,
        UploadIdMarker: uploadIdMarker,
      }));
    } catch (err: any) {
      break;
    }

    const cutoff = Date.now() - olderThanMs;
    const abortPromises = [];

    for (const { Key, UploadId, Initiated } of response.Uploads ?? []) {
      if (Initiated && Initiated.getTime() > cutoff) continue;
      abortPromises.push(
        s3Client.send(new AbortMultipartUploadCommand({
          Bucket: R2_CONFIG.bucket, Key: Key!, UploadId: UploadId!,
        }))
          .then(() => { aborted++; })
          .catch(() => { errors++; })
      );
    }

    if (abortPromises.length > 0) await Promise.all(abortPromises);
    isTruncated = response.IsTruncated ?? false;
    keyMarker = response.NextKeyMarker;
    uploadIdMarker = response.NextUploadIdMarker;
  }

  return { aborted, errors };
}

export async function processAndStoreVideo(
  videoUrl: string,
  jobId: string,
  title: string,
  refreshUrl?: () => Promise<string>,
  onProgress?: (progress: Job['progress']) => Promise<void>
): Promise<ProcessingResult> {
  let ffmpegProc: ChildProcess | null = null;

  try {
    const secondsLeft = getUrlSecondsRemaining(videoUrl);
    if (secondsLeft !== -1 && secondsLeft < 120) {
      if (!refreshUrl) {
        return { success: false, error: `URL expires in ${secondsLeft}s` };
      }
      try {
        videoUrl = await Promise.race([
          refreshUrl(),
          new Promise<string>((_, reject) => setTimeout(() => reject(new Error('Timeout')), 15000))
        ]);
      } catch (e: any) {
        return { success: false, error: `Refresh failed: ${e.message}` };
      }
    }

    const isM3U8 = videoUrl.includes('.m3u8');
    const hash = crypto.createHash('sha256');
    const profile = SPEED_PROFILE;
    const config = SPEED_PROFILES[profile];

    // Try to detect if already H.264 MP4 (for instant mode)
    let usingCopyMode = false;
    if (profile === 'instant' && !isM3U8) {
      const codec = await detectVideoCodec(videoUrl);
      if (codec === 'h264') {
        usingCopyMode = true;
        console.log(`[STORAGE:${jobId}] ✓ H.264 detected — using COPY mode (INSTANT)`);
      }
    }

    const r2Key = `videos/${Date.now()}-${crypto.randomBytes(4).toString('hex')}.${config.format}`;
    const { referer, origin } = getSiteOrigin(videoUrl);
    const headers = buildHeaders(referer, origin);
    const ffmpegArgs = buildFFmpegArgs(headers, videoUrl, isM3U8, profile);

    const startTime = Date.now();
    console.log(`[STORAGE:${jobId}] START | Profile: ${profile} | Mode: ${usingCopyMode ? 'COPY' : 'ENCODE'} | Format: ${config.format}`);

    let bytesReceived = 0;
    let ffmpegError: Error | null = null;

    const { process: ffmpeg, promise: ffmpegPromise } = spawnFFmpeg(ffmpegArgs);
    ffmpegProc = ffmpeg;

    const ffmpegTimeoutHandle = setTimeout(() => {
      if (ffmpegProc && !ffmpegProc.killed) {
        ffmpegProc.kill('SIGKILL');
      }
    }, 600000);

    ffmpeg.stderr!.on('data', (data: any) => {
      const text = data.toString();
      if (text.includes('Error')) console.error(`[FFMPEG:${jobId}]: ${text.trim()}`);
    });

    ffmpeg.stdout!.on('data', (chunk: Buffer) => {
      hash.update(chunk);
      bytesReceived += chunk.length;
    });

    ffmpeg.stdout!.on('error', (err: any) => {
      ffmpegError = new Error(`Stream error: ${err.message}`);
    });

    // Simple upload progress
    const upload = new Upload({
      client: s3Client,
      params: {
        Bucket: R2_CONFIG.bucket,
        Key: r2Key,
        Body: ffmpeg.stdout!,
        ContentType: config.format === 'webm' ? 'video/webm' : 'video/mp4',
        Metadata: { 'original-title': title.substring(0, 100) },
      },
      queueSize: 16,
      partSize: 52428800,
      leavePartsOnError: false,
    });

    const [uploadResult] = await Promise.allSettled([
      upload.done(),
      ffmpegPromise,
    ]);

    clearTimeout(ffmpegTimeoutHandle);

    if (ffmpegError) throw ffmpegError;
    if (uploadResult.status === 'rejected') throw uploadResult.reason;
    if (bytesReceived === 0) {
      await deleteFromR2(r2Key);
      return { success: false, error: 'No video data' };
    }

    const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
    const sizeMB = (bytesReceived / 1024 / 1024).toFixed(2);

    if (onProgress) {
      onProgress({ stage: 'complete', percent: 100, bytesProcessed: bytesReceived }).catch(() => {});
    }

    const finalHash = hash.digest('hex');
    const database = await getDb();

    const existing = await database.collection('videos').findOne({ hash: finalHash });
    if (existing) {
      await deleteFromR2(r2Key);
      console.log(`[STORAGE:${jobId}] ✓ DUPLICATE | ${elapsed}s | ${sizeMB}MB`);
      return { success: true, hash: finalHash, r2Key: existing.r2Key, isDuplicate: true, bytesProcessed: bytesReceived, encodingMode: profile };
    }

    await database.collection('videos').insertOne({
      title,
      hash: finalHash,
      r2Key,
      originalUrl: videoUrl,
      type: `speed_${profile}`,
      format: config.format,
      sizeBytes: bytesReceived,
      processedAt: new Date(),
    });

    console.log(`[STORAGE:${jobId}] ✓ DONE | ${elapsed}s | ${sizeMB}MB | ${profile} | ${finalHash.substring(0, 8)}...`);
    return { success: true, hash: finalHash, r2Key, bytesProcessed: bytesReceived, encodingMode: profile };

  } catch (error: any) {
    if (ffmpegProc && !ffmpegProc.killed) {
      ffmpegProc.kill('SIGKILL');
    }
    console.error(`[STORAGE:${jobId}] ERROR: ${error.message}`);
    return { success: false, error: error.message };
  }
}

export async function closeConnections(): Promise<void> {
  try {
    await mongoClient.close();
  } catch (err: any) {
    console.error(`Shutdown error: ${err.message}`);
  }
}