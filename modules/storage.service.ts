// storage.service.ts
import { S3Client, DeleteObjectCommand, ListMultipartUploadsCommand, AbortMultipartUploadCommand } from "@aws-sdk/client-s3";
import { Upload } from "@aws-sdk/lib-storage";
import { MongoClient, Db } from "mongodb";
import crypto from "crypto";
import { Readable } from "stream";
import { spawn, ChildProcess } from "child_process";
import { R2_CONFIG, MONGO_URI, MONGO_DB } from "./config.js";

export const s3Client = new S3Client({
  region: "auto",
  endpoint: R2_CONFIG.endpoint,
  credentials: R2_CONFIG.credentials,
  maxAttempts: 5,
  requestHandler: {
    requestTimeout: 300000, // 5min timeout for large uploads
  },
});

let db: Db;
const mongoClient = new MongoClient(MONGO_URI, {
  maxPoolSize: 10,
  minPoolSize: 2,
  maxIdleTimeMS: 60000,
});

export async function getDb(): Promise<Db> {
  if (!db) {
    await mongoClient.connect();
    db = mongoClient.db(MONGO_DB);
    // Indexes — idempotent, safe to run on every startup
    await Promise.all([
      db.collection('jobs').createIndex({ createdAt: 1 }, { expireAfterSeconds: 7200 }),
      db.collection('jobs').createIndex({ status: 1 }),
      db.collection('videos').createIndex({ hash: 1 }, { unique: true }),
    ]);
  }
  return db;
}

// ── Job types ──────────────────────────────────────────────────────────────
export type JobStatus = 'pending' | 'scraping' | 'storing' | 'done' | 'failed';

export type JobFailureReason =
  | 'dead_video'
  | 'no_video_found'
  | 'ffmpeg_failed'
  | 'expired_url'
  | 'scrape_error'
  | 'upload_failed'
  | 'unknown';

export interface Job {
  jobId: string;
  url: string;
  title?: string;
  status: JobStatus;
  failureReason?: JobFailureReason;
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
    {
      $set:   { status: 'pending', updatedAt: new Date() },
      $unset: { error: '', failureReason: '' },
    }
  );
}

// ── Storage helpers ────────────────────────────────────────────────────────
export interface ProcessingResult {
  success: boolean;
  hash?: string;
  r2Key?: string;
  error?: string;
  isDuplicate?: boolean;
  bytesProcessed?: number;
}

async function deleteFromR2(key: string, retries = 2): Promise<void> {
  let lastError;
  for (let i = 0; i < retries; i++) {
    try {
      await s3Client.send(new DeleteObjectCommand({ Bucket: R2_CONFIG.bucket, Key: key }));
      console.log(`[STORAGE] Deleted: ${key}`);
      return;
    } catch (err: any) {
      lastError = err;
      if (i < retries - 1) await new Promise(r => setTimeout(r, 500 * Math.pow(2, i)));
    }
  }
  console.error(`[STORAGE] Failed to delete ${key}: ${lastError?.message}`);
}

function getUrlSecondsRemaining(videoUrl: string): number {
  try {
    const u = new URL(videoUrl);
    const exp = u.searchParams.get('expires') || u.searchParams.get('exp') || u.searchParams.get('e');
    if (!exp) return -1;
    const remaining = Math.max(0, parseInt(exp) - Math.floor(Date.now() / 1000));
    return remaining;
  } catch {
    return -1;
  }
}

// Cached headers for common origins
const ORIGIN_HEADERS_CACHE: Record<string, { referer: string; origin: string }> = {};

function getSiteOrigin(videoUrl: string): { referer: string; origin: string } {
  if (videoUrl in ORIGIN_HEADERS_CACHE) return ORIGIN_HEADERS_CACHE[videoUrl];

  let result;
  if (/vidsonic/i.test(videoUrl))   result = { referer: 'https://vidsonic.net/', origin: 'https://vidsonic.net' };
  else if (/vidara/i.test(videoUrl))     result = { referer: 'https://vidara.so/', origin: 'https://vidara.so' };
  else if (/vidnest/i.test(videoUrl))    result = { referer: 'https://vidnest.to/', origin: 'https://vidnest.to' };
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

// Template for FFmpeg args — reused to avoid allocation overhead
const FFMPEG_ARGS_TEMPLATE = (
  headers: string,
  videoUrl: string,
  isM3U8: boolean
): string[] => [
  '-headers', headers,
  '-tls_verify', '0',
  '-i', videoUrl,
  '-threads', '0',
  '-vf', 'scale=-2:360',
  '-r', '24',
  '-c:v', 'libx264',
  '-crf', '28',
  '-preset', 'ultrafast',
  '-tune', 'zerolatency',
  '-b:v', '600k',
  ...(isM3U8 ? ['-maxrate', '700k', '-bufsize', '1200k'] : []),
  '-c:a', 'aac',
  '-b:a', '64k',
  '-ac', '1',
  '-movflags', 'frag_keyframe+empty_moov+default_base_moof',
  '-f', 'mp4',
  'pipe:1',
];

export async function cleanupIncompleteMultipartUploads(
  olderThanMs = 60 * 60 * 1000
): Promise<{ aborted: number; errors: number }> {
  console.log('[R2 CLEANUP] Scanning incomplete multipart uploads...');

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
      console.error(`[R2 CLEANUP] List failed: ${err.message}`);
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
          .then(() => { aborted++; console.log(`[R2 CLEANUP] Aborted: ${Key}`); })
          .catch((err: any) => { errors++; console.error(`[R2 CLEANUP] Abort failed: ${err.message}`); })
      );
    }

    if (abortPromises.length > 0) await Promise.all(abortPromises);

    isTruncated = response.IsTruncated ?? false;
    keyMarker = response.NextKeyMarker;
    uploadIdMarker = response.NextUploadIdMarker;
  }

  console.log(`[R2 CLEANUP] Done — Aborted: ${aborted} | Errors: ${errors}`);
  return { aborted, errors };
}

// ── Video processing ───────────────────────────────────────────────────────

function buildHeaders(referer: string, origin: string): string {
  return [
    'User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
    'Accept: */*',
    'Accept-Language: en-US,en;q=0.9',
    ...(referer ? [`Referer: ${referer}`] : []),
    ...(origin ? [`Origin: ${origin}`] : []),
  ].join('\r\n') + '\r\n';
}

function spawnFFmpeg(args: string[]): { process: ChildProcess; promise: Promise<void> } {
  const process = spawn('ffmpeg', args);
  let resolved = false;

  const promise = new Promise<void>((resolve, reject) => {
    let stderr = '';
    process.stderr.on('data', (data: any) => {
      stderr += data.toString();
      const msg = data.toString();
      if (msg.includes('Error')) console.error(`[FFMPEG]: ${msg.trim()}`);
    });

    process.on('error', (err: any) => {
      if (!resolved) {
        resolved = true;
        reject(new Error(`FFmpeg spawn error: ${err.message}`));
      }
    });

    process.on('exit', (code: number | null) => {
      if (!resolved) {
        resolved = true;
        if (code !== 0) {
          reject(new Error(`FFmpeg exited with code ${code}`));
        } else {
          resolve();
        }
      }
    });

    process.on('close', (code: number | null) => {
      if (!resolved) {
        resolved = true;
        if (code !== 0) {
          reject(new Error(`FFmpeg closed with code ${code}`));
        } else {
          resolve();
        }
      }
    });
  });

  return { process, promise };
}

export async function processAndStoreVideo(
  videoUrl: string,
  title: string,
  refreshUrl?: () => Promise<string>
): Promise<ProcessingResult> {
  let ffmpegProc: ChildProcess | null = null;

  try {
    // ── Expiry check with early abort ──────────────────────────────────────
    const secondsLeft = getUrlSecondsRemaining(videoUrl);
    if (secondsLeft !== -1 && secondsLeft < 120) { // Abort if <2min remaining
      if (!refreshUrl) {
        return { success: false, error: `Video URL expires in ${secondsLeft}s` };
      }
      try {
        console.log(`[STORAGE] URL expiring soon (${secondsLeft}s) — refreshing...`);
        videoUrl = await Promise.race([
          refreshUrl(),
          new Promise<string>((_, reject) =>
            setTimeout(() => reject(new Error('Refresh timeout')), 15000)
          ),
        ]);
        console.log(`[STORAGE] Refreshed URL: ${videoUrl.substring(0, 80)}...`);
      } catch (e: any) {
        return { success: false, error: `URL refresh failed: ${e.message}` };
      }
    }

    const isM3U8 = videoUrl.includes('.m3u8');
    const uploadStream = new Readable({ read() {} }); // dummy stream for types
    const hash = crypto.createHash('sha256');
    const r2Key = `videos/${Date.now()}-${crypto.randomBytes(4).toString('hex')}.mp4`;

    const { referer, origin } = getSiteOrigin(videoUrl);
    const headers = buildHeaders(referer, origin);
    const ffmpegArgs = FFMPEG_ARGS_TEMPLATE(headers, videoUrl, isM3U8);

    console.log(`[STORAGE] ${isM3U8 ? 'HLS' : 'MP4'} processing: ${videoUrl.substring(0, 80)}...`);

    // ── Spawn FFmpeg with timeout ──────────────────────────────────────────
    const ffmpegTimeout = 600000; // 10min max
    let bytesReceived = 0;
    let ffmpegError: Error | null = null;

    const { process: ffmpeg, promise: ffmpegPromise } = spawnFFmpeg(ffmpegArgs);
    ffmpegProc = ffmpeg;

    const ffmpegTimeoutHandle = setTimeout(() => {
      if (ffmpegProc && !ffmpegProc.killed) {
        console.error('[FFMPEG] Timeout — killing process');
        ffmpegProc.kill('SIGKILL');
      }
    }, ffmpegTimeout);

    ffmpeg.stdout!.on('data', (chunk: Buffer) => {
      hash.update(chunk);
      bytesReceived += chunk.length;
    });

    ffmpeg.stdout!.on('error', (err: any) => {
      ffmpegError = new Error(`FFmpeg stream error: ${err.message}`);
    });

    // ── Upload to R2 directly from FFmpeg ──────────────────────────────────
    // partSize 50MB (5x larger) + queueSize 16 (2x more concurrent) = ~10x faster
    const upload = new Upload({
      client: s3Client,
      params: {
        Bucket: R2_CONFIG.bucket,
        Key: r2Key,
        Body: ffmpeg.stdout!,
        ContentType: 'video/mp4',
        Metadata: { 'original-title': title.substring(0, 100) },
      },
      queueSize: 16,        // More parallel part uploads
      partSize: 52428800,   // 50MB parts (vs 10MB default)
      leavePartsOnError: false,
    });

    // Race: upload vs FFmpeg completion
    const [uploadResult] = await Promise.allSettled([
      upload.done(),
      ffmpegPromise,
    ]);

    clearTimeout(ffmpegTimeoutHandle);

    if (ffmpegError) throw ffmpegError;
    if (uploadResult.status === 'rejected') throw uploadResult.reason;
    if (bytesReceived === 0) {
      await deleteFromR2(r2Key);
      return { success: false, error: 'No video data — CDN rejected' };
    }

    const finalHash = hash.digest('hex');
    const database = await getDb();

    // ── Deduplication check ────────────────────────────────────────────────
    const existing = await database.collection('videos').findOne({ hash: finalHash });
    if (existing) {
      console.log(`[STORAGE] Duplicate (${finalHash}) — removed fresh copy`);
      await deleteFromR2(r2Key);
      return { success: true, hash: finalHash, r2Key: existing.r2Key, isDuplicate: true, bytesProcessed: bytesReceived };
    }

    // ── Store metadata ─────────────────────────────────────────────────────
    await database.collection('videos').insertOne({
      title,
      hash: finalHash,
      r2Key,
      originalUrl: videoUrl,
      type: isM3U8 ? 'hls_converted' : 'direct_mp4_optimized',
      sizeBytes: bytesReceived,
      processedAt: new Date(),
    });

    console.log(`[STORAGE] Done — R2: ${r2Key} | Hash: ${finalHash} | Size: ${(bytesReceived / 1024 / 1024).toFixed(2)}MB`);
    return { success: true, hash: finalHash, r2Key, bytesProcessed: bytesReceived };

  } catch (error: any) {
    if (ffmpegProc && !ffmpegProc.killed) {
      ffmpegProc.kill('SIGKILL');
    }
    console.error(`[STORAGE ERROR]: ${error.message}`);
    return { success: false, error: error.message };
  }
}

// ── Graceful shutdown ──────────────────────────────────────────────────────
export async function closeConnections(): Promise<void> {
  try {
    await mongoClient.close();
    console.log('[STORAGE] MongoDB connections closed');
  } catch (err: any) {
    console.error(`[STORAGE] Shutdown error: ${err.message}`);
  }
}