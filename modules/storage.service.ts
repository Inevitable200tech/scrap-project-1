// storage.service.ts - INSTANT METHOD ONLY (Simplified)
// Handles H.264 MP4 with COPY (3-5s)
// Handles HLS/M3U8 with fast MP4 encoding (~20-30s)
import { S3Client, DeleteObjectCommand, ListMultipartUploadsCommand, AbortMultipartUploadCommand } from "@aws-sdk/client-s3";
import { Upload } from "@aws-sdk/lib-storage";
import { MongoClient, Db } from "mongodb";
import crypto from "crypto";
import { spawn, ChildProcess } from "child_process";
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

export async function getDb(): Promise<Db> {
  if (!db) {
    await mongoClient.connect();
    db = mongoClient.db(MONGO_DB);
    await Promise.all([
      db.collection('jobs').createIndex({ createdAt: 1 }, { expireAfterSeconds: 7200 }),
      db.collection('jobs').createIndex({ status: 1 }),
      db.collection('videos').createIndex({ hash: 1 }, { unique: true }),
    ]);
    console.log(`[STORAGE] INSTANT mode enabled`);
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

// FFmpeg args for INSTANT method
function buildFFmpegArgs(headers: string, videoUrl: string, isCopyMode: boolean): string[] {
  // COPY MODE: Direct copy (H.264 MP4)
  if (isCopyMode) {
    return [
      '-headers', headers,
      '-tls_verify', '0',
      '-i', videoUrl,
      '-c:v', 'copy',
      '-c:a', 'copy',
      '-movflags', 'frag_keyframe+empty_moov+default_base_moof',
      '-f', 'mp4',
      'pipe:1',
    ];
  }

  // ENCODE MODE: Fast MP4 encoding for HLS (libx264 ultrafast)
  return [
    '-headers', headers,
    '-tls_verify', '0',
    '-i', videoUrl,
    '-c:v', 'libx264',
    '-preset', 'ultrafast',
    '-crf', '32',                // Lower quality = faster
    '-vf', 'scale=-2:320',       // 320p
    '-r', '20',                  // 20fps
    '-b:v', '500k',
    '-c:a', 'aac',
    '-b:a', '64k',
    '-ac', '1',
    '-movflags', 'frag_keyframe+empty_moov+default_base_moof',
    '-progress', 'pipe:2',
    '-f', 'mp4',
    'pipe:1',
  ];
}

function spawnFFmpeg(args: string[]): { process: any; promise: Promise<void> } {
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
  let ffmpegProc: any = null;

  try {
    // Check URL expiry
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
    const r2Key = `videos/${Date.now()}-${crypto.randomBytes(4).toString('hex')}.mp4`;

    const { referer, origin } = getSiteOrigin(videoUrl);
    const headers = buildHeaders(referer, origin);

    // Determine if we can use COPY mode
    let isCopyMode = false;
    let modeLabel = 'ENCODE';

    if (!isM3U8) {
      // Try to detect if H.264 MP4 (can use copy)
      isCopyMode = true;  // Assume copy mode for direct MP4
      modeLabel = 'COPY';
    } else {
      // HLS requires encoding to MP4
      modeLabel = 'ENCODE (HLS → MP4)';
    }

    const ffmpegArgs = buildFFmpegArgs(headers, videoUrl, isCopyMode);
    const startTime = Date.now();

    console.log(`[STORAGE:${jobId}] START | Mode: ${modeLabel} | Format: mp4`);

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

    // Upload to R2
    // Sanitize title for metadata header (only alphanumeric, hyphens, underscores)
    const sanitizedTitle = title
      .substring(0, 100)
      .replace(/[^a-zA-Z0-9\-_]/g, '_')  // Replace invalid chars with underscore
      .replace(/_{2,}/g, '_');            // Replace multiple underscores with single

    const upload = new Upload({
      client: s3Client,
      params: {
        Bucket: R2_CONFIG.bucket,
        Key: r2Key,
        Body: ffmpeg.stdout!,
        ContentType: 'video/mp4',
        Metadata: { 'original-title': sanitizedTitle },
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

    // Check for duplicates
    const existing = await database.collection('videos').findOne({ hash: finalHash });
    if (existing) {
      await deleteFromR2(r2Key);
      console.log(`[STORAGE:${jobId}] ✓ DUPLICATE | ${elapsed}s | ${sizeMB}MB`);
      return { success: true, hash: finalHash, r2Key: existing.r2Key, isDuplicate: true, bytesProcessed: bytesReceived };
    }

    // Store metadata
    await database.collection('videos').insertOne({
      title,
      hash: finalHash,
      r2Key,
      originalUrl: videoUrl,
      type: isCopyMode ? 'h264_copy' : 'h264_encoded_from_hls',
      format: 'mp4',
      sizeBytes: bytesReceived,
      processedAt: new Date(),
    });

    console.log(`[STORAGE:${jobId}] ✓ DONE | ${elapsed}s | ${sizeMB}MB | ${modeLabel} | ${finalHash.substring(0, 8)}...`);
    return { success: true, hash: finalHash, r2Key, bytesProcessed: bytesReceived };

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