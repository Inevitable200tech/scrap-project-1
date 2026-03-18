// storage.service.ts with progress tracking
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
    requestTimeout: 300000,
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
    await Promise.all([
      db.collection('jobs').createIndex({ createdAt: 1 }, { expireAfterSeconds: 7200 }),
      db.collection('jobs').createIndex({ status: 1 }),
      db.collection('videos').createIndex({ hash: 1 }, { unique: true }),
    ]);
  }
  return db;
}

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
    {
      $set:   { status: 'pending', updatedAt: new Date() },
      $unset: { error: '', failureReason: '' },
    }
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

const FFMPEG_ARGS_TEMPLATE = (headers: string, videoUrl: string, isM3U8: boolean): string[] => [
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
  '-progress', 'pipe:2',  // NEW: Progress reporting
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
          .then(() => { aborted++; })
          .catch((err: any) => { errors++; })
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

function buildHeaders(referer: string, origin: string): string {
  return [
    'User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
    'Accept: */*',
    'Accept-Language: en-US,en;q=0.9',
    ...(referer ? [`Referer: ${referer}`] : []),
    ...(origin ? [`Origin: ${origin}`] : []),
  ].join('\r\n') + '\r\n';
}

interface FFmpegProgress {
  frame?: number;
  fps?: number;
  stream_0_0_q?: number;
  bitrate?: number;
  total_size?: number;
  out_time_us?: number;
  out_time?: string;
  dup_frames?: number;
  drop_frames?: number;
  speed?: number;
  progress?: string;
}

function parseFFmpegProgress(line: string): Partial<FFmpegProgress> {
  const result: Partial<FFmpegProgress> = {};
  const parts = line.split('=');
  if (parts.length === 2) {
    const key = parts[0].trim();
    const value = parts[1].trim();
    if (key === 'frame') result.frame = parseInt(value);
    else if (key === 'fps') result.fps = parseFloat(value);
    else if (key === 'bitrate') result.bitrate = parseFloat(value);
    else if (key === 'total_size') result.total_size = parseInt(value);
    else if (key === 'out_time_us') result.out_time_us = parseInt(value);
    else if (key === 'out_time') result.out_time = value;
    else if (key === 'progress') result.progress = value;
  }
  return result;
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
      if (!resolved) {
        resolved = true;
        reject(new Error(`FFmpeg spawn error: ${err.message}`));
      }
    });

    process.on('exit', (code: number | null) => {
      if (!resolved) {
        resolved = true;
        if (code !== 0) reject(new Error(`FFmpeg exited with code ${code}`));
        else resolve();
      }
    });

    process.on('close', (code: number | null) => {
      if (!resolved) {
        resolved = true;
        if (code !== 0) reject(new Error(`FFmpeg closed with code ${code}`));
        else resolve();
      }
    });
  });

  return { process, promise };
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
        return { success: false, error: `Video URL expires in ${secondsLeft}s` };
      }
      try {
        console.log(`[STORAGE:${jobId}] URL expiring (${secondsLeft}s) — refreshing...`);
        videoUrl = await Promise.race([
          refreshUrl(),
          new Promise<string>((_, reject) =>
            setTimeout(() => reject(new Error('Refresh timeout')), 15000)
          ),
        ]);
        console.log(`[STORAGE:${jobId}] Refreshed URL: ${videoUrl.substring(0, 80)}...`);
      } catch (e: any) {
        return { success: false, error: `URL refresh failed: ${e.message}` };
      }
    }

    const isM3U8 = videoUrl.includes('.m3u8');
    const hash = crypto.createHash('sha256');
    const r2Key = `videos/${Date.now()}-${crypto.randomBytes(4).toString('hex')}.mp4`;

    const { referer, origin } = getSiteOrigin(videoUrl);
    const headers = buildHeaders(referer, origin);
    const ffmpegArgs = FFMPEG_ARGS_TEMPLATE(headers, videoUrl, isM3U8);

    console.log(`[STORAGE:${jobId}] Starting ${isM3U8 ? 'HLS' : 'MP4'} processing...`);

    const ffmpegTimeout = 600000;
    let bytesReceived = 0;
    let ffmpegError: Error | null = null;
    let videoDuration = 0;
    let currentFrame = 0;

    const { process: ffmpeg, promise: ffmpegPromise } = spawnFFmpeg(ffmpegArgs);
    ffmpegProc = ffmpeg;

    const ffmpegTimeoutHandle = setTimeout(() => {
      if (ffmpegProc && !ffmpegProc.killed) {
        console.error(`[STORAGE:${jobId}] FFmpeg timeout — killing`);
        ffmpegProc.kill('SIGKILL');
      }
    }, ffmpegTimeout);

    // ── Parse FFmpeg progress from stderr ──────────────────────────────────
    const progressBuffer: string[] = [];
    let lastProgressLog = Date.now();

    ffmpeg.stderr!.on('data', (data: any) => {
      const text = data.toString();
      
      // Parse duration once
      if (!videoDuration && text.includes('Duration:')) {
        const durationMatch = text.match(/Duration: (\d+):(\d+):(\d+\.\d+)/);
        if (durationMatch) {
          const [, hours, minutes, seconds] = durationMatch;
          videoDuration = parseInt(hours) * 3600 + parseInt(minutes) * 60 + parseFloat(seconds);
          console.log(`[STORAGE:${jobId}] Video duration: ${videoDuration.toFixed(1)}s`);
        }
      }

      // Accumulate progress lines
      progressBuffer.push(text);

      // Log progress periodically (every 2s)
      const now = Date.now();
      if (now - lastProgressLog > 2000) {
        const fullText = progressBuffer.join('');
        progressBuffer.length = 0;

        const lines = fullText.split('\n');
        for (const line of lines) {
          if (line.includes('frame=')) {
            const progress = parseFFmpegProgress(line);
            if (progress.frame) currentFrame = progress.frame;

            const fps = progress.fps || 24;
            const timeUs = progress.out_time_us || 0;
            const timeSeconds = timeUs / 1_000_000;

            let encodePercent = 0;
            if (videoDuration > 0) {
              encodePercent = Math.min(100, (timeSeconds / videoDuration) * 100);
            }

            const downloadPercent = 50; // Assume download is ~50% when encoding starts
            const uploadPercent = 0; // Will be updated later
            const overallPercent = Math.round(
              downloadPercent * 0.3 + encodePercent * 0.5 + uploadPercent * 0.2
            );

            if (onProgress) {
              onProgress({
                stage: 'encoding',
                percent: overallPercent,
                bytesProcessed: bytesReceived,
              }).catch(() => {});
            }

            console.log(
              `[STORAGE:${jobId}] ⏳ ENCODING: ${encodePercent.toFixed(1)}% | Frame ${currentFrame} @ ${fps.toFixed(1)}fps | Overall: ${overallPercent}%`
            );
          }
        }
        lastProgressLog = now;
      }

      if (text.includes('Error')) console.error(`[FFMPEG:${jobId}]: ${text.trim()}`);
    });

    // ── FFmpeg stdout: capture video data ───────────────────────────────────
    ffmpeg.stdout!.on('data', (chunk: Buffer) => {
      hash.update(chunk);
      bytesReceived += chunk.length;

      if (onProgress) {
        onProgress({
          stage: 'uploading',
          percent: 50,  // Will be refined below
          bytesProcessed: bytesReceived,
        }).catch(() => {});
      }
    });

    ffmpeg.stdout!.on('error', (err: any) => {
      ffmpegError = new Error(`FFmpeg stream error: ${err.message}`);
    });

    // ── Upload to R2 with progress tracking ─────────────────────────────────
    const upload = new Upload({
      client: s3Client,
      params: {
        Bucket: R2_CONFIG.bucket,
        Key: r2Key,
        Body: ffmpeg.stdout!,
        ContentType: 'video/mp4',
        Metadata: { 'original-title': title.substring(0, 100) },
      },
      queueSize: 16,
      partSize: 52428800,
      leavePartsOnError: false,
    });

    // Track upload progress
    let totalBytes = 0;
    let uploadedBytes = 0;

    // Monkey-patch the upload to track progress
    const origDone = upload.done.bind(upload);
    upload.done = async function() {
      // This is hacky but works - monitor the actual upload
      const uploadStart = Date.now();
      const progressInterval = setInterval(() => {
        if (bytesReceived > 0) {
          // Estimate upload progress (will be 0-100% as data flows)
          const uploadPercent = Math.min(100, (uploadedBytes / bytesReceived) * 100);
          const overall = Math.round(
            50 * 0.3 + 75 * 0.5 + uploadPercent * 0.2  // 50% download, 75% encode, X% upload
          );

          if (onProgress && uploadPercent > 0) {
            onProgress({
              stage: 'uploading',
              percent: overall,
              bytesProcessed: bytesReceived,
            }).catch(() => {});

            console.log(
              `[STORAGE:${jobId}] 📤 UPLOADING: ${uploadPercent.toFixed(1)}% | ${(uploadedBytes / 1024 / 1024).toFixed(1)}MB / ${(bytesReceived / 1024 / 1024).toFixed(1)}MB | Overall: ${overall}%`
            );
          }
        }
      }, 2000);

      try {
        const result = await origDone.call(this);
        clearInterval(progressInterval);
        uploadedBytes = bytesReceived;
        return result;
      } catch (err) {
        clearInterval(progressInterval);
        throw err;
      }
    };

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

    if (onProgress) {
      onProgress({
        stage: 'complete',
        percent: 100,
        bytesProcessed: bytesReceived,
      }).catch(() => {});
    }

    const finalHash = hash.digest('hex');
    const database = await getDb();

    const existing = await database.collection('videos').findOne({ hash: finalHash });
    if (existing) {
      console.log(`[STORAGE:${jobId}] ✓ Duplicate (${finalHash}) — removed copy`);
      await deleteFromR2(r2Key);
      return { success: true, hash: finalHash, r2Key: existing.r2Key, isDuplicate: true, bytesProcessed: bytesReceived };
    }

    await database.collection('videos').insertOne({
      title,
      hash: finalHash,
      r2Key,
      originalUrl: videoUrl,
      type: isM3U8 ? 'hls_converted' : 'direct_mp4_optimized',
      sizeBytes: bytesReceived,
      processedAt: new Date(),
    });

    console.log(
      `[STORAGE:${jobId}] ✓ COMPLETE | ${r2Key} | ${(bytesReceived / 1024 / 1024).toFixed(2)}MB | Hash: ${finalHash.substring(0, 8)}...`
    );

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
    console.log('[STORAGE] MongoDB connections closed');
  } catch (err: any) {
    console.error(`[STORAGE] Shutdown error: ${err.message}`);
  }
}