// storage.service.ts - Upload to Main Instance
// Handles H.264 MP4 with COPY (3-5s)
// Handles HLS/M3U8 with fast MP4 encoding (~20-30s)
// Uploads to main instance via /api/upload instead of direct R2
import { MongoClient, Db } from "mongodb";
import crypto from "crypto";
import { spawn } from "child_process";
import fs from "fs";
import { MAIN_INSTANCE, MONGO_URI, MONGO_DB } from "./config.js";

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
    console.log(`[STORAGE] INSTANT mode enabled (uploading to main instance)`);
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
  error?: string;
  isDuplicate?: boolean;
  bytesProcessed?: number;
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
  // Base optimizations to skip the 10-second probing delay and handle flaky servers
  const fastProbeArgs = [
    '-analyzeduration', '5000000',
    '-probesize', '5000000',
    '-reconnect', '1',
    '-reconnect_streamed', '1',
    '-reconnect_delay_max', '5'
  ];

  if (isCopyMode) {
    // Direct MP4 COPY MODE
    return [
      '-headers', headers,
      '-tls_verify', '0',
      ...fastProbeArgs,
      '-i', videoUrl,
      '-c:v', 'copy',
      '-c:a', 'copy',
      '-ignore_unknown',
      '-movflags', 'frag_keyframe+empty_moov+default_base_moof',
      '-f', 'mp4',
      'pipe:1',
    ];
  }

  // HLS/M3U8 COPY MODE (Lightning fast compared to the old libx264 re-encoding)
  return [
    '-headers', headers,
    '-tls_verify', '0',
    ...fastProbeArgs,
    '-i', videoUrl,
    '-c:v', 'copy',
    '-c:a', 'copy',
    '-bsf:a', 'aac_adtstoasc', // Fixes audio mapping when extracting AAC from MPEG-TS chunks
    '-copyts',                // Preserve original timestamps (better for stream stability)
    '-ignore_unknown',        // Ignore unknown streams instead of failing
    '-movflags', 'frag_keyframe+empty_moov+default_base_moof',
    '-f', 'mp4',
    'pipe:1',
  ];
}

function spawnFFmpeg(args: string[]): { process: any; promise: Promise<void> } {
  const process = spawn('ffmpeg', args);
  let resolved = false;
  const stderrLines: string[] = [];

  const promise = new Promise<void>((resolve, reject) => {
    process.stderr.on('data', (data: any) => {
      const msg = data.toString();
      if (msg.includes('Error')) console.error(`[FFMPEG]: ${msg.trim()}`);
      
      // Store last 10 lines of stderr for better error reporting
      stderrLines.push(msg.trim());
      if (stderrLines.length > 10) stderrLines.shift();
    });
    process.on('error', (err: any) => {
      if (!resolved) { 
        resolved = true; 
        reject(new Error(`FFmpeg spawn: ${err.message}`)); 
      }
    });
    process.on('exit', (code: number | null) => {
      if (!resolved) { 
        resolved = true; 
        if (code !== 0) {
          const lastError = stderrLines.join('\n');
          reject(new Error(`FFmpeg code ${code}${lastError ? ': ' + lastError : ''}`)); 
        } else {
          resolve(); 
        }
      }
    });
    process.on('close', (code: number | null) => {
      if (!resolved) { 
        resolved = true; 
        if (code !== 0) {
          const lastError = stderrLines.join('\n');
          reject(new Error(`FFmpeg code ${code}${lastError ? ': ' + lastError : ''}`)); 
        } else {
          resolve(); 
        }
      }
    });
  });

  return { process, promise };
}

// ============ UPLOAD TO MAIN INSTANCE ============
async function uploadToMainInstance(
  filePath: string,
  fileName: string,
  title: string,
  hash: string,
  fileSizeBytes: number
): Promise<{ success: boolean; hash?: string; isDuplicate?: boolean; error?: string }> {
  try {
    const formData = new FormData();

    // Append file as Blob from disk (avoids OOM crash)
    const blob = await fs.openAsBlob(filePath);

    // Append metadata
    formData.append('hash', hash);
    formData.append('title', title || fileName);
    formData.append('file', blob, fileName)
    console.log(`[MAIN-UPLOAD] 📤 Uploading to main instance: ${MAIN_INSTANCE.url}/api/upload`);
    console.log(`[MAIN-UPLOAD]    File: ${fileName}`);
    console.log(`[MAIN-UPLOAD]    Title: ${title}`);
    console.log(`[MAIN-UPLOAD]    Size: ${(fileSizeBytes / 1024 / 1024).toFixed(2)} MB`);

    const uploadUrl = `${MAIN_INSTANCE.url}/api/upload`;
    const response = await fetch(uploadUrl, {
      method: 'POST',
      body: formData,
      // Note: FormData automatically sets multipart/form-data header
    });

    if (!response.ok) {
      const error = await response.text();
      throw new Error(`HTTP ${response.status}: ${error}`);
    }

    const result = await response.json();

    if (response.status === 200) {
      // 200 = duplicate file
      console.log(`[MAIN-UPLOAD] ✓ DUPLICATE | Hash: ${result.hash}`);
      return {
        success: true,
        hash: result.hash,
        isDuplicate: true,
      };
    } else if (response.status === 202) {
      // 202 = async processing started
      console.log(`[MAIN-UPLOAD] ✓ QUEUED | Hash: ${result.hash} | Poll: ${result.pollUrl}`);
      return {
        success: true,
        hash: result.hash,
        isDuplicate: false,
      };
    } else {
      throw new Error(`Unexpected status ${response.status}`);
    }
  } catch (error: any) {
    console.error(`[MAIN-UPLOAD] ❌ Error: ${error.message}`);
    return {
      success: false,
      error: error.message,
    };
  }
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

    const { referer, origin } = getSiteOrigin(videoUrl);
    const headers = buildHeaders(referer, origin);

    // Determine if we can use COPY mode
    let isCopyMode = false;
    let modeLabel = 'ENCODE';

    if (!isM3U8) {
      // Try to detect if H.264 MP4 (can use copy)
      isCopyMode = true;  // Assume copy mode for direct MP4
      modeLabel = 'COPY (Direct)';
    } else {
      // HLS now uses COPY mode too (lightning fast)
      modeLabel = 'COPY (HLS → MP4)';
    }

    const ffmpegArgs = buildFFmpegArgs(headers, videoUrl, isCopyMode);
    const startTime = Date.now();

    console.log(`[STORAGE:${jobId}] START | Mode: ${modeLabel} | Format: mp4`);

    let bytesReceived = 0;
    let ffmpegError: Error | null = null;
    
    // Write directly to a temporary file on disk instead of memory array
    const tempFilePath = `/tmp/${jobId}_${Date.now()}.mp4`;
    const fileStream = fs.createWriteStream(tempFilePath);

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

    let lastLogTime = Date.now();
    let lastLogBytes = 0;

    ffmpeg.stdout!.on('data', (chunk: Buffer) => {
      if (bytesReceived === 0) {
        console.log(`[STORAGE:${jobId}] FFmpeg started receiving data...`);
      }
      hash.update(chunk);
      fileStream.write(chunk);
      bytesReceived += chunk.length;

      const now = Date.now();
      if (now - lastLogTime > 5000) {
        const mb = (bytesReceived / 1024 / 1024).toFixed(2);
        const speed = ((bytesReceived - lastLogBytes) / 1024 / 1024 / ((now - lastLogTime) / 1000)).toFixed(2);
        console.log(`[STORAGE:${jobId}] Downloading... ${mb} MB (${speed} MB/s)`);
        lastLogTime = now;
        lastLogBytes = bytesReceived;
      }
    });

    ffmpeg.stdout!.on('error', (err: any) => {
      ffmpegError = new Error(`Stream error: ${err.message}`);
    });

    // Wait for FFmpeg to complete
    const [ffmpegResult] = await Promise.allSettled([ffmpegPromise]);
    console.log(`[STORAGE:${jobId}] FFmpeg promise settled: ${ffmpegResult.status}`);

    clearTimeout(ffmpegTimeoutHandle);
    
    // Close the file stream properly and wait for finish
    console.log(`[STORAGE:${jobId}] Ending file stream...`);
    fileStream.end();
    await new Promise<void>((resolve, reject) => {
      fileStream.on('finish', () => {
        console.log(`[STORAGE:${jobId}] File stream finished.`);
        resolve();
      });
      fileStream.on('error', (err) => {
        console.error(`[STORAGE:${jobId}] File stream error: ${err.message}`);
        reject(err);
      });
    });

    console.log(`[STORAGE:${jobId}] Checking for errors...`);
    if (ffmpegResult.status === 'rejected') {
      ffmpegError = new Error(`FFmpeg exited with error: ${ffmpegResult.reason}`);
    }

    if (ffmpegError) {
      if (fs.existsSync(tempFilePath)) fs.unlinkSync(tempFilePath);
      throw ffmpegError;
    }
    
    if (bytesReceived === 0) {
      if (fs.existsSync(tempFilePath)) fs.unlinkSync(tempFilePath);
      return { success: false, error: 'No video data' };
    }

    const finalHash = hash.digest('hex');

    if (onProgress) {
      onProgress({ stage: 'uploading', percent: 75, bytesProcessed: bytesReceived }).catch(() => { });
    }

    // Upload to main instance via disk stream Blob
    let uploadResult;
    try {
      uploadResult = await uploadToMainInstance(
        tempFilePath,
        `${finalHash}.mp4`,
        title,
        finalHash,
        bytesReceived
      );
    } finally {
      // Always cleanup the temp file!
      if (fs.existsSync(tempFilePath)) fs.unlinkSync(tempFilePath);
    }

    if (!uploadResult.success) {
      return {
        success: false,
        error: uploadResult.error || 'Upload failed',
      };
    }

    const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
    const sizeMB = (bytesReceived / 1024 / 1024).toFixed(2);

    if (onProgress) {
      onProgress({ stage: 'complete', percent: 100, bytesProcessed: bytesReceived }).catch(() => { });
    }

    // Store metadata locally (reference to main instance)
    const database = await getDb();
    const existing = await database.collection('videos').findOne({ hash: finalHash });

    if (existing) {
      console.log(`[STORAGE:${jobId}] ✓ DUPLICATE | ${elapsed}s | ${sizeMB}MB`);
      return {
        success: true,
        hash: finalHash,
        isDuplicate: true,
        bytesProcessed: bytesReceived,
      };
    }

    // Store metadata locally
    await database.collection('videos').insertOne({
      title,
      hash: finalHash,
      originalUrl: videoUrl,
      type: isCopyMode ? 'h264_copy' : 'h264_encoded_from_hls',
      format: 'mp4',
      sizeBytes: bytesReceived,
      processedAt: new Date(),
      uploadedToMain: true,
      mainInstanceUrl: MAIN_INSTANCE.url,
    });

    console.log(`[STORAGE:${jobId}] ✓ DONE | ${elapsed}s | ${sizeMB}MB | ${modeLabel} | ${finalHash.substring(0, 8)}...`);
    return { success: true, hash: finalHash, bytesProcessed: bytesReceived };

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