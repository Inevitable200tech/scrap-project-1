import { S3Client } from "@aws-sdk/client-s3";
import { Upload } from "@aws-sdk/lib-storage";
import { MongoClient, Db } from "mongodb";
import axios from "axios";
import crypto from "crypto";
import { PassThrough, Readable } from "stream";
import { spawn } from "child_process";
import { R2_CONFIG, MONGO_URI, MONGO_DB } from "./config.js";

// Exported for use in main.ts (for generating Signed URLs)
export const s3Client = new S3Client({
  region: "auto",
  endpoint: R2_CONFIG.endpoint,
  credentials: R2_CONFIG.credentials,
});

let db: Db;
const mongoClient = new MongoClient(MONGO_URI);

// Exported for use in main.ts (for listing videos)
export async function getDb() {
  if (!db) {
    await mongoClient.connect();
    db = mongoClient.db(MONGO_DB);
  }
  return db;
}

export interface ProcessingResult {
  success: boolean;
  hash?: string;
  r2Key?: string;
  error?: string;
  isDuplicate?: boolean;
}

/**
 * Downloads/Streams video to R2 and saves metadata to MongoDB
 */
export async function processAndStoreVideo(videoUrl: string, title: string): Promise<ProcessingResult> {
  let ffmpegProcess: any = null;

  try {
    const isM3U8 = videoUrl.includes('.m3u8');
    const uploadStream = new PassThrough();
    const hash = crypto.createHash('sha256');
    const r2Key = `videos/${Date.now()}-${crypto.randomBytes(4).toString('hex')}.mp4`;

    let videoSource: Readable;

    if (isM3U8) {
      console.log(`[STORAGE] HLS detected. Starting FFmpeg: ${videoUrl}`);
      ffmpegProcess = spawn('ffmpeg', [
        '-i', videoUrl,
        '-c', 'copy',
        '-bsf:a', 'aac_adtstoasc',
        '-movflags', 'frag_keyframe+empty_moov',
        '-f', 'mp4',
        'pipe:1'
      ]);
      
      videoSource = ffmpegProcess.stdout;

      // Handle FFmpeg errors to prevent hanging
      ffmpegProcess.on('error', (err: any) => console.error('[FFMPEG SPAWN ERROR]:', err));
      ffmpegProcess.stderr.on('data', (data: any) => {
        if (data.toString().includes('Error')) console.error(`[FFMPEG]: ${data}`);
      });
    } else {
      console.log(`[STORAGE] Direct link detected: ${videoUrl}`);
      const response = await axios({
        method: 'get',
        url: videoUrl,
        responseType: 'stream',
        timeout: 90000, // Increased timeout for large files
      });
      videoSource = response.data;
    }

    // Process data through Hash and Upload simultaneously
    videoSource.on('data', (chunk) => hash.update(chunk));
    videoSource.pipe(uploadStream);

    const upload = new Upload({
      client: s3Client,
      params: {
        Bucket: R2_CONFIG.bucket,
        Key: r2Key,
        Body: uploadStream,
        ContentType: "video/mp4",
      },
      queueSize: 4,
      partSize: 1024 * 1024 * 5,
      leavePartsOnError: false,
    });

    await upload.done();
    const finalHash = hash.digest('hex');

    const database = await getDb();
    
    // Check for duplicates based on hash before inserting
    const existing = await database.collection('videos').findOne({ hash: finalHash });
    if (existing) {
      console.log(`[STORAGE] Duplicate found. Skipping DB insert for ${r2Key}`);
      return { success: true, hash: finalHash, r2Key: existing.r2Key, isDuplicate: true };
    }

    await database.collection('videos').insertOne({
      title,
      hash: finalHash,
      r2Key: r2Key,
      originalUrl: videoUrl,
      type: isM3U8 ? 'hls_converted' : 'direct_mp4',
      processedAt: new Date(),
    });

    return { success: true, hash: finalHash, r2Key };
  } catch (error: any) {
    // Cleanup FFmpeg if it crashes
    if (ffmpegProcess) ffmpegProcess.kill('SIGKILL');
    console.error(`[STORAGE ERROR]: ${error.message}`);
    return { success: false, error: error.message };
  }
}