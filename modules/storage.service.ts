import { S3Client } from "@aws-sdk/client-s3";
import { Upload } from "@aws-sdk/lib-storage";
import { MongoClient, Db } from "mongodb";
import axios from "axios";
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

export async function processAndStoreVideo(videoUrl: string, title: string): Promise<ProcessingResult> {
  let ffmpegProcess: any = null;

  try {
    const isM3U8 = videoUrl.includes('.m3u8');
    const uploadStream = new PassThrough();
    const hash = crypto.createHash('sha256');
    const r2Key = `videos/${Date.now()}-${crypto.randomBytes(4).toString('hex')}.mp4`;

    let videoSource: Readable;

    if (isM3U8) {
      console.log(`[STORAGE] HLS detected. Shrinking to 480p (Ultrafast): ${videoUrl}`);
      ffmpegProcess = spawn('ffmpeg', [
        '-i', videoUrl,
        '-vf', 'scale=-2:480',           // Downscale to 480p
        '-r', '20',                      // Slightly smoother 20fps
        '-c:v', 'libx264',
        '-crf', '32',                    // High compression (lower quality)
        '-preset', 'ultrafast',          // Minimum CPU usage
        '-b:v', '500k',                  // Cap video bitrate at 500kbps
        '-maxrate', '600k',
        '-bufsize', '1000k',
        '-c:a', 'aac',
        '-b:a', '64k',                   // Low audio quality
        '-ac', '1',                      // Mono audio to save space
        '-movflags', 'frag_keyframe+empty_moov+default_base_moof', 
        '-f', 'mp4',
        'pipe:1'
      ]);
      videoSource = ffmpegProcess.stdout;
    } else {
      console.log(`[STORAGE] Direct link detected. Shrinking to 480p (Ultrafast): ${videoUrl}`);
      ffmpegProcess = spawn('ffmpeg', [
        '-i', videoUrl,
        '-vf', 'scale=-2:480',
        '-r', '20',
        '-c:v', 'libx264',
        '-crf', '32',
        '-preset', 'ultrafast',
        '-b:v', '500k',
        '-c:a', 'aac',
        '-b:a', '64k',
        '-ac', '1',
        '-movflags', 'frag_keyframe+empty_moov+default_base_moof',
        '-f', 'mp4',
        'pipe:1'
      ]);
      videoSource = ffmpegProcess.stdout;
    }

    // FFmpeg Error Handling
    ffmpegProcess.on('error', (err: any) => console.error('[FFMPEG SPAWN ERROR]:', err));
    ffmpegProcess.stderr.on('data', (data: any) => {
      const msg = data.toString();
      if (msg.includes('Error')) console.error(`[FFMPEG]: ${msg}`);
    });

    // FIX: Update hash as data flows into the pipe to the upload stream.
    // We attach the data listener to videoSource, but we don't 'pipe' twice.
    videoSource.on('data', (chunk) => hash.update(chunk));

    const upload = new Upload({
      client: s3Client,
      params: {
        Bucket: R2_CONFIG.bucket,
        Key: r2Key,
        Body: videoSource.pipe(uploadStream), // Source -> PassThrough -> R2
        ContentType: "video/mp4",
      },
      queueSize: 4,
      partSize: 1024 * 1024 * 5,
      leavePartsOnError: false,
    });

    await upload.done();
    const finalHash = hash.digest('hex');

    const database = await getDb();
    
    const existing = await database.collection('videos').findOne({ hash: finalHash });
    if (existing) {
      console.log(`[STORAGE] Duplicate found. Cleaning up R2...`);
      // Optional: Delete the file we just uploaded since it's a duplicate
      // For now, we just skip the DB entry
      return { success: true, hash: finalHash, r2Key: existing.r2Key, isDuplicate: true };
    }

    await database.collection('videos').insertOne({
      title,
      hash: finalHash,
      r2Key: r2Key,
      originalUrl: videoUrl,
      type: isM3U8 ? 'hls_converted' : 'direct_mp4_optimized',
      processedAt: new Date(),
    });

    return { success: true, hash: finalHash, r2Key };
  } catch (error: any) {
    if (ffmpegProcess) ffmpegProcess.kill('SIGKILL');
    console.error(`[STORAGE ERROR]: ${error.message}`);
    return { success: false, error: error.message };
  }
}