import { S3Client } from "@aws-sdk/client-s3";
import { Upload } from "@aws-sdk/lib-storage";
import { MongoClient, Db } from "mongodb";
import axios from "axios";
import crypto from "crypto";
import { PassThrough, Readable } from "stream";
import { spawn } from "child_process";
import { R2_CONFIG, MONGO_URI, MONGO_DB } from "./config.js";

const s3Client = new S3Client({
  region: "auto",
  endpoint: R2_CONFIG.endpoint,
  credentials: R2_CONFIG.credentials,
});

let db: Db;
const mongoClient = new MongoClient(MONGO_URI);

async function getDb() {
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
}

export async function processAndStoreVideo(videoUrl: string, title: string): Promise<ProcessingResult> {
  try {
    const isM3U8 = videoUrl.includes('.m3u8');
    const uploadStream = new PassThrough();
    const hash = crypto.createHash('sha256');
    const r2Key = `videos/${Date.now()}-${crypto.randomBytes(4).toString('hex')}.mp4`;

    let videoSource: Readable;

    if (isM3U8) {
      console.log(`[STORAGE] HLS Stream detected. Invoking FFmpeg...`);
      // FFmpeg acts as the 'downloader' for m3u8 segments
      const ffmpeg = spawn('ffmpeg', [
        '-i', videoUrl,
        '-c', 'copy',             // No re-encoding (keeps quality & saves CPU)
        '-bsf:a', 'aac_adtstoasc', // Standard fix for MP4 audio streams
        '-movflags', 'frag_keyframe+empty_moov', // Enables streaming to S3/R2
        '-f', 'mp4',
        'pipe:1'                  // Sends the output to stdout
      ]);
      
      videoSource = ffmpeg.stdout;

      ffmpeg.stderr.on('data', (data) => {
        if (data.toString().includes('Error')) console.error(`[FFMPEG]: ${data}`);
      });
    } else {
      console.log(`[STORAGE] Direct MP4 detected. Using Axios stream...`);
      const response = await axios({
        method: 'get',
        url: videoUrl,
        responseType: 'stream',
        timeout: 60000,
      });
      videoSource = response.data;
    }

    // Branch the data: one path for the SHA-256 hash, one for the R2 Upload
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
    });

    await upload.done();
    const finalHash = hash.digest('hex');

    // Save metadata to MongoDB for later retrieval
    const database = await getDb();
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
    console.error(`[STORAGE ERROR]: ${error.message}`);
    return { success: false, error: error.message };
  }
}