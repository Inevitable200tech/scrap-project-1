import path from 'path';
import os from 'os';
import dotenv from 'dotenv';

dotenv.config({ path: "cert.env" });

// Helper to require env vars
function requireEnv(name: string): string {
    const value = process.env[name];
    if (!value) {
        throw new Error(`Missing required environment variable: ${name}`);
    }
    return value;
}

export const PORT = parseInt(process.env.PORT || '3000', 10);
export const CONCURRENCY_LIMIT = 1;
export const USER_DATA_DIR = path.join(os.tmpdir(), 'scraper-profile');

export const BROWSER_ARGS = [
    '--no-sandbox', 
    '--disable-dev-shm-usage', 
    '--no-zygote',
    '--disable-setuid-sandbox', 
    '--disable-infobars', 
    '--window-size=1280,900',
    '--disable-blink-features=AutomationControlled',
];

// --- Configuration ---
export const R2_CONFIG = {
    endpoint: `https://${requireEnv('R2_ACCOUNT_ID')}.r2.cloudflarestorage.com`,
    bucket: requireEnv('R2_BUCKET_NAME'),
    credentials: {
        accessKeyId: requireEnv('R2_ACCESS_KEY_ID'),
        secretAccessKey: requireEnv('R2_SECRET_ACCESS_KEY'),
    },
};

export const MONGO_URI = requireEnv('MONGODB_URI');
export const MONGO_DB = requireEnv('MONGODB_DB_NAME');
