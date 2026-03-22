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

// --- Main Instance Configuration (Upload destination) ---
export const MAIN_INSTANCE = {
    url: requireEnv('MAIN_INSTANCE_URL'),  // e.g., http://localhost:3000
    apiKey: process.env.MAIN_INSTANCE_API_KEY || 'default-api-key',  // Optional: for auth if needed
};

// --- MongoDB Configuration (Local storage for scraper metadata) ---
export const MONGO_URI = requireEnv('MONGODB_URI');
export const MONGO_DB = requireEnv('MONGODB_DB_NAME');