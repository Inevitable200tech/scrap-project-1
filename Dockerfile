# --- Stage 1: Build ---
FROM mcr.microsoft.com/playwright:v1.57.0-jammy AS builder

WORKDIR /app

COPY package*.json ./
RUN npm install

COPY . .
# Compiles TS to JS (assumes you have a 'build' script in package.json)
RUN npm run build 

# --- Stage 2: Production ---
FROM mcr.microsoft.com/playwright:v1.57.0-jammy

# Install FFmpeg and Python for the production environment
RUN apt-get update && apt-get install -y \
    python3 \
    ffmpeg \
    curl \
    && curl -L https://github.com/yt-dlp/yt-dlp/releases/latest/download/yt-dlp -o /usr/local/bin/yt-dlp \
    && chmod a+rx /usr/local/bin/yt-dlp \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Only copy production dependencies and the compiled dist folder
COPY package*.json ./
RUN npm install --omit=dev

COPY --from=builder /app/dist ./dist

# Create the profile directory used in config.ts
RUN mkdir -p /tmp/scraper-profile && chmod -R 777 /tmp/scraper-profile

ENV NODE_ENV=production
EXPOSE 3000

# Run the compiled JavaScript file directly with Node
CMD ["node", "dist/main.js"]