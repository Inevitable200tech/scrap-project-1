# --- Stage 1: Build ---
FROM mcr.microsoft.com/playwright:v1.57.0-jammy AS builder

WORKDIR /app

# Copy dependency files first for caching
COPY package*.json ./
RUN npm install

# Copy EVERYTHING from your root (including public/)
COPY . .

# VERIFICATION: List files to ensure 'public' is inside the builder
RUN ls -la /app && ls -la /app/public

# Build the project
RUN npm run build 

# --- Stage 2: Production ---
FROM mcr.microsoft.com/playwright:v1.57.0-jammy

# Environment setup
RUN apt-get update && apt-get install -y \
    python3 \
    ffmpeg \
    curl \
    && curl -L https://github.com/yt-dlp/yt-dlp/releases/latest/download/yt-dlp -o /usr/local/bin/yt-dlp \
    && chmod a+rx /usr/local/bin/yt-dlp \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install production dependencies only
COPY package*.json ./
RUN npm install --omit=dev

# Move all necessary folders from the builder stage
COPY --from=builder /app/dist ./dist
COPY --from=builder /app/public ./public

# Ensure directory permissions
RUN mkdir -p /tmp/scraper-profile && chmod -R 777 /tmp/scraper-profile

ENV NODE_ENV=production
EXPOSE 3000

CMD ["node", "dist/main.js"]