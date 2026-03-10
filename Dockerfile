# Official Playwright image — updated to 1.57.0
FROM mcr.microsoft.com/playwright:v1.57.0-jammy 
# Note: Use 'noble' or 'jammy' - both work, but noble is the latest Ubuntu LTS

# 1. Install Python (Required for yt-dlp) and FFmpeg (for merging video/audio)
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    ffmpeg \
    curl \
    && curl -L https://github.com/yt-dlp/yt-dlp/releases/latest/download/yt-dlp -o /usr/local/bin/yt-dlp \
    && chmod a+rx /usr/local/bin/yt-dlp \
    && rm -rf /var/lib/apt/lists/*

# Working directory
WORKDIR /app

# Copy package files first
COPY package*.json ./

# Install dependencies (Note: tsx needs to be in your package.json)
RUN npm install

# Copy source code
COPY . .

# Expose port
EXPOSE 3000

# Start the app
CMD ["npx", "tsx", "main.ts"]