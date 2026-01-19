# Official Playwright image — includes all OS dependencies + Chromium
FROM mcr.microsoft.com/playwright:v1.49.0-jammy

# Working directory
WORKDIR /app

# Copy package files first (better layer caching)
COPY package*.json ./

# Install production dependencies only
RUN npm ci --omit=dev

# Copy source code
COPY . .

# Expose port (Render expects this)
EXPOSE 3000

# Start the app with tsx directly — no build step needed
CMD ["npx", "tsx", "main.ts"]