# Official Playwright image — updated to 1.57.0
FROM mcr.microsoft.com/playwright:v1.57.0-jammy

# Working directory
WORKDIR /app

# Copy package files first (optimizes caching)
COPY package*.json ./

# Install production dependencies only
RUN npm ci --omit=dev

# Copy source code
COPY . .

# Expose port (Render expects this)
EXPOSE 3000

# Start the app with tsx directly (no build step needed)
CMD ["npx", "tsx", "main.ts"]