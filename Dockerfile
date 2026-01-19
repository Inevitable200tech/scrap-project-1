# Use official Playwright image (all dependencies + Chromium pre-installed)
FROM mcr.microsoft.com/playwright:v1.49.0-jammy

# Set working directory
WORKDIR /app

# Copy package files
COPY package*.json ./

# Install Node.js dependencies
RUN npm ci --omit=dev

# Copy source code
COPY . .

# ← ADD THIS LINE: Compile TypeScript to JavaScript
RUN npm run build

# Expose port
EXPOSE 3000

# Start the server
CMD ["npm", "start"]