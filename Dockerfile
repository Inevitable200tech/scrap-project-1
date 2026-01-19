# Use official Playwright image with all dependencies pre-installed
FROM mcr.microsoft.com/playwright:v1.49.0-jammy

# Set working directory inside container
WORKDIR /app

# Copy package files first (optimizes caching)
COPY package*.json ./

# Install Node.js dependencies
RUN npm ci

# Copy the rest of your application code
COPY . .

# Expose the port your app listens on
EXPOSE 3000

# Start the server
CMD ["npm", "start"]