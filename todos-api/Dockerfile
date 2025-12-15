# Use a supported Node LTS version
FROM node:20-alpine AS builder

WORKDIR /app

# Copy package files first
COPY package*.json ./

# Install only production dependencies
RUN npm install --production

# Copy source code
COPY . .

# Create a minimal runtime image
FROM node:20-alpine
WORKDIR /app

# Copy only necessary files from builder
COPY --from=builder /app ./

# Use non-root user for security
RUN addgroup -S appgroup && adduser -S appuser -G appgroup
USER appuser

# Expose port
EXPOSE 8082

# Run the application
CMD ["node", "server.js"]
