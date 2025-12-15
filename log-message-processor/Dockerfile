# Use a smaller Python image
FROM python:3.9-slim

# Set the working directory
WORKDIR /app

# Copy only requirements first for caching
COPY requirements.txt .

# Upgrade pip and install dependencies
RUN pip install --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create a non-root user for security
RUN addgroup --system appgroup && adduser --system appuser --ingroup appgroup
USER appuser

# Expose default port (optional)
EXPOSE 5000

# Run the application
CMD ["python", "main.py"]
