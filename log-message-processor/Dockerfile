# Python 3.7 is the "Safe Zone" for legacy thriftpy compilation
FROM python:3.7-slim-buster

WORKDIR /app

# Install build tools needed for the Thrift C-extensions
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

COPY . /app

# Use an older pip version to ensure legacy support
RUN pip install --upgrade "pip<21.0" && \
    pip install --no-cache-dir -r requirements.txt

CMD ["python", "main.py"]