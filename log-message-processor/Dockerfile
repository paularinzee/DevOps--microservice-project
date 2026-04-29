FROM python:3.7-slim-buster

WORKDIR /app

# Point to the Debian Archive since Buster is EOL (End of Life)
RUN sed -i s/deb.debian.org/archive.debian.org/g /etc/apt/sources.list && \
    sed -i 's|security.debian.org/debian-security|archive.debian.org/debian-security|g' /etc/apt/sources.list && \
    sed -i '/stretch-updates/d' /etc/apt/sources.list && \
    apt-get update && \
    apt-get install -y --no-install-recommends \
    gcc \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

COPY . /app

# Keep pip older for legacy thriftpy support
RUN pip install --upgrade "pip<21.0" && \
    pip install --no-cache-dir -r requirements.txt

CMD ["python", "main.py"]
