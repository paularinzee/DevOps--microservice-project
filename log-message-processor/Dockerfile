FROM python:3.7-slim-buster

WORKDIR /app

# 1. Fix the Archive repositories for Buster
RUN sed -i s/deb.debian.org/archive.debian.org/g /etc/apt/sources.list && \
    sed -i 's|security.debian.org/debian-security|archive.debian.org/debian-security|g' /etc/apt/sources.list && \
    sed -i '/stretch-updates/d' /etc/apt/sources.list && \
    apt-get update && \
    apt-get install -y --no-install-recommends gcc python3-dev && \
    rm -rf /var/lib/apt/lists/*

# 2. THE SECRET SAUCE: Set CFLAGS to allow "Legacy" C code
# This tells GCC to ignore the strict errors that are killing your build.
ENV CFLAGS="-fcommon -fno-strict-aliasing"

COPY . /app

# 3. Install requirements
# We install thriftpy2 first because it is a drop-in binary replacement 
# that prevents the broken thriftpy from ever being touched.
RUN pip install --upgrade "pip<21.0" && \
    pip install thriftpy2 && \
    pip install --no-cache-dir -r requirements.txt

CMD ["python", "main.py"]
