FROM python:3.9-slim

WORKDIR /app

# 1. Install build tools needed for C-extensions
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# 2. Relax GCC strictness to allow legacy C code to compile if needed
ENV CFLAGS="-fcommon"

COPY . /app

# 3. THE FIX: Install thriftpy2 manually BEFORE requirements.txt
# This satisfies the py_zipkin dependency so the broken thriftpy is skipped.
RUN pip install --upgrade pip && \
    pip install thriftpy2 && \
    pip install --no-cache-dir -r requirements.txt

CMD ["python3", "main.py"]
