# Dockerfile
FROM python:3.12-slim

# System deps
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt \
    && pip install --no-cache-dir gunicorn

# Copy app
COPY . .

ENV PYTHONUNBUFFERED=1
# Tuneables
ENV TRINO_MAX_CONCURRENCY=4
ENV DASHBOARD_CACHE_TTL=30
ENV SMALL_CACHE_TTL=30


CMD gunicorn app:app --workers=2 --threads=4 --timeout=60 --log-level=info --bind=0.0.0.0:$PORT
