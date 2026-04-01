# Production-oriented image for FastAPI service
FROM python:3.11-slim

# Runtime behavior settings
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    APP_HOST=0.0.0.0 \
    APP_PORT=8000 \
    APP_RELOAD=0

WORKDIR /app

# Install dependencies first for better layer caching
COPY requirements.txt ./
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# Create unprivileged runtime user
RUN groupadd --system appgroup \
    && useradd --system --gid appgroup --create-home appuser

# Copy application source and create logs path
COPY . .
RUN mkdir -p /app/logs \
    && chown -R appuser:appgroup /app

USER appuser

EXPOSE 8000

# Health endpoint is provided by /health in main.py
HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/health', timeout=3)" || exit 1

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
