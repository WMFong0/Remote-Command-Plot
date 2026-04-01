# Remote Command Plot

FastAPI service that opens interactive SSH sessions and executes remote shell commands through API endpoints.

## Quick Start

### Local

```bash
python -m pip install --upgrade pip
pip install -r requirements.txt
uvicorn main:app --host 0.0.0.0 --port 8000
```

### Docker Compose

```bash
docker compose up --build -d
```

## Startup Readiness Script

Use [scripts/startup-readiness.sh](scripts/startup-readiness.sh) to block until the API is healthy.

```bash
bash scripts/startup-readiness.sh
```

Optional environment variables:

- `READINESS_URL` (default: `http://127.0.0.1:8000/health`)
- `READINESS_TIMEOUT_SECONDS` (default: `60`)
- `READINESS_INTERVAL_SECONDS` (default: `2`)

Example with custom URL and timeout:

```bash
READINESS_URL="http://127.0.0.1:8000/health" READINESS_TIMEOUT_SECONDS=90 bash scripts/startup-readiness.sh
```

## CI Workflow

Minimal CI is defined in [.github/workflows/ci.yml](.github/workflows/ci.yml). It runs on push and pull request and performs:

- Dependency installation
- Python syntax compilation checks
- Module import smoke checks

## Operations Runbook

### Health and Session Checks

```bash
curl -s http://127.0.0.1:8000/health
curl -s http://127.0.0.1:8000/sessions
```

### Common Runtime Commands

```bash
# Start
uvicorn main:app --host 0.0.0.0 --port 8000

# Start with a different port
uvicorn main:app --host LOCALHOST --port YOUR PORT

# Start with compose
docker compose up -d

# Stop with compose
docker compose down
```

### Log Access

Local logs are written to `logs/app.log` by default. In Docker Compose, logs are persisted in the `app_logs` volume.

```bash
# Container logs
docker compose logs -f remote-command-plot
```

### Safe Recovery

1. Confirm service health with `/health`.
2. If unhealthy, inspect logs for SSH/authentication failures.
3. Restart service:
   ```bash
   docker compose restart remote-command-plot
   ```
4. If still unhealthy, recreate container:
   ```bash
   docker compose down && docker compose up -d --build
   ```

### Configuration Checklist

- Set `APP_RELOAD=0` in production.
- Tune `SESSION_TTL_SECONDS` for expected session lifetimes.
- Keep `LOG_JSON=1` when shipping logs to an aggregator.
- Rotate credentials used for SSH access and avoid reusing personal accounts.
