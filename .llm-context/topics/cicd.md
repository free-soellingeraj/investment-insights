# CI/CD

## Build Pipeline

Source Code → Cloud Build → Docker Build → Artifact Registry → Cloud Run Deploy

## Docker Setup

**Dockerfile**: Python 3.11-slim, port 8080, Litestar ASGI server.

```dockerfile
FROM python:3.11-slim
WORKDIR /app
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
EXPOSE 8080
CMD ["litestar", "run", "--app", "web.app:app", "--host", "0.0.0.0", "--port", "8080"]
```

## Cloud Build Pipelines

### Web Service (cloudbuild.yaml)
1. Build Docker image tagged with commit SHA
2. Push to Artifact Registry (`us-central1-docker.pkg.dev/$PROJECT_ID/ai-opportunity-index/app:$COMMIT_SHA`)
3. Deploy to Cloud Run: 2Gi memory, 2 CPU, 0-3 instances, `--allow-unauthenticated`

### Batch Job (cloudbuild-job.yaml)
1. Build + push image
2. Update Cloud Run Job (`ai-opportunity-scoring`)
3. Command: `python scripts/score_companies.py`, timeout 3600s

## Secrets (Google Secret Manager)
stripe-secret-key, stripe-webhook-secret, stripe-price-id, github-token, DATABASE_URL (batch only)

## .gcloudignore
Excludes: `.git`, `__pycache__`, `.env`, `data/raw/filings/`, `data/raw/company_universe.csv`, `.claude/`, `.pdf`

## Deployment Timeline
- Web: ~5-8 min (build + push + deploy)
- Batch job: build ~5 min, execution 5-60 min depending on universe size

## Database Migrations
```bash
alembic current        # Check status
alembic upgrade head   # Apply migrations
alembic downgrade -1   # Rollback last
alembic revision --autogenerate -m "description"  # Create new
```
