# Debugging

## Logging Setup
All scripts use Python `logging.basicConfig(level=logging.INFO)`. Format: `%(asctime)s %(name)s %(levelname)s: %(message)s`.

For debug logging: `logging.basicConfig(level=logging.DEBUG)`
For SQLAlchemy queries: `logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)`

## Environment Variables Required

| Variable | Required | Default |
|----------|----------|---------|
| DATABASE_URL | Yes | `postgresql://localhost:5432/ai_opportunity_index` |
| GOOGLE_API_KEY | For link discovery | - |
| *(FIRECRAWL_API_KEY removed — no longer needed)* | | |
| GITHUB_TOKEN | For GitHub signals | - |
| PATENTSVIEW_API_KEY | For patents | - |
| STRIPE_SECRET_KEY | For subscriptions | - |
| RESEND_API_KEY | For emails | - |
| BASE_URL | For web app | `http://localhost:8080` |

## Running Locally

```bash
source .venv/bin/activate
source .env

# Database
alembic upgrade head

# Web server
litestar run --app web.app:app --host 0.0.0.0 --port 8080 --reload

# Full pipeline
python scripts/build_universe.py
python scripts/collect_data.py
python scripts/discover_links.py --sources website --limit 50
python scripts/collect_evidence.py
python scripts/score_companies.py
```

## Common Issues

**DB connection**: Check PostgreSQL running, verify DATABASE_URL format, test with `psql $DATABASE_URL`

**Missing API keys**: Compare .env with .env.example. Most scripts skip gracefully if key missing.

**Rate limits**: GitHub (5000 req/hr authenticated), Yahoo Finance (built-in 0.2s delay)

**LLM errors**: Gemini Flash requires `GOOGLE_API_KEY`. Claude requires `ANTHROPIC_API_KEY`.

**Materialized view stale**: Run `REFRESH MATERIALIZED VIEW CONCURRENTLY latest_company_scores;` after scoring.

## Database Inspection

```sql
-- Count companies with scores
SELECT COUNT(*) FROM company_scores;

-- Latest scoring run
SELECT * FROM scoring_runs ORDER BY started_at DESC LIMIT 1;

-- Companies missing links
SELECT ticker FROM companies WHERE careers_url IS NULL AND is_active = true LIMIT 20;

-- Evidence by type
SELECT evidence_type, COUNT(*) FROM evidence GROUP BY evidence_type;
```
