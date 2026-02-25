# Product Features

## Core Scoring Pipeline

### 4-Value Framework
1. **Cost Opportunity**: AI's potential to reduce costs (BLS occupational applicability)
2. **Cost Capture**: Actual cost-saving AI deployments detected
3. **Revenue Opportunity**: AI's potential to grow revenue (industry classification)
4. **Revenue Capture**: Actual revenue-facing AI products detected

### 4-Stage Evidence Pipeline
1. **Extract**: Keyword or LLM extractors on filings, news, patents, jobs
2. **Classify**: Target (cost/revenue/general) + stage (planned/invested/realized)
3. **Value**: Formula or LLM dollar estimation (3-year horizon)
4. **Aggregate**: Weighted → quadrant (AI Leaders / Untapped Potential / Over-investing / AI-Resistant)

### Realization Sub-Scorers
Filing NLP (35%), Product Analysis (25%), Job Signals (20%), Patent Signals (20%)

## Web Application

- **Landing** (`/`): Hero, performance stats, backtest chart, $99/month CTA
- **Dashboard** (`/dashboard`): Token-auth, sortable tables, sector/quadrant filters, Plotly charts
- **Status** (`/status`): Pipeline funnel, per-source progress, company drilldown with link verification (GH/Careers/IR/Blog), heatmap
- **Company** (`/company/{ticker}`): Scoring breakdown, evidence detail, peer comparison

## CLI Scripts

| Script | Purpose |
|--------|---------|
| `build_universe.py` | Bootstrap 10K+ companies from SEC EDGAR |
| `collect_data.py` | Fetch financials, download SEC filings |
| `collect_evidence.py` | Fetch news, patents, jobs, GitHub, Web Enrichment |
| `discover_links.py` | Homepage scrape + Gemini Flash + Google fallback |
| `score_companies.py` | Run 4-stage scoring pipeline |
| `compute_index_history.py` | Build portfolio variants, backtest vs S&P 500 |
| `export_index.py` | Export to CSV/Parquet |

## Subscription & API
- Stripe checkout, webhook processing, token-based auth, Resend emails
- REST API: `/api/status`, `/api/companies`, `/api/companies/{ticker}/refresh`, `/api/portfolios/{variant}`
