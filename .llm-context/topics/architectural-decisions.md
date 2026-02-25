# Architectural Decisions

## Overall Architecture

Data processing and scoring platform with modular layers:
- **Collection Layer**: External API integration → local JSON cache (`scripts/collect_evidence.py`)
- **Evidence Pipeline**: Multi-stage transformation (`scoring/pipeline/`)
- **Scoring Engines**: Dimension-specific scorers (`scoring/ai_opportunity.py`, `ai_capture.py`)
- **Storage Layer**: PostgreSQL + SQLAlchemy ORM (`storage/models.py`, `storage/db.py`)
- **Web Layer**: Litestar API + dashboard (`web/app.py`)
- **Configuration**: Centralized `config.py` — no magic constants in scoring modules

## Module Organization

```
ai_opportunity_index/
├── config.py                 # All weights, thresholds, API keys, paths
├── domains.py                # Pydantic domain models
├── cost_tracker.py           # LLM cost tracking
├── storage/                  # ORM models + CRUD
├── scoring/
│   ├── ai_opportunity.py     # Revenue/Cost opportunity scores
│   ├── ai_capture.py         # Cost/Revenue capture framework
│   ├── composite.py          # Final index + quadrant assignment
│   ├── realization/          # Sub-scorers (filing_nlp, product, job, patent)
│   └── pipeline/             # Evidence-to-dollar (base, extractors, estimators, LLM variants)
├── data/                     # Industry mappings, news signals
└── index_computation/        # Batch scoring runner
```

## Key Design Patterns

1. **Strategy Pattern**: Extract/Value stages support keyword/formula (free) and LLM/Claude (premium) implementations
2. **4-Value Classification**: Evidence tagged by Target (COST/REVENUE/GENERAL) × Stage (PLANNED/INVESTED/REALIZED)
3. **Pydantic Domain Bridge**: `domains.py` canonical models for API, DB, and pipeline boundaries
4. **Materialized View**: `latest_company_scores` for fast dashboard queries
5. **Lazy LLM Imports**: LLM code imported only when `use_llm=True`

## Data Flow: Collect → Extract → Value → Score

1. **Collect**: External APIs → local JSON cache (`data/raw/`). Scoring never contacts external services.
2. **Extract**: Raw text → `EvidencePassage` (target dimension, capture stage, confidence)
3. **Value**: Evidence + financials → `ValuedEvidence` (3-year dollar estimates, horizon shapes)
4. **Score**: Aggregation → `CompanyDollarScore` (ROI metrics, quadrant: AI Leaders / Untapped Potential / Over-investing / AI-Resistant)

## Important Decisions

1. **Collection/Scoring separation**: Collection writes cache; scoring reads only cached data. Enables offline testing, respects rate limits.
2. **Evidence classification over single score**: Each evidence item gets target+stage, enabling separate cost vs revenue tracking.
3. **Horizon-dependent dollar curves**: REALIZED=flat(1,1,1), INVESTED=ramp(0.33,0.66,1), PLANNED=slow(0.1,0.4,1).
4. **Per-company independent scoring**: No peer comparison at pipeline level. Dashboard applies relative thresholds post-hoc.
5. **Flexible dollar fallback**: Missing financials → sector/size heuristics rather than failing.
