# Plan: Fix Scoring Calibration Regression & Coverage Gaps

## Context

Investigation of the scoring pipeline on Mar 9-10, 2026 revealed five issues:

1. **Batch scoring regression**: Avg opportunity score jumped to 0.770 (all-time avg 0.393). 15+ companies simultaneously hit 1.000 at 22:27 UTC on Mar 9. This is a suspected scoring regression.
2. **Realization near zero**: Avg realization is 0.0006. 97% of companies fall in low-realization quadrants, suggesting the capture dimension is data-starved.
3. **Low valuation coverage**: Only 10.4% of active companies have evidence groups and dollar valuations.
4. **Materialized view gap**: 3,317 rows in `latest_company_scores` vs 4,450 scored companies -- 1,133 missing from the dashboard view.
5. **capture_probability missing on 73%**: Only 3,654 of 13,641 score rows have a non-null `capture_probability`.

## Root Cause Analysis

### 1. Opportunity Score Spike (LIKELY BUG)

The opportunity score is computed via `compute_opportunity_score()` in `ai_opportunity_index/scoring/ai_opportunity.py`. It uses BLS/SOC AI applicability data mapped from SIC codes. The score is normalized via `_normalize_score(value, OPP_NORMALIZE_MIN=0.03, OPP_NORMALIZE_MAX=0.65)`.

**Most likely cause**: The `_score_from_sector()` fallback path (used when SIC is null) applies `base_score * OPP_B2B_BOOST * OPP_AI_INDUSTRY_BOOST` for Technology B2B companies: `0.32 * 1.3 * 1.4 = 0.582`, normalizing to `(0.582 - 0.03) / (0.65 - 0.03) = 0.89`. With employee scaling on top, this saturates to 1.0. If a batch of companies lost their SIC codes (e.g., a data refresh cleared them), they'd all fall into sector-based scoring with inflated scores.

The `compute_index_4v()` function computes `opportunity = 0.5 * cost_opportunity + 0.5 * revenue_opportunity`. If both dimensions independently saturate, the composite hits 1.0. This is consistent with 15+ companies simultaneously hitting 1.000.

**Key diagnostic**: Check whether the companies that spiked have `sic IS NULL` in the companies table.

### 2. Realization Near Zero (STRUCTURAL, NOT A BUG)

The capture dimension depends on `ClassifiedScorerOutput` from sub-scorers (filing NLP, product analysis, web enrichment, GitHub, analyst). If most companies have no filing extractions, no news extractions, and no web enrichment data, all sub-scorers return None and the capture score defaults to 0.0. Only 10.4% valuation coverage confirms this is a coverage problem, not a code bug.

### 3. Materialized View Gap (FILTER + TIMING)

The view definition includes `WHERE c.is_active = true`. The 1,133 gap comes from inactive companies with scores. The view refresh runs at the end of scoring -- if the script crashes before that point, the view becomes stale.

### 4. capture_probability Missing (EXPECTED)

Only populated when `_compute_ai_index_for_company()` returns a result, which requires final valuations. Since only 10.4% have valuations, 73% missing is consistent.

## Proposed Actions

### Action 1: Add Guardrails to Prevent Opportunity Score Saturation
**File**: `ai_opportunity_index/scoring/ai_opportunity.py`
- In `_score_from_sector()`, cap raw scores before normalization to `OPP_NORMALIZE_MAX * 0.9`
- In `score_cost_opportunity()` and `score_revenue_opportunity()`, add the same cap
- Log WARNING when any company's composite_opportunity exceeds 0.95

### Action 2: Add Batch Score Anomaly Detection
**File**: `scripts/score_companies.py`
- After scoring all companies, compute batch mean/stddev of `composite_opp_score`
- If batch mean exceeds historical mean by >2 stddev, log ERROR
- Add `--dry-run` flag for validation without saving

### Action 3: Diagnose and Fix the SIC-Null Problem
**File**: `scripts/score_companies.py`
- Before scoring, log count of companies with `sic IS NULL`
- When using sector fallback, apply 0.7x penalty and add "sector_fallback_used" flag

### Action 4: Improve Materialized View Robustness
**Files**: `ai_opportunity_index/storage/db.py`, `scripts/score_companies.py`
- Add view refresh at START of scoring (not just end)
- Add standalone `--refresh-view` CLI command
- Log row count after refresh and warn if gap exceeds 5%

### Action 5: Backfill capture_probability for Legacy Scores
**File**: `scripts/backfill_capture_probability.py` (new)
- Iterate scores with NULL capture_probability, compute from existing valuations
- Set `AI_INDEX_P_BASE` (0.05) for companies without valuations

### Action 6: Expand Extraction Coverage
**File**: `scripts/daily_refresh.py`
- Ensure extraction runs for ALL active companies, not just those with refresh requests
- Prioritize companies with zero evidence groups

## Files Likely Affected

| File | Changes |
|------|---------|
| `ai_opportunity_index/scoring/ai_opportunity.py` | Raw score caps, saturation logging |
| `scripts/score_companies.py` | Anomaly detection, SIC diagnostics, dry-run, view refresh |
| `ai_opportunity_index/storage/db.py` | View refresh logging |
| `ai_opportunity_index/config.py` | New constants for thresholds |
| `scripts/daily_refresh.py` | Expand extraction coverage |
| `scripts/backfill_capture_probability.py` | New backfill script |

## Success Criteria

1. No company should have `composite_opp_score > 0.95` via sector fallback path
2. Scoring script logs WARNING if batch mean opportunity exceeds historical mean + 2 stddev
3. Materialized view gap < 5% of scored active companies
4. capture_probability coverage > 50% after backfill
5. >25% of active companies have at least one evidence group within 30 days

## Priority Order

1. **Action 1** (score cap) -- immediate, prevents future regressions
2. **Action 3** (SIC diagnostics) -- immediate, identifies root cause
3. **Action 4** (view robustness) -- quick win, fixes dashboard discrepancy
4. **Action 2** (anomaly detection) -- medium-term, prevents future incidents
5. **Action 5** (backfill) -- medium-term, improves completeness
6. **Action 6** (extraction coverage) -- longer-term, addresses structural gap
