# Data Model

## Database Tables

### companies
Core registry. `ticker`, `exchange`, `company_name`, `cik`, `sic`, `naics`, `sector`, `industry`, `github_url`, `careers_url`, `ir_url`, `is_active`. Unique on (ticker, exchange).

### financial_observations
Time-series metrics. `company_id`, `metric` (revenue/employees/market_cap), `value`, `value_units`, `source_name`, `fiscal_period`. Indexed on (company_id, metric).

### evidence
Scoring evidence with 4-value classification. `evidence_type` (filing_nlp/patent/product/job/cost_opportunity/revenue_opportunity), `target_dimension` (cost/revenue/general), `capture_stage` (planned/invested/realized), `dollar_estimate_usd`, `dollar_year_1/2/3`, `source_excerpt`, `payload` (JSONB). GIN index on payload.

### company_scores
Per-run aggregates. Opportunity scores (`revenue_opp_score`, `cost_opp_score`), capture scores, ROI metrics, dollar metrics (`cost_opp_usd`, `revenue_opp_usd`, `cost_capture_usd`, `revenue_capture_usd`), `quadrant`, `quadrant_label`, `combined_rank`, `flags`.

### scoring_runs
Batch operations. `run_id` (UUID), `run_type` (full/partial/refresh_request), `status`, company counts.

### subscribers
SaaS accounts. `email`, `stripe_customer_id`, `stripe_subscription_id`, `status`, `plan_tier`, `access_token`.

### refresh_requests, notifications, score_change_log
Subscriber rescoring, outbound notifications, score audit trail.

## Materialized View: latest_company_scores
Joins companies with most recent scores. Indexed on company_id and ticker. Requires `REFRESH MATERIALIZED VIEW CONCURRENTLY` after scoring runs.

## Pipeline Data Models (Pydantic)
- **EvidencePassage**: source_type, passage_text, target, stage, confidence
- **ValuedEvidence**: passage + dollar_year_1/2/3, horizon_shape, valuation_method
- **CompanyDollarScore**: opportunity/capture USD by dimension, quadrant, valued_evidence list

## Migration History
1. **001**: Initial schema (all tables + materialized view)
2. **002**: Four-value framework (capture scores, ROI, target_dimension/capture_stage)
3. **003**: Financial observations table, source_excerpt on evidence
4. **004**: Dollar pipeline columns (USD on scores, dollar estimates on evidence)
5. **005**: Company URL columns (github_url, careers_url, ir_url)
