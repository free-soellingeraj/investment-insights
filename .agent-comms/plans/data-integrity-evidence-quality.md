# Plan: Improve Evidence Quality and Freshness

**Team**: data-integrity
**Status**: review
**Created**: 2026-03-10

## Context

Data integrity investigation revealed three high-impact issues degrading scoring accuracy:

1. **Trillion-dollar LLM hallucinations**: NVDA revenue valuations total $3.28T positive. The `check_dollar_sanity` function has a $10B global cap but only *flags* values above it -- it does not *cap* them. Additionally, the sanity check only runs on `dollar_mid` (the average of low/high), meaning individual `dollar_low`/`dollar_high` values can still be wildly inflated.

2. **Evidence staleness**: 59% of evidence groups (5,133 of 8,727) are >90 days old. Only 2.4% from the last 7 days. The `compute_recency` function in `evidence_valuation.py` uses `0.7^years_old` with a floor of 0.3, which means even 3-year-old evidence retains 30% weight. Meanwhile, `calibration.py` defines a more aggressive `temporal_weight` function with source-specific half-lives (news=30d, SEC=365d) but this function is **never called** in the valuation pipeline.

3. **Confidence clustering at 0.87-0.91**: All source types produce nearly identical calibrated confidence. Root cause: the Platt sigmoid `sigmoid(2x - 1)` maps the typical LLM output range of 0.7-0.9 to a narrow band around 0.73-0.88, and then the passage-level confidence is not differentiated by source type. The `calibrate_confidence` function accepts a `source_type` parameter but the valuation pipeline always passes `"llm"` regardless of whether the underlying source is a SEC filing, news article, or GitHub repo.

## Root Cause Analysis

### Issue 1: Dollar Hallucinations
- **File**: `ai_opportunity_index/scoring/calibration.py:178`
- `check_dollar_sanity` flags but does not cap values exceeding `_GLOBAL_DOLLAR_CAP` ($10B)
- **File**: `ai_opportunity_index/scoring/evidence_valuation.py:539-543`
- Sanity check runs only on `dollar_mid`, not on raw `dollar_low`/`dollar_high`
- The 2x revenue cap works but only if `company_revenue` is passed correctly -- and it is, but the cap allows $2x a company's full revenue per single evidence group (NVDA ~$130B revenue = $260B cap per group)
- The LLM prompt (`prompts/value_preliminary_evidence.md`) says "Estimate the annualized dollar impact" but provides no guardrails on magnitude relative to company revenue

### Issue 2: Evidence Staleness
- **File**: `ai_opportunity_index/scoring/evidence_valuation.py:129-135`
- `compute_recency` uses a single decay curve (`0.7^years`) for all source types, with a generous floor of 0.3
- **File**: `ai_opportunity_index/scoring/calibration.py:246-269`
- `temporal_weight` implements source-specific half-lives (news=30d, SEC=365d, GitHub=60d) but is **unused**
- The munger (`evidence_munger.py`) does not filter or deprioritize stale evidence
- 599 undated evidence groups get `RECENCY_FLOOR = 0.3` -- better than nothing but not ideal

### Issue 3: Confidence Clustering
- **File**: `ai_opportunity_index/scoring/evidence_valuation.py:346-347`
- Both preliminary and final valuations call `calibrate_confidence(raw_confidence, "llm")` -- hardcoded to `"llm"` source type
- The evidence group carries `source_types` (e.g., `["filing", "news"]`) but this is not used for calibration
- **File**: `ai_opportunity_index/scoring/calibration.py:24-30`
- Different calibration curves exist for different source types but are never reached because `"llm"` is always passed

## Proposed Actions

### Action 1: Fix Dollar Sanity Cap (Critical, Low Effort)

**Change**: Make `check_dollar_sanity` actually cap values exceeding `_GLOBAL_DOLLAR_CAP`, not just flag them. Also apply sanity checking to `dollar_low` and `dollar_high` individually, not just `dollar_mid`.

**File**: `ai_opportunity_index/scoring/calibration.py`
- In `check_dollar_sanity()` (line 178): change the `> _GLOBAL_DOLLAR_CAP` branch to set `adjusted = _GLOBAL_DOLLAR_CAP` after logging the warning
- Reduce `_MAX_REVENUE_MULTIPLE` from 2.0 to 0.5 (a single AI initiative rarely exceeds 50% of total revenue)

**File**: `ai_opportunity_index/scoring/evidence_valuation.py`
- In `run_final_valuations()` (around line 529-543): apply `check_dollar_sanity` to `dollar_low` and `dollar_high` individually before computing `dollar_mid`, not just to `dollar_mid` after the fact
- Add company revenue as context in the sanity check for both bounds

**File**: `ai_opportunity_index/prompts/value_preliminary_evidence.md`
- Add explicit guardrail text: "Dollar estimates for a single AI initiative should typically be 0.1%-10% of company revenue. Estimates exceeding 50% of revenue require extraordinary justification."

### Action 2: Use Source-Specific Temporal Decay (High Impact, Medium Effort)

**Change**: Replace the single `compute_recency` decay curve with the existing `temporal_weight` function from calibration.py, which already implements source-specific half-lives.

**File**: `ai_opportunity_index/scoring/evidence_valuation.py`
- In `run_final_valuations()` (line 556): replace `compute_recency(group.date_latest)` with a call that uses `temporal_weight` from calibration.py, passing the dominant source type from the evidence group
- For groups with multiple source types, use the longest half-life (most generous decay)
- For undated groups (599 currently), assign a default age of 180 days instead of using the flat `RECENCY_FLOOR`

**File**: `ai_opportunity_index/scoring/evidence_munger.py`
- In `munge_evidence()`: add a pre-filter that drops passages older than 2 years (730 days) to prevent ancient evidence from entering the pipeline at all
- Log count of dropped passages for transparency

### Action 3: Source-Aware Confidence Calibration (High Impact, Low Effort)

**Change**: Pass the actual source type to `calibrate_confidence` instead of hardcoding `"llm"`.

**File**: `ai_opportunity_index/scoring/evidence_valuation.py`
- In `run_preliminary_valuations()` (line 346): determine dominant source type from `group.source_types` and pass it to `calibrate_confidence`
- In `run_final_valuations()` (line 513): same change
- Map evidence group source types to calibration source types: `"filing"` -> `"sec_filing"`, `"news"` -> `"news"`, `"github"` -> `"github"`, `"analyst_report"` -> `"analyst"`, default -> `"llm"`

**File**: `ai_opportunity_index/scoring/calibration.py`
- Add `"filing"` as an alias for `"sec_filing"` in `_CALIBRATION_CURVES` for convenience
- Widen the spread between source-type curves to produce more differentiated confidence values:
  - `"sec_filing"`: keep at `("linear", "1.1")` -- high trust
  - `"news"`: change from `("linear", "0.8")` to `("platt", "1.5", "-0.5")` -- moderate trust with more spread
  - `"github"`: change from `("linear", "0.7")` to `("linear", "0.6")` -- weaker signal

### Action 4: Backfill Missing Evidence Dates (Medium Impact, Low Effort)

**Change**: For the 599 undated evidence groups, attempt to infer dates from passage metadata.

**File**: `ai_opportunity_index/scoring/evidence_munger.py`
- In `munge_evidence()`, after building evidence groups: for groups where `date_latest is None`, attempt to parse dates from `source_filename` patterns (e.g., "10-K_2024-11-01.txt")
- If still undated, set `date_latest` to the pipeline run date minus 180 days as a conservative estimate, rather than leaving it None

## Files Likely Affected

| File | Changes |
|------|---------|
| `ai_opportunity_index/scoring/calibration.py` | Cap global dollar limit, add filing alias, widen calibration curves |
| `ai_opportunity_index/scoring/evidence_valuation.py` | Source-aware calibration, per-bound dollar sanity, use temporal_weight |
| `ai_opportunity_index/scoring/evidence_munger.py` | Staleness pre-filter, date backfill |
| `ai_opportunity_index/prompts/value_preliminary_evidence.md` | Dollar guardrail text |
| `tests/test_calibration.py` | Update tests for new cap behavior, curve changes |
| `tests/test_evidence_valuation.py` | Update tests for source-aware calibration |

## Success Criteria

1. **Dollar estimates**: No single evidence group valuation exceeds $10B. NVDA total revenue valuations drop from $3.28T to under $200B (reasonable for a $130B revenue company).
2. **Evidence freshness**: Evidence older than 2 years is excluded. Stale evidence (>90 days) weight drops from effective 0.3-0.7 to 0.05-0.3 for fast-decaying sources (news, GitHub).
3. **Confidence differentiation**: Standard deviation of calibrated confidence across source types increases from <0.02 to >0.10. SEC filings should calibrate 15-25% higher than GitHub/blog sources.
4. **Undated groups**: Reduce from 599 to <100 by inferring dates from filenames.
5. **No regressions**: All existing tests pass. Scores change but remain in valid [0,1] range.

## Implementation Order

1. Action 1 (dollar cap) -- critical fix, lowest risk
2. Action 3 (confidence calibration) -- easy win, high differentiation impact
3. Action 2 (temporal decay) -- most code changes, needs careful testing
4. Action 4 (date backfill) -- independent, can be done in parallel with any other action

## Estimated Scope

- ~150 lines of code changes across 4 files
- ~50 lines of test updates
- No schema/migration changes needed
- No new dependencies
