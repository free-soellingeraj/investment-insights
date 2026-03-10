# Implementation: [data-integrity] Pipeline Freshness — Plan #1

## Plan Reference
Plan #178

## Code Impact
### Files Likely Affected
- ai_opportunity_index/pipeline/runner.py
- scripts/daily_refresh.py
- ai_opportunity_index/data/sec_edgar.py

### Proposed Actions
1. Identify companies stuck in scoring pipeline (>30 days without new score)
2. Check if SEC filing extraction is running for stale companies
3. Verify news extraction isn't rate-limited or failing silently
4. Add monitoring for pipeline throughput (scores/hour metric)
5. Consider priority queue for most-viewed companies

## Implementation Steps
# [data-integrity] Pipeline Freshness — Plan #1

## Context
This plan is based on 3 investigation findings from the data-integrity team.

## Key Findings
- Passage provenance: 1 passages missing URLs out of 10200 total (0% gap). 149 have URLs but no publisher attribution. Provenance coverage is good.
- Valuation discrepancies: 5215 discrepancies across 487 companies. HIGH: 5215 unresolved discrepancies is concerning — suggests conflicting evidence sources.
- Evidence freshness (90d) by dimension: general: 4192 groups (conf=0.874, 0 stale >30d); revenue: 3644 groups (conf=0.907, 0 stale >30d); cost: 891 groups (conf=0.894, 0 stale >30d). Freshness looks acceptable.

## Issues Identified
- HIGH: 5215 unresolved discrepancies is concerning — suggests conflicting evidence sources.

## Challenges Raised

- @data-integrity-investigator Discrepancies aren't necessarily bad — they could mean we have multiple perspectives on the same evidence. But are these discrepancies between high-authority sources or lo
- @data-integrity-investigator Coverage numbers are helpful. Let's make sure we're not double-counting across evidence groups.

## Proposed Actions
1. Identify companies stuck in scoring pipeline (>30 days without new score)
2. Check if SEC filing extraction is running for stale companies
3. Verify news extraction isn't rate-limited or failing silently
4. Add monitoring for pipeline throughput (scores/hour metric)
5. Consider priority queue for most-viewed companies

## Success Criteria
- Measurable improvement in the metrics identified in findings
- No regression in other scoring dimensions
- Changes pass code review and automated tests

## Files Likely Affected
- ai_opportunity_index/pipeline/runner.py
- scripts/daily_refresh.py
- ai_opportunity_index/data/sec_edgar.py

## Expected Impact
Targeted improvement to pipeline freshness based on data-driven investigation by the data-integrity team.


## Test Instructions
- Measurable improvement in the metrics identified in findings
- No regression in other scoring dimensions
- Changes pass code review and automated tests

## Verification Checklist
- [ ] All action items from the plan are addressed
- [ ] No regressions in existing tests
- [ ] Code impact matches what was described
- [ ] Human has tested per the test instructions above
