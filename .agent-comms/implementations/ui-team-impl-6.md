# Implementation: [ui-team] Pipeline Freshness — Plan #1

## Plan Reference
Plan #179

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
# [ui-team] Pipeline Freshness — Plan #1

## Context
This plan is based on 3 investigation findings from the ui-team team.

## Key Findings
- User engagement (24h): 16 chat messages, 16 addressed. Response rate: 100%.
- Companies with stale scores (>30d): 6999. CRITICAL: Large number of active companies have outdated scores. Pipeline may be stuck.
- Scoring stats (24h): 2795 scores computed. Average opportunity score: 0.325 (scale 0-1). Average realization score: 0. Average combined opportunity: 0.770. High volume day.

## Issues Identified
- CRITICAL: Large number of active companies have outdated scores. Pipeline may be stuck.

## Challenges Raised

- @ui-team-investigator Before we panic about this, let's separate correlation from causation. Are these companies genuinely stuck, or do they just have less public data? Small-cap companies might have 
- @ui-team-investigator The average realization score being 0 is a major red flag. That means none of our companies are showing real-world AI adoption. Is this a data problem or a scoring methodology pr

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
Targeted improvement to pipeline freshness based on data-driven investigation by the ui-team team.


## Test Instructions
- Measurable improvement in the metrics identified in findings
- No regression in other scoring dimensions
- Changes pass code review and automated tests

## Verification Checklist
- [ ] All action items from the plan are addressed
- [ ] No regressions in existing tests
- [ ] Code impact matches what was described
- [ ] Human has tested per the test instructions above
