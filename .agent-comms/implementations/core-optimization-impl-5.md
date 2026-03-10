# Implementation: [core-optimization] Scoring Calibration — Plan #1

## Plan Reference
Plan #177

## Code Impact
### Files Likely Affected
- ai_opportunity_index/scoring/calibration.py
- ai_opportunity_index/scoring/composite.py

### Proposed Actions
1. Review Platt scaling parameters for capture_probability
2. Check if stddev of capture_probability is too low (clustering issue)
3. Compare calibration curves across different company sectors
4. Validate against historical outcomes where available
5. Update calibration.py scaling factors if needed

## Implementation Steps
# [core-optimization] Scoring Calibration — Plan #1

## Context
This plan is based on 3 investigation findings from the core-optimization team.

## Key Findings
- Scoring stats (24h): 2795 scores computed. Average opportunity score: 0.325 (scale 0-1). Average realization score: 0. Average combined opportunity: 0.770. High volume day.
- Calibration stats (7d): avg capture_probability=0.460, stddev=0.157, range=[0.050, 0.970], n=2580. Calibration looks reasonable.
- Quadrant distribution (7d): high_opp_low_real=2402, low_opp_low_real=414. Total: 2816 scores. CONCERN: Most companies in high_opp_low_real — realization scores may need recalibration.

## Issues Identified
- CONCERN: Most companies in high_opp_low_real — realization scores may need recalibration.

## Challenges Raised

- @core-optimization-investigator The average realization score being 0 is a major red flag. That means none of our companies are showing real-world AI adoption. Is this a data problem or a scoring meth
- @core-optimization-investigator The stddev tells us about spread, but not about accuracy. What we really need is a calibration curve — are companies with 80% capture_probability actually being capture
- @core-optimization-investigator If most companies are in high_opp_low_real, that's either a genuine market signal (lots of AI opportunity but early days for real adoption) or it means our realization 

## Proposed Actions
1. Review Platt scaling parameters for capture_probability
2. Check if stddev of capture_probability is too low (clustering issue)
3. Compare calibration curves across different company sectors
4. Validate against historical outcomes where available
5. Update calibration.py scaling factors if needed

## Success Criteria
- Measurable improvement in the metrics identified in findings
- No regression in other scoring dimensions
- Changes pass code review and automated tests

## Files Likely Affected
- ai_opportunity_index/scoring/calibration.py
- ai_opportunity_index/scoring/composite.py

## Expected Impact
Targeted improvement to scoring calibration based on data-driven investigation by the core-optimization team.


## Test Instructions
- Measurable improvement in the metrics identified in findings
- No regression in other scoring dimensions
- Changes pass code review and automated tests

## Verification Checklist
- [ ] All action items from the plan are addressed
- [ ] No regressions in existing tests
- [ ] Code impact matches what was described
- [ ] Human has tested per the test instructions above
