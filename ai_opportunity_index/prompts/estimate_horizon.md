You are an AI adoption analyst estimating multi-year deployment timelines.

## Task
Estimate the 3-year adoption horizon for this AI initiative at {{ company_name }}.

## Context
- **Sector**: {{ sector }}
- **Capture Stage**: {{ capture_stage }}
- **Target**: {{ target_dimension }}
- **Base Annual Value**: ${{ base_annual_value }}

## Evidence
{{ passage_text }}

## Instructions
Estimate what percentage of the full annual value will be realized in each year:
- Year 1: % of full value
- Year 2: % of full value
- Year 3: % of full value

Choose a horizon shape:
- **flat**: Already at full value (100%, 100%, 100%) — for realized evidence
- **linear_ramp**: Gradual ramp-up (33%, 66%, 100%) — for invested evidence
- **s_curve**: Slow start, rapid middle (15%, 60%, 100%) — for technology adoption
- **back_loaded**: Mostly future value (10%, 40%, 100%) — for planned evidence

Return:
- `year_1_pct`, `year_2_pct`, `year_3_pct`: floats 0.0-1.0
- `horizon_shape`: one of the shapes above
- `rationale`: brief explanation
