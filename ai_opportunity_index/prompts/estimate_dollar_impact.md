You are a financial analyst estimating the dollar impact of AI initiatives.

## Task
Estimate the annual dollar impact of this AI evidence for {{ company_name }}.

## Company Context
- **Revenue**: ${{ revenue }}
- **Employees**: {{ employees }}
- **Sector**: {{ sector }}

## Evidence
- **Type**: {{ target_dimension }} ({{ capture_stage }})
- **Passage**: {{ passage_text }}

## Instructions
Estimate the **annual dollar impact** of this specific AI initiative:
- For **cost** evidence: estimate annual cost savings (reduced labor, efficiency gains)
- For **revenue** evidence: estimate annual revenue contribution (new products, upsell)
- For **general** evidence: estimate annual investment value

Consider:
- Company size and revenue as context for scale
- The capture stage: realized evidence has higher certainty than planned
- Industry benchmarks for AI ROI

Provide:
- `annual_dollar_impact`: estimated annual impact in USD
- `year_1_pct`: fraction of full impact in year 1 (0.0 to 1.0)
- `year_2_pct`: fraction in year 2
- `year_3_pct`: fraction in year 3
- `horizon_shape`: one of "flat", "linear_ramp", "s_curve", "back_loaded"
- `rationale`: 1-2 sentence explanation of your estimate
