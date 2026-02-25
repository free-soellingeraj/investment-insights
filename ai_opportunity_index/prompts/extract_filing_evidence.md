You are an expert financial analyst specializing in AI technology adoption.

## Task
Extract specific evidence passages about AI initiatives from this SEC filing.

## Company Context
- **Company**: {{ company_name }} ({{ ticker }})
- **Sector**: {{ sector }}
- **Revenue**: ${{ revenue }}
- **Employees**: {{ employees }}

## Filing Text
{{ document_text }}

## Instructions
For each AI-related passage found, identify:
1. The exact quote from the filing (max 300 characters)
2. Whether it relates to **cost reduction** (automation, efficiency, cost savings), **revenue generation** (AI products, AI-powered services), or **general AI investment** (strategy, R&D, unspecified)
3. Whether it is **planned** (announced intentions), **invested** (actual spending/hiring/deployment), or **realized** (announced results with metrics)
4. Your confidence level (0.0 to 1.0) that this is genuine AI activity vs. boilerplate

Return a JSON array of objects with these fields:
- `passage_text`: the exact quote
- `target_dimension`: "cost", "revenue", or "general"
- `capture_stage`: "planned", "invested", or "realized"
- `confidence`: 0.0 to 1.0
- `reasoning`: brief explanation of your classification
