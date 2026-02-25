# AI Applicability Scoring Methodology

## Taxonomic Foundation
- Uses **O*NET 29.0** occupational database
- Hierarchy: Tasks (occupation-specific) -> IWAs (cross-occupational) -> GWAs (37 broad categories)
- **IWAs (Intermediate Work Activities)** are the unit of analysis

## Dual-Perspective Classification
Each conversation is analyzed from two angles:
1. **User-goal side:** What work activity is the person trying to accomplish?
2. **AI-action side:** What work activities does the AI actually perform?

Key finding: These diverge substantially — 40% of conversations have completely disjoint
IWA sets between user and AI sides. For every 1 user-goal IWA, the AI performs ~2 IWAs.

## Three Base Metrics (per IWA)

### 1. Completion Rate
- LLM-evaluated: did the AI fulfill the user's objective?
- Validated against thumbs up/down feedback

### 2. Scope (Impact Breadth)
- 6-point scale: none / minimal / limited / moderate / significant / complete
- Measures what fraction of an activity's work the AI can assist with
- Highly correlated with (log) activity share (r=0.64)

### 3. Activity Share
- Fraction of conversations mapping to each IWA
- Equal weight distributed across multiple IWA matches per conversation
- Avg: 3 user-goal IWAs, 6 AI-action IWAs per conversation

## Composite Score Formula

### IWA-level success component:
```
success = completion_rate × fraction_of_conversations_with_moderate+_scope
```

### Occupation-level applicability score:
1. Filter: only IWAs with ≥0.05% activity share qualify (frequency threshold)
2. Weight by O*NET task importance × relevance per occupation
3. Compute separately for user-goal (standard weights) and AI-action (nonphysical weights only)
4. Final score = average of user-goal and AI-action scores

### Interpretation
- Scores are **relative** — meaningful for comparing occupations, not as absolute measures
- Employment-weighted correlation with expert predictions (Eloundou et al.): r=0.73
- Wage correlation is weak (r=0.13) — AI applicability cuts across income levels
- Education correlation is also weak — applicability spans all education requirements
