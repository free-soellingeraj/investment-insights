# Investment Insights Constitution

A living architecture document defining the mission, structure, and principles of the investment-insights system.

---

## 1. Mission

Build an always-up-to-date information truth-seeking architecture that:

- **Compiles sources** from diverse origins and **extracts normalized evidence** to enable structured comparison across companies, sectors, and time.
- **Tracks complex provenance** for every fact: author, publish date, date learned, ephemeral source handling, and authority justification.
- **Develops source-based facts like an unbiased journalist** -- assertions are grounded in cited evidence, not opinion. The system forms views; it does not inherit them.

---

## 2. Core Architecture

### 2.1 Journalism Subsystem (Roles)

| Role | Responsibility |
|---|---|
| **Editor** | Oversees evidence collection strategy. Assigns research tasks, reviews quality, prioritizes coverage gaps, and decides when a topic has sufficient evidence. |
| **Researcher** | Discovers and collects raw sources. Extracts relevant passages, tags metadata, and delivers raw material with full provenance. |
| **Reporter** | Synthesizes evidence into structured facts. Writes narratives, generates summaries, and produces human-readable output from the fact graph. |

### 2.2 Safety Subsystem (Oversight)

- **Correctness auditing** over information trees -- verify that downstream facts remain consistent with upstream evidence.
- **Connection discovery** -- create new edges in the information graph when latent relationships are detected.
- **Counterfactual development** -- fork alternate interpretations, score their plausibility, and surface the strongest challenges to current beliefs.
- **Cross-source verification** -- detect discrepancies between independent sources and flag unresolved conflicts.

### 2.3 Fact Graph

Every attribute in the system is a node with:

```
{
  value: T | null,
  p_true: float,        # probability the value is correct
  provenance: [],        # chain of sources supporting this value
  inferred_by: str       # which inference strategy produced it
}
```

**Missing values are first-class.** Nullability is by design -- the absence of information is itself informative.

#### Inference Strategies (ordered by preference)

1. **Logical (Sudoku-style)** -- Non-probabilistic constraint propagation. If the answer can be derived with certainty from known facts, do so first. No guessing.
2. **Probabilistic** -- Guess a value, derive its downstream consequences, verify consistency with observed data. Assign confidence based on coherence.
3. **Counterfactual** -- Fork alternate realities where a different value is assumed. Compare outcomes to determine truth or inform future strategy.

### 2.4 Investment System (Roles)

| Role | Responsibility |
|---|---|
| **Accounting** | Track financial flows, costs, and P&L across all portfolios and strategies. |
| **Market Risk Management** | Monitor exposure, volatility, correlation, and tail risk. Flag breaches. |
| **Quantitative Research** | Develop, backtest, and validate investment strategies against historical and live data. |
| **Trade Execution** | Generate trade orders and execute them. Dry-run mode first; live execution requires human oversight. |
| **Financial Software Engineer** | Build and maintain the system itself -- pipelines, storage, APIs, and frontend. |

---

## 3. Data Architecture

- **Fact nodes** -- Typed values with probabilistic confidence and full provenance metadata.
- **Provenance chains** -- Who said it, when it was published, when we learned it, where it came from, why the source is authoritative, and how the data was obtained.
- **Evidence groups** -- Collections of facts from independent sources that confirm or contradict each other. Cross-source confirmation strengthens confidence; unresolved conflicts are surfaced.
- **Counterfactual branches** -- Alternate realities forked from the main fact graph. Used to stress-test beliefs and explore what-if scenarios.

---

## 4. System Objectives

1. **Investment theses research** -- Produce whitepapers, prospectus documents, and live data streams/indices grounded in the fact graph.
2. **Automated trade execution** -- Dry-run first, then live with human oversight. Every trade must be justified by a traceable chain of evidence.
3. **Portfolio design** -- Multi-customer portfolio management with per-customer risk profiles and constraints.

---

## 5. Go-Live Target

**Phase 1: Dry-run trades with full rationale chains.**

- Every trade decision produces a human-readable justification: `trade -> insight -> provenance`.
- The system must expose its inner workings transparently. No black boxes.
- A human reviewer can follow any trade back to the raw sources that motivated it.

---

## 6. Quality Standards

- **Strongly typed, properly formatted code.** Types are documentation; enforce them.
- **No unit tests until go-live.** Focus on integration-level validation and proving features work end-to-end via the UI.
- **Git hygiene.** Clean commits, meaningful PRs, descriptive branch names.
- **Prefer free and open tools and services.** Minimize vendor lock-in and licensing costs.
- **Every feature must be provably working via the UI.** If it cannot be demonstrated, it is not done.

---

## 7. Transparency Principles

1. All system internals are visible on the frontend.
2. Changelog and release notes are published automatically.
3. Every fact is traceable to its source.
4. Every inference is traceable to its method.
5. Every trade is traceable to its rationale.

---

*This is a living document. Update it as the architecture evolves.*
