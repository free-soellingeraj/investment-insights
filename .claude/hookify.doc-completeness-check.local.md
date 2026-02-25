---
event: stop
action: warn
conditions:
  - field: transcript
    operator: not_contains
    pattern: ".llm-context/topics/"
---

## Documentation Completeness Check

No topic documentation was updated during this session. Before ending, consider:

1. **List available topics**: `./ctx`
2. **Review which topics apply** to the work done

### When to Update Documentation

| Work Type | Update These Topics |
|-----------|---------------------|
| Bug fix | `bugs-fixed` (add entry with cause/fix/file) |
| Architecture decision | `architectural-decisions` (add ADR-style entry) |
| New feature | `product-features` (document feature and usage) |
| Tech stack change | `tech-stack` (update dependencies/tools) |
| New logging/debugging | `debugging` (add debugging tips) |
| CI/CD change | `cicd` (document pipeline changes) |
| Data model change | `data-model` (update schema docs) |

*If this was an exploratory session with no implementation changes, you can safely disregard this reminder.*
