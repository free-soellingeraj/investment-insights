---
event: prompt
action: warn
conditions:
  - field: prompt
    operator: regex_match
    pattern: "implement|add|create|build|refactor|change|update|modify|remove|delete|migrate|upgrade|debug|troubleshoot|feature|endpoint|fix|bug"
---

## Documentation Context Reminder

Before starting implementation, load relevant documentation context:

1. **List available topics**: `./ctx`
2. **Load relevant topics**: `./ctx <topic>` for each topic that relates to your work

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
