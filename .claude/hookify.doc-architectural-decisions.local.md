---
event: file
action: warn
conditions:
  - field: file_path
    operator: regex_match
    pattern: "CLAUDE\\.md|templates/CLAUDE\\.md\\.template|templates/settings\\.json\\.template|\\.claude/settings\\.json"
---

## Architectural Decisions Documentation Reminder

You're editing structural or convention files. Consider updating `.llm-context/topics/architectural-decisions.md`:

Add an ADR-style entry:
```markdown
### ADR-XXX: Decision title
- **Status**: Accepted/Superseded/Deprecated
- **Context**: Why this decision was needed
- **Decision**: What was decided
- **Consequences**: Trade-offs and implications
```

Run `./ctx architectural-decisions` to see existing ADRs.
