---
event: file
action: warn
conditions:
  - field: file_path
    operator: regex_match
    pattern: "package\\.json|requirements\\.txt|Cargo\\.toml|pyproject\\.toml|go\\.mod|\\.tool-versions|tsconfig\\.json|webpack\\.config|vite\\.config"
---

## Tech Stack Documentation Reminder

You're editing dependency or configuration files. Consider updating `.llm-context/topics/tech-stack.md`:

- Document new dependencies and their purpose
- Note version upgrades and any breaking changes
- Update tooling requirements
- Document build configuration changes

Run `./ctx tech-stack` to see current documentation.
