---
event: file
action: warn
conditions:
  - field: file_path
    operator: regex_match
    pattern: "\\.env\\.example|logging|logger|debug|\\.vscode/launch\\.json|jest\\.config|vitest\\.config|pytest\\.ini|playwright\\.config"
---

## Debugging Documentation Reminder

You're editing debugging or test configuration files. Consider updating `.llm-context/topics/debugging.md`:

- Document new debugging techniques or tools
- Add troubleshooting tips for common issues
- Update test configuration documentation
- Note logging conventions or new log formats

Run `./ctx debugging` to see current documentation.
