---
event: prompt
action: warn
conditions:
  - field: prompt
    operator: regex_match
    pattern: "bug|fix|broken|crash|error|regression|issue|defect|patch|hotfix|not working|fails|failing"
---

## Bug Fix Documentation Reminder

When fixing a bug, update `.llm-context/topics/bugs-fixed.md` with an entry:

```markdown
### BUG-XXX: Brief description
- **Symptom**: What the user experienced
- **Cause**: Root cause of the issue
- **Fix**: What was changed to resolve it
- **File**: path/to/file.ext:line
```

Run `./ctx bugs-fixed` to see existing entries and determine the next BUG number.
