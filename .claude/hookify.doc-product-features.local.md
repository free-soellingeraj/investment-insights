---
event: file
action: warn
conditions:
  - field: file_path
    operator: regex_match
    pattern: "^(llm-context|templates/)"
---

## Product Features Documentation Reminder

You're editing core source files. Consider updating `.llm-context/topics/product-features.md`:

- Document new features and their usage
- Update command-line interface documentation
- Note behavior changes to existing features
- Add examples for new functionality

Run `./ctx product-features` to see current documentation.
