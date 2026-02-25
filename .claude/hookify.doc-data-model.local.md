---
event: file
action: warn
conditions:
  - field: file_path
    operator: regex_match
    pattern: "migrations?/|schema|\\.sql$|models?/|types?\\.ts|\\.prisma$|\\.graphql$|\\.proto$|openapi|swagger"
---

## Data Model Documentation Reminder

You're editing data model or schema files. Consider updating `.llm-context/topics/data-model.md`:

- Document new entities, tables, or types
- Note field additions, removals, or type changes
- Update relationship documentation
- Document migration procedures if applicable

Run `./ctx data-model` to see current documentation.
