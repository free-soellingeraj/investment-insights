---
event: file
action: warn
conditions:
  - field: file_path
    operator: regex_match
    pattern: "\\.github/workflows/|Jenkinsfile|Makefile|Dockerfile|docker-compose|\\.gitlab-ci|deploy|templates/pre-commit"
---

## CI/CD Documentation Reminder

You're editing CI/CD-related files. Consider updating `.llm-context/topics/cicd.md`:

- Document new workflows, jobs, or pipeline stages
- Update deployment procedures
- Note any new environment variables or secrets required
- Document build/test command changes

Run `./ctx cicd` to see current documentation.
