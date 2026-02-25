"""Template loader for prompt files.

All LLM agents use load_prompt() instead of inline strings. This makes prompts
editable without code changes, reviewable in version control, and reusable
across extractors and estimators.
"""

from __future__ import annotations

from pathlib import Path

from jinja2 import Template

PROMPTS_DIR = Path(__file__).parent


def load_prompt(name: str, **kwargs) -> str:
    """Load and render a prompt template.

    Args:
        name: Template name without extension (e.g. 'extract_filing_evidence').
        **kwargs: Variables to render into the template.

    Returns:
        Rendered prompt string.
    """
    template_path = PROMPTS_DIR / f"{name}.md"
    if not template_path.exists():
        raise FileNotFoundError(f"Prompt template not found: {template_path}")
    template = Template(template_path.read_text())
    return template.render(**kwargs)
