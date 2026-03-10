"""Changelog entry management — tracks system releases and feature changes."""

from datetime import datetime
from enum import Enum
from pathlib import Path
from pydantic import BaseModel, Field
import json


class ChangeType(str, Enum):
    FEATURE = "feature"          # New capability
    IMPROVEMENT = "improvement"  # Enhancement to existing
    FIX = "fix"                 # Bug fix
    ARCHITECTURE = "architecture"  # Structural change
    DATA = "data"               # Data model/pipeline change
    INFRASTRUCTURE = "infrastructure"  # CI/CD, deployment


class ChangeEntry(BaseModel):
    """A single change within a release."""
    description: str
    change_type: ChangeType
    component: str  # e.g. "fact-graph", "web-ui", "pipeline", "scoring"
    files_changed: list[str] = Field(default_factory=list)
    pr_number: int | None = None
    commit_sha: str | None = None


class Release(BaseModel):
    """A versioned release with one or more changes."""
    version: str  # semver: 0.1.0, 0.2.0, etc.
    title: str
    date: datetime
    summary: str
    changes: list[ChangeEntry] = Field(default_factory=list)
    status: str = "released"  # "released", "in-progress", "planned"


CHANGELOG_PATH = Path(__file__).resolve().parent.parent / "data" / "changelog.json"


def load_changelog() -> list[Release]:
    if not CHANGELOG_PATH.exists():
        return []
    data = json.loads(CHANGELOG_PATH.read_text())
    return [Release(**r) for r in data]


def save_changelog(releases: list[Release]):
    CHANGELOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    CHANGELOG_PATH.write_text(
        json.dumps([r.model_dump(mode="json") for r in releases], indent=2, default=str)
    )


def add_release(release: Release):
    releases = load_changelog()
    releases.insert(0, release)
    save_changelog(releases)
