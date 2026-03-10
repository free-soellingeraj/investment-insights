"""GitHub PR helper — creates branches and PRs via gh CLI without touching working directory.

Uses the GitHub API (via gh) to create branches and commit files remotely,
then opens PRs. This avoids interfering with the user's local checkout.
"""

import base64
import json
import logging
import re
import subprocess

logger = logging.getLogger(__name__)

OWNER_REPO = "free-soellingeraj/investment-insights"
BASE_BRANCH = "main"


def _run_gh(args: list[str], check: bool = True) -> subprocess.CompletedProcess:
    """Run a gh CLI command."""
    result = subprocess.run(
        ["gh"] + args,
        capture_output=True,
        text=True,
        timeout=30,
    )
    if check and result.returncode != 0:
        logger.error("gh command failed: %s\nstderr: %s", " ".join(args[:4]), result.stderr[:200])
    return result


def _slugify(text: str, max_len: int = 40) -> str:
    """Convert text to a branch-safe slug."""
    slug = re.sub(r"[^a-z0-9]+", "-", text.lower().strip())
    return slug.strip("-")[:max_len].rstrip("-")


def get_head_sha() -> str:
    """Get the current HEAD SHA."""
    result = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        capture_output=True, text=True, timeout=10,
    )
    return result.stdout.strip()


def create_branch(branch_name: str) -> bool:
    """Create a branch on the remote pointing to HEAD."""
    sha = get_head_sha()
    result = _run_gh([
        "api", f"repos/{OWNER_REPO}/git/refs",
        "-X", "POST",
        "-f", f"ref=refs/heads/{branch_name}",
        "-f", f"sha={sha}",
    ], check=False)
    if result.returncode != 0:
        # Branch may already exist
        if "Reference already exists" in result.stderr:
            logger.info("Branch %s already exists", branch_name)
            return True
        return False
    return True


def commit_file(branch_name: str, file_path: str, content: str, message: str) -> bool:
    """Commit a file to a branch via the GitHub Contents API."""
    content_b64 = base64.b64encode(content.encode("utf-8")).decode("ascii")

    # Check if file already exists on the branch (need sha for update)
    check = _run_gh([
        "api", f"repos/{OWNER_REPO}/contents/{file_path}",
        "-H", "Accept: application/vnd.github.v3+json",
        "--jq", ".sha",
        "-f", f"ref={branch_name}",
    ], check=False)

    args = [
        "api", f"repos/{OWNER_REPO}/contents/{file_path}",
        "-X", "PUT",
        "-f", f"message={message}",
        "-f", f"content={content_b64}",
        "-f", f"branch={branch_name}",
    ]

    if check.returncode == 0 and check.stdout.strip():
        # File exists, include sha for update
        args.extend(["-f", f"sha={check.stdout.strip()}"])

    result = _run_gh(args, check=False)
    return result.returncode == 0


def create_pr(
    branch_name: str,
    title: str,
    body: str,
    draft: bool = True,
    labels: list[str] | None = None,
) -> tuple[int | None, str | None]:
    """Create a PR and return (pr_number, pr_url) or (None, None) on failure."""
    args = [
        "pr", "create",
        "--base", BASE_BRANCH,
        "--head", branch_name,
        "--title", title,
        "--body", body[:65000],
    ]
    if draft:
        args.append("--draft")
    if labels:
        for label in labels:
            args.extend(["--label", label])

    result = _run_gh(args, check=False)
    if result.returncode != 0:
        logger.error("PR creation failed: %s", result.stderr[:200])
        return None, None

    pr_url = result.stdout.strip()
    try:
        pr_number = int(pr_url.rstrip("/").split("/")[-1])
    except (ValueError, IndexError):
        return None, pr_url

    return pr_number, pr_url


def check_pr_status(pr_number: int) -> dict:
    """Check PR status. Returns {state, merged, mergeable}."""
    result = _run_gh([
        "pr", "view", str(pr_number),
        "--json", "state,mergedAt,mergeable,isDraft,title",
    ], check=False)
    if result.returncode != 0:
        return {"state": "unknown", "merged": False}
    try:
        data = json.loads(result.stdout)
        # Normalize: gh uses mergedAt (not merged), so derive a boolean
        data["merged"] = data.get("state") == "MERGED" or bool(data.get("mergedAt"))
        return data
    except json.JSONDecodeError:
        return {"state": "unknown", "merged": False}


def add_pr_comment(pr_number: int, body: str) -> bool:
    """Add a comment to a PR."""
    result = _run_gh([
        "pr", "comment", str(pr_number),
        "--body", body[:65000],
    ], check=False)
    return result.returncode == 0


def merge_pr(pr_number: int, method: str = "merge") -> bool:
    """Merge a PR. Method can be 'merge', 'squash', or 'rebase'.

    If the PR is a draft, it is first marked as ready for review.
    Returns True on success.
    """
    # Check if draft — if so, mark ready first
    status = check_pr_status(pr_number)
    if status.get("isDraft"):
        ready_result = _run_gh(["pr", "ready", str(pr_number)], check=False)
        if ready_result.returncode != 0:
            logger.error("Failed to mark PR #%d ready: %s", pr_number, ready_result.stderr[:200])
            return False

    result = _run_gh([
        "pr", "merge", str(pr_number),
        f"--{method}",
        "--admin",
    ], check=False)
    if result.returncode != 0:
        logger.error("PR merge failed for #%d: %s", pr_number, result.stderr[:200])
        return False
    logger.info("Merged PR #%d via %s", pr_number, method)
    return True


def create_plan_pr(
    team_name: str,
    plan_id: int,
    title: str,
    plan_text: str,
) -> tuple[int | None, str | None, str]:
    """Create a full plan PR: branch + file + PR.

    Returns (pr_number, pr_url, branch_name).
    """
    slug = _slugify(title)
    branch_name = f"plan/{team_name}/{plan_id}-{slug}"
    file_path = f".agent-comms/plans/{team_name}-plan-{plan_id}.md"

    if not create_branch(branch_name):
        logger.error("Failed to create branch %s", branch_name)
        return None, None, branch_name

    if not commit_file(branch_name, file_path, plan_text, f"Plan: {title}"):
        logger.error("Failed to commit plan file to %s", branch_name)
        return None, None, branch_name

    pr_body = f"""## Plan: {title}

**Team:** {team_name}
**Plan ID:** {plan_id}

---

{plan_text}

---

*Review this plan and merge to approve, or close to reject.*
*Created by the {team_name} idea guy agent.*
"""

    pr_number, pr_url = create_pr(
        branch_name, title, pr_body, draft=True,
    )

    return pr_number, pr_url, branch_name


def create_implementation_pr(
    team_name: str,
    project_id: int,
    plan_id: int,
    title: str,
    code_impact: str,
    test_instructions: str,
    plan_text: str,
) -> tuple[int | None, str | None, str]:
    """Create an implementation PR: branch + spec file + PR.

    Returns (pr_number, pr_url, branch_name).
    """
    slug = _slugify(title)
    branch_name = f"impl/{team_name}/{project_id}-{slug}"
    file_path = f".agent-comms/implementations/{team_name}-impl-{project_id}.md"

    impl_spec = f"""# Implementation: {title}

## Plan Reference
Plan #{plan_id}

## Code Impact
{code_impact}

## Implementation Steps
{plan_text}

## Test Instructions
{test_instructions}

## Verification Checklist
- [ ] All action items from the plan are addressed
- [ ] No regressions in existing tests
- [ ] Code impact matches what was described
- [ ] Human has tested per the test instructions above
"""

    if not create_branch(branch_name):
        logger.error("Failed to create branch %s", branch_name)
        return None, None, branch_name

    if not commit_file(branch_name, file_path, impl_spec, f"Implementation: {title}"):
        logger.error("Failed to commit implementation spec to %s", branch_name)
        return None, None, branch_name

    pr_body = f"""## Implementation: {title}

**Team:** {team_name}
**Project ID:** {project_id}
**Plan ID:** {plan_id}

### Code Impact
{code_impact}

### Test Instructions
{test_instructions}

---

*Review the implementation spec and verify the changes.*
*Merge to complete, or request changes if testing fails.*
"""

    pr_number, pr_url = create_pr(
        branch_name, f"[impl] {title}", pr_body, draft=False,
    )

    return pr_number, pr_url, branch_name
