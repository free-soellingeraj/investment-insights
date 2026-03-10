"""Engineer agent — claims approved plans, spawns Claude Code to implement changes."""

import asyncio
import json
import logging
import os
import re
import shutil
import subprocess

from sqlalchemy import text

from ai_opportunity_index.storage.db import get_session
from .base import BaseAgent
from .pr_helper import create_pr, create_branch, merge_pr

logger = logging.getLogger(__name__)

CLAUDE_BIN = shutil.which("claude") or "claude"
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
WORKTREE_BASE = "/tmp/agent-worktrees"
IMPL_TIMEOUT = 600  # 10 minutes max for Claude Code
SOURCE_BRANCH = "rime-related-alerts"  # Branch with all actual code


def _slugify(text: str, max_len: int = 40) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", text.lower().strip())
    return slug.strip("-")[:max_len].rstrip("-")


def _extract_section(plan_text: str, heading: str) -> str:
    pattern = rf"## {re.escape(heading)}\s*\n(.*?)(?=\n## |\Z)"
    match = re.search(pattern, plan_text, re.DOTALL)
    return match.group(1).strip() if match else ""


class Engineer(BaseAgent):
    def __init__(self, team_name: str, agent_name: str):
        super().__init__(agent_name=agent_name, role="engineer", team_name=team_name)
        self._active_project_id: int | None = None
        self._impl_task: asyncio.Task | None = None

    async def run_cycle(self, cycle_num: int):
        channel = f"#{self.team_name}"

        # If Claude Code is running, check status
        if self._impl_task is not None:
            if self._impl_task.done():
                try:
                    self._impl_task.result()
                except Exception as e:
                    logger.error("[%s] Implementation task failed: %s", self.agent_name, e)
                    self.post_message(
                        channel,
                        f"[Engineering] Implementation failed with error: {str(e)[:200]}. "
                        f"Marking project #{self._active_project_id} as failed.",
                        msg_type="chat",
                    )
                    self._mark_project_failed(self._active_project_id, str(e))
                self._impl_task = None
                self._active_project_id = None
            else:
                # Still running — post progress update every 20 cycles
                if cycle_num % 20 == 0:
                    self.post_message(
                        channel,
                        f"[Engineering] Claude Code still working on project "
                        f"#{self._active_project_id}...",
                        msg_type="chat",
                    )
            return

        # Read messages for triggers
        messages = self.read_others_messages(channel)
        for msg in messages:
            content = msg["content"]
            sender_role = msg.get("sender_role", "")

            # Respond to plan drafts
            if sender_role == "idea_guy" and "Plan" in content and "PR:" in content:
                self.post_message(
                    channel,
                    f"@{msg['sender_name']} I see the plan PR. "
                    f"Ready to implement once it's merged/approved.",
                    msg_type="chat",
                )

            # React to plan approval
            if (msg.get("message_type") == "system" and "APPROVED" in content) or (
                sender_role == "idea_guy"
                and "approved" in content.lower()
                and "pick it up" in content.lower()
            ):
                if not self._active_project_id:
                    self._claim_and_implement(channel)

            # Directly addressed
            if self.agent_name in content or (
                sender_role == "idea_guy" and "engineer" in content.lower()
            ):
                if not self._active_project_id:
                    self._claim_and_implement(channel)

    def _claim_and_implement(self, channel: str):
        """Claim an approved plan and spawn Claude Code to implement it."""
        session = get_session()
        try:
            row = session.execute(
                text("""
                    SELECT p.id, p.title, p.plan_text
                    FROM agent_plans p
                    JOIN agent_teams t ON t.id = p.team_id
                    WHERE t.name = :team AND p.status = 'approved'
                    ORDER BY p.created_at ASC LIMIT 1
                """),
                {"team": self.team_name},
            ).fetchone()

            if not row:
                return

            plan_id, title, plan_text = row[0], row[1], row[2] or ""

            # Claim plan
            session.execute(
                text("UPDATE agent_plans SET status = 'implementing' WHERE id = :id AND status = 'approved'"),
                {"id": plan_id},
            )

            # Create project
            result = session.execute(
                text("""
                    INSERT INTO agent_projects (plan_id, team_id, title, status, assigned_to, created_at)
                    SELECT :plan_id, t.id, :title, 'implementing', :agent_id, NOW()
                    FROM agent_teams t WHERE t.name = :team
                    RETURNING id
                """),
                {"plan_id": plan_id, "title": title, "agent_id": self.agent_id, "team": self.team_name},
            )
            project_row = result.fetchone()
            session.commit()

            if not project_row:
                return

            project_id = project_row[0]
            self._active_project_id = project_id

            # Extract context from plan
            code_impact = _extract_section(plan_text, "Files Likely Affected")
            actions = _extract_section(plan_text, "Proposed Actions")
            success_criteria = _extract_section(plan_text, "Success Criteria")

            self.post_message(
                channel,
                f'[Engineering] Claimed plan "{title}" -> project #{project_id}. '
                f"Spawning Claude Code to implement. This may take several minutes.",
                msg_type="chat",
            )

            # Launch async implementation task
            self._impl_task = asyncio.create_task(
                self._run_claude_implementation(
                    channel, project_id, plan_id, title, plan_text,
                    code_impact, actions, success_criteria,
                )
            )
            logger.info("[%s] Spawning Claude Code for project %d", self.agent_name, project_id)

        except Exception as e:
            logger.error("[%s] Plan claim error: %s", self.agent_name, e)
            session.rollback()
        finally:
            session.close()

    async def _run_claude_implementation(
        self, channel: str, project_id: int, plan_id: int,
        title: str, plan_text: str,
        code_impact: str, actions: str, success_criteria: str,
    ):
        """Spawn Claude Code in a worktree, wait for completion, push + create PR."""
        slug = _slugify(title)
        branch = f"impl/{self.team_name}/{project_id}-{slug}"
        worktree_path = os.path.join(WORKTREE_BASE, f"{self.team_name}-{project_id}")

        try:
            # Clean up any leftover worktree
            if os.path.exists(worktree_path):
                subprocess.run(
                    ["git", "worktree", "remove", worktree_path, "--force"],
                    capture_output=True, cwd=REPO_ROOT,
                )

            os.makedirs(WORKTREE_BASE, exist_ok=True)

            # Create worktree from main
            wt_result = subprocess.run(
                ["git", "worktree", "add", "-b", branch, worktree_path, SOURCE_BRANCH],
                capture_output=True, text=True, cwd=REPO_ROOT,
            )
            if wt_result.returncode != 0:
                # Branch might exist already, try checkout
                subprocess.run(
                    ["git", "worktree", "add", worktree_path, branch],
                    capture_output=True, text=True, cwd=REPO_ROOT,
                )

            # Build the prompt for Claude Code
            prompt = self._build_prompt(title, plan_text, code_impact, actions, success_criteria)

            # Spawn Claude Code — strip CLAUDECODE env var to avoid nested session error
            logger.info("[%s] Spawning claude -p in %s", self.agent_name, worktree_path)
            spawn_env = {k: v for k, v in os.environ.items() if k != "CLAUDECODE"}
            spawn_env["CLAUDE_MODEL"] = "claude-sonnet-4-6"
            proc = await asyncio.create_subprocess_exec(
                CLAUDE_BIN, "-p", prompt,
                "--dangerously-skip-permissions",
                cwd=worktree_path,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                env=spawn_env,
            )

            try:
                stdout, stderr = await asyncio.wait_for(
                    proc.communicate(), timeout=IMPL_TIMEOUT,
                )
            except asyncio.TimeoutError:
                proc.kill()
                await proc.wait()
                raise RuntimeError(f"Claude Code timed out after {IMPL_TIMEOUT}s")

            claude_output = stdout.decode("utf-8", errors="replace")[:5000]
            returncode = proc.returncode

            if returncode != 0:
                stderr_text = stderr.decode("utf-8", errors="replace")[:1000]
                logger.warning(
                    "[%s] Claude Code exited %d: %s", self.agent_name, returncode, stderr_text,
                )

            # Check what files were changed
            diff_result = subprocess.run(
                ["git", "diff", "--name-only", SOURCE_BRANCH],
                capture_output=True, text=True, cwd=worktree_path,
            )
            files_changed = [f.strip() for f in diff_result.stdout.strip().split("\n") if f.strip()]

            # Also check for uncommitted changes and commit them
            status_result = subprocess.run(
                ["git", "status", "--porcelain"],
                capture_output=True, text=True, cwd=worktree_path,
            )
            if status_result.stdout.strip():
                # There are uncommitted changes — stage and commit
                subprocess.run(["git", "add", "-A"], capture_output=True, cwd=worktree_path)
                subprocess.run(
                    ["git", "commit", "-m", f"impl: {title}\n\nAutomated implementation by {self.agent_name}"],
                    capture_output=True, cwd=worktree_path,
                )
                # Re-check files changed
                diff_result = subprocess.run(
                    ["git", "diff", "--name-only", SOURCE_BRANCH],
                    capture_output=True, text=True, cwd=worktree_path,
                )
                files_changed = [f.strip() for f in diff_result.stdout.strip().split("\n") if f.strip()]

            if not files_changed:
                self.post_message(
                    channel,
                    f"[Engineering] Claude Code finished for project #{project_id} but made no changes. "
                    f"The plan may need to be more specific. Output: {claude_output[:300]}",
                    msg_type="chat",
                )
                self._mark_project_failed(project_id, "No files changed")
                return

            # Push the branch
            push_result = subprocess.run(
                ["git", "push", "-u", "origin", branch],
                capture_output=True, text=True, cwd=worktree_path,
            )
            if push_result.returncode != 0:
                raise RuntimeError(f"git push failed: {push_result.stderr[:200]}")

            # Build PR body
            test_instructions = success_criteria or "Verify all action items from the plan."
            pr_body = (
                f"## Implementation: {title}\n\n"
                f"**Team:** {self.team_name}\n"
                f"**Project ID:** {project_id}\n"
                f"**Plan ID:** {plan_id}\n\n"
                f"### Files Changed ({len(files_changed)})\n"
                + "\n".join(f"- `{f}`" for f in files_changed[:30])
                + f"\n\n### Claude Code Output\n```\n{claude_output[:2000]}\n```\n\n"
                f"### Test Instructions\n{test_instructions}\n\n"
                f"---\n*Implemented by {self.agent_name} using Claude Code.*"
            )

            # Create PR (not draft — ready for review)
            pr_number, pr_url = create_pr(
                branch, f"[impl] {title}", pr_body, draft=False,
            )

            # Update project in DB
            session = get_session()
            try:
                update_fields = {
                    "id": project_id,
                    "code_impact": "\n".join(f"- `{f}`" for f in files_changed),
                    "test_instructions": test_instructions,
                    "files_changed": json.dumps(files_changed),
                    "status": "code_review",
                }
                update_sql = """
                    UPDATE agent_projects
                    SET code_impact = :code_impact,
                        test_instructions = :test_instructions,
                        files_changed = CAST(:files_changed AS jsonb),
                        status = :status
                """
                if pr_number:
                    update_sql += ", pr_number = :pr_number, pr_url = :pr_url, pr_branch = :pr_branch"
                    update_fields.update({
                        "pr_number": pr_number,
                        "pr_url": pr_url,
                        "pr_branch": branch,
                    })
                update_sql += " WHERE id = :id"
                session.execute(text(update_sql), update_fields)
                session.commit()
            finally:
                session.close()

            pr_info = f" PR: {pr_url}" if pr_url else ""
            self.post_message(
                channel,
                f'[Engineering] Claude Code finished project #{project_id} "{title}".{pr_info} '
                f"Changed {len(files_changed)} files. Ready for code review. "
                f"@{self.team_name}-code_reviewer please review.",
                msg_type="chat",
            )
            logger.info(
                "[%s] Implementation complete: project %d, %d files, PR %s",
                self.agent_name, project_id, len(files_changed), pr_url,
            )

        except Exception as e:
            logger.error("[%s] Implementation error: %s", self.agent_name, e)
            self.post_message(
                channel,
                f"[Engineering] Implementation error for project #{project_id}: {str(e)[:300]}",
                msg_type="chat",
            )
            self._mark_project_failed(project_id, str(e))
            raise
        finally:
            # Clean up worktree
            try:
                subprocess.run(
                    ["git", "worktree", "remove", worktree_path, "--force"],
                    capture_output=True, cwd=REPO_ROOT,
                )
            except Exception:
                pass

    def _build_prompt(
        self, title: str, plan_text: str,
        code_impact: str, actions: str, success_criteria: str,
    ) -> str:
        return f"""You are a software engineer implementing a plan for the {self.team_name} team.
This is a real production codebase — an AI-driven investment insights platform with:
- Backend: Litestar + SQLAlchemy + PostgreSQL
- Frontend: Next.js + Tailwind
- Scoring pipeline: evidence extraction, valuation, composite scoring
- Python 3.11

## Plan: {title}

{plan_text}

## Your Task

Implement the changes described in "Proposed Actions" above. Follow these rules:

1. Read the files listed under "Files Likely Affected" first to understand the current code
2. Make minimal, targeted changes — only modify what the plan requires
3. Do NOT add unnecessary comments, docstrings, or type annotations
4. Do NOT create new files unless the plan explicitly requires it
5. Run the test suite after making changes: /opt/homebrew/bin/python3.11 -m pytest tests/ -x -q
6. Fix any test failures your changes cause
7. Commit your changes when done with: git add -A && git commit -m "impl: {title}"

If a proposed action is too vague or would require changes outside your scope, skip it.
Focus on correctness and minimal diff size."""

    def _mark_project_failed(self, project_id: int, reason: str):
        session = get_session()
        try:
            session.execute(
                text("""
                    UPDATE agent_projects
                    SET status = 'failed', test_results = CAST(:results AS jsonb)
                    WHERE id = :id
                """),
                {"id": project_id, "results": json.dumps({"error": reason[:500]})},
            )
            # Also reset the plan so it can be retried
            session.execute(
                text("""
                    UPDATE agent_plans SET status = 'approved', updated_at = NOW()
                    WHERE id = (SELECT plan_id FROM agent_projects WHERE id = :id)
                """),
                {"id": project_id},
            )
            session.commit()
        finally:
            session.close()
