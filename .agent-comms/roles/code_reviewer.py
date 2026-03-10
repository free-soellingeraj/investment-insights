"""Code Reviewer agent — spawns Claude Code to review implementation PRs."""

import asyncio
import logging
import os
import shutil
import subprocess

from sqlalchemy import text

from ai_opportunity_index.storage.db import get_session
from .base import BaseAgent
from .pr_helper import add_pr_comment, check_pr_status

logger = logging.getLogger(__name__)

CLAUDE_BIN = shutil.which("claude") or "claude"
REVIEW_TIMEOUT = 300  # 5 minutes max for review


class CodeReviewer(BaseAgent):
    def __init__(self, team_name: str, agent_name: str):
        super().__init__(agent_name=agent_name, role="code_reviewer", team_name=team_name)
        self._reviewing_project_id: int | None = None
        self._review_task: asyncio.Task | None = None

    async def run_cycle(self, cycle_num: int):
        channel = f"#{self.team_name}"

        # If Claude Code review is running, check status
        if self._review_task is not None:
            if self._review_task.done():
                try:
                    self._review_task.result()
                except Exception as e:
                    logger.error("[%s] Review task failed: %s", self.agent_name, e)
                    self.post_message(
                        channel,
                        f"[Code Review] Review failed for project "
                        f"#{self._reviewing_project_id}: {str(e)[:200]}",
                        msg_type="chat",
                    )
                self._review_task = None
                self._reviewing_project_id = None
            else:
                if cycle_num % 15 == 0:
                    self.post_message(
                        channel,
                        f"[Code Review] Still reviewing project "
                        f"#{self._reviewing_project_id}...",
                        msg_type="chat",
                    )
            return

        # Read messages for triggers
        messages = self.read_others_messages(channel)
        for msg in messages:
            content = msg["content"]
            sender_role = msg.get("sender_role", "")

            if sender_role == "engineer" and (
                "code review" in content.lower() or "code_reviewer" in content
            ):
                if not self._reviewing_project_id:
                    self._claim_and_review(channel)

            if self.agent_name in content:
                if not self._reviewing_project_id:
                    self._claim_and_review(channel)

    def _claim_and_review(self, channel: str):
        """Claim a project for review and spawn Claude Code."""
        session = get_session()
        try:
            row = session.execute(
                text("""
                    SELECT p.id, p.title, p.plan_id, p.pr_number, p.pr_branch,
                           pl.plan_text
                    FROM agent_projects p
                    JOIN agent_plans pl ON pl.id = p.plan_id
                    JOIN agent_teams t ON t.id = p.team_id
                    WHERE t.name = :team AND p.status = 'code_review'
                    AND p.reviewer_id IS NULL
                    ORDER BY p.created_at ASC LIMIT 1
                """),
                {"team": self.team_name},
            ).fetchone()

            if not row:
                return

            project_id = row[0]
            title = row[1]
            plan_id = row[2]
            pr_number = row[3]
            pr_branch = row[4]
            plan_text = row[5] or ""

            # Claim
            session.execute(
                text("UPDATE agent_projects SET reviewer_id = :reviewer WHERE id = :id"),
                {"reviewer": self.agent_id, "id": project_id},
            )
            session.commit()

            self._reviewing_project_id = project_id
            self.post_message(
                channel,
                f'[Code Review] Starting Claude Code review of project #{project_id} "{title}".'
                + (f" PR #{pr_number}." if pr_number else ""),
                msg_type="chat",
            )

            # Launch async review
            self._review_task = asyncio.create_task(
                self._run_claude_review(
                    channel, project_id, title, plan_id,
                    plan_text, pr_number, pr_branch,
                )
            )
            logger.info("[%s] Spawning Claude Code review for project %d", self.agent_name, project_id)

        except Exception as e:
            logger.error("[%s] Review claim error: %s", self.agent_name, e)
            session.rollback()
        finally:
            session.close()

    async def _run_claude_review(
        self, channel: str, project_id: int, title: str,
        plan_id: int, plan_text: str,
        pr_number: int | None, pr_branch: str | None,
    ):
        """Get the PR diff, spawn Claude Code to review it, post results."""
        try:
            # Get the diff
            diff = ""
            if pr_number:
                diff_result = subprocess.run(
                    ["gh", "pr", "diff", str(pr_number)],
                    capture_output=True, text=True, timeout=30,
                )
                diff = diff_result.stdout[:50000]
            elif pr_branch:
                diff_result = subprocess.run(
                    ["git", "diff", f"rime-related-alerts...{pr_branch}"],
                    capture_output=True, text=True, timeout=30,
                )
                diff = diff_result.stdout[:50000]

            if not diff.strip():
                self.post_message(
                    channel,
                    f"[Code Review] No diff found for project #{project_id}. "
                    f"Cannot review without changes.",
                    msg_type="chat",
                )
                return

            # Build review prompt
            prompt = self._build_review_prompt(title, plan_text, diff)

            # Spawn Claude Code — strip CLAUDECODE env var to avoid nested session error
            spawn_env = {k: v for k, v in os.environ.items() if k != "CLAUDECODE"}
            spawn_env["CLAUDE_MODEL"] = "claude-sonnet-4-6"
            proc = await asyncio.create_subprocess_exec(
                CLAUDE_BIN, "-p", prompt,
                "--dangerously-skip-permissions",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                env=spawn_env,
            )

            try:
                stdout, stderr = await asyncio.wait_for(
                    proc.communicate(), timeout=REVIEW_TIMEOUT,
                )
            except asyncio.TimeoutError:
                proc.kill()
                await proc.wait()
                raise RuntimeError(f"Claude Code review timed out after {REVIEW_TIMEOUT}s")

            review_output = stdout.decode("utf-8", errors="replace")[:8000]

            # Parse verdict from Claude's output
            verdict = "APPROVE"  # default optimistic
            output_lower = review_output.lower()
            if "request_changes" in output_lower or "request changes" in output_lower:
                verdict = "REQUEST_CHANGES"
            elif "reject" in output_lower:
                verdict = "REQUEST_CHANGES"

            # Post review to GitHub PR
            if pr_number:
                review_comment = (
                    f"## Code Review by {self.agent_name}\n\n"
                    f"**Verdict: {verdict}**\n\n"
                    f"{review_output}\n\n"
                    f"---\n*Automated review by {self.agent_name} using Claude Code.*"
                )
                add_pr_comment(pr_number, review_comment)

            # Update project status
            session = get_session()
            try:
                if verdict == "APPROVE":
                    session.execute(
                        text("""
                            UPDATE agent_projects
                            SET status = 'verified', completed_at = NOW()
                            WHERE id = :id
                        """),
                        {"id": project_id},
                    )
                    session.execute(
                        text("UPDATE agent_plans SET status = 'verified', updated_at = NOW() WHERE id = :plan_id"),
                        {"plan_id": plan_id},
                    )

                    # Store review summary as a plan comment
                    session.execute(
                        text("""
                            INSERT INTO agent_plan_comments
                                (plan_id, line_number, author_name, content, created_at)
                            VALUES (:plan_id, NULL, :author, :content, NOW())
                        """),
                        {
                            "plan_id": plan_id,
                            "author": self.agent_name,
                            "content": f"Code review APPROVED for project #{project_id}.\n\n"
                                       f"{review_output[:2000]}",
                        },
                    )
                else:
                    # Request changes — move back to implementing so engineer can fix
                    session.execute(
                        text("UPDATE agent_projects SET status = 'implementing' WHERE id = :id"),
                        {"id": project_id},
                    )

                session.commit()
            finally:
                session.close()

            pr_info = f" (PR #{pr_number})" if pr_number else ""
            self.post_message(
                channel,
                f'[Code Review] Project #{project_id} "{title}"{pr_info} — '
                f"**{verdict}**. {review_output[:300]}",
                msg_type="chat",
            )
            if verdict == "APPROVE":
                self.increment_fix()

            logger.info(
                "[%s] Review complete: project %d, verdict=%s",
                self.agent_name, project_id, verdict,
            )

        except Exception as e:
            logger.error("[%s] Review error: %s", self.agent_name, e)
            self.post_message(
                channel,
                f"[Code Review] Error reviewing project #{project_id}: {str(e)[:300]}",
                msg_type="chat",
            )
            raise

    def _build_review_prompt(self, title: str, plan_text: str, diff: str) -> str:
        return f"""You are a senior code reviewer. Review this implementation PR against its plan.

## Plan: {title}

{plan_text}

## PR Diff

```diff
{diff}
```

## Your Task

Review the implementation against the plan requirements:

1. Does the diff implement the action items from "Proposed Actions"?
2. Are there bugs, regressions, or security issues (SQL injection, XSS, etc.)?
3. Is the code quality acceptable? No unnecessary changes, clean implementation?
4. Do the changes stay within scope of the plan?

Respond with a structured review in this exact format:

VERDICT: APPROVE or REQUEST_CHANGES

SUMMARY: 1-2 sentence summary

ISSUES: (if any)
- bullet list of specific issues with file paths and line numbers

STRENGTHS: (if any)
- bullet list of things done well

Be concise. Focus on correctness and security. Minor style issues are not grounds for REQUEST_CHANGES."""
