import os
import json
import logging
import subprocess
from datetime import datetime

logger = logging.getLogger("bgd")


def _read_log_tail(cwd: str, config: dict, lines: int = 100) -> str:
    """Read the last N lines of the log file."""
    log_file = config.get("logging", {}).get("file", "logs/bgd.log")
    log_path = os.path.join(cwd, log_file)
    if not os.path.exists(log_path):
        return "(no log file found)"
    with open(log_path, "r") as f:
        all_lines = f.readlines()
        return "".join(all_lines[-lines:])


def _read_todo(cwd: str) -> str:
    """Read existing TODO.md if it exists."""
    todo_path = os.path.join(cwd, "TODO.md")
    if os.path.exists(todo_path):
        with open(todo_path, "r") as f:
            return f.read()
    return ""


def triage_errors(config: dict, error_details: str, project_dir: str = None):
    """Invoke Claude Code CLI to diagnose and fix errors, then re-run to verify."""
    heal_cfg = config.get("self_heal", {})
    if not heal_cfg.get("enabled", True):
        logger.info("Self-healing disabled in config")
        return False

    claude_cmd = heal_cfg.get("claude_code_path", "claude")
    model = config.get("models", {}).get("self_heal", "")
    max_retries = heal_cfg.get("max_retries", 3)
    cwd = project_dir or os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    log_tail = _read_log_tail(cwd, config)
    existing_todo = _read_todo(cwd)

    prompt = f"""You are maintaining the Board Game Discounts pipeline. It encountered errors.

ERROR DETAILS:
{error_details}

RECENT LOG (last 100 lines):
{log_tail}

EXISTING TODO.md:
{existing_todo if existing_todo else "(none)"}

YOUR INSTRUCTIONS:
1. Read CLAUDE.md to understand the project structure and patterns.
2. Read the relevant source files to understand the error context.
3. Diagnose the root cause of the error.
4. Fix the code — make the minimal change needed to resolve the issue.
5. Run `python main.py run` to verify your fix works end-to-end.
6. If the fix works, commit your changes to git with a clear message.
7. If you identify improvements or better approaches beyond the immediate fix,
   append them to TODO.md in this format:

   ## Suggested Improvement - [date]
   **What:** [brief description]
   **Why:** [reasoning]
   **Risk:** [low/medium/high]
   **Approved:** [ ] (owner must check this box before implementation)

   Do NOT implement non-critical improvements — only add them to TODO.md.
8. If you cannot fix the error after investigating, log what you found and
   add a detailed entry to TODO.md describing the issue for manual review.

CRITICAL: Only fix the immediate error. Do not refactor or improve unrelated code.
"""

    for attempt in range(1, max_retries + 1):
        logger.info(f"Self-heal attempt {attempt}/{max_retries}")
        try:
            cmd = [claude_cmd, "--dangerously-skip-permissions", "-p", prompt]
            if model:
                cmd.extend(["--model", model])
            result = subprocess.run(
                cmd, cwd=cwd, capture_output=True, text=True, timeout=600
            )

            logger.info(f"Self-heal attempt {attempt} exit code: {result.returncode}")
            if result.stdout:
                logger.info(f"Claude output (truncated): {result.stdout[:1000]}")
            if result.stderr:
                logger.warning(f"Claude stderr: {result.stderr[:500]}")

            if result.returncode == 0:
                # Verify the fix by running a quick status check
                verify_result = subprocess.run(
                    ["python", "main.py", "status"],
                    cwd=cwd, capture_output=True, text=True, timeout=30
                )
                if verify_result.returncode == 0:
                    logger.info(f"Self-heal attempt {attempt} succeeded — pipeline verified")
                    return True
                else:
                    logger.warning(f"Self-heal attempt {attempt}: fix applied but status check failed")
                    # Update prompt with new context for next attempt
                    error_details = (
                        f"Previous fix attempt did not resolve the issue.\n"
                        f"Status check output: {verify_result.stdout}\n"
                        f"Status check errors: {verify_result.stderr}"
                    )
            else:
                logger.warning(f"Self-heal attempt {attempt} failed")

        except subprocess.TimeoutExpired:
            logger.warning(f"Self-heal attempt {attempt} timed out (600s)")
        except FileNotFoundError:
            logger.error(f"Claude Code CLI not found at '{claude_cmd}' — "
                         f"install it or update self_heal.claude_code_path in config.yaml")
            return False

    logger.error(f"Self-healing failed after {max_retries} attempts — manual intervention needed")
    _add_manual_review_todo(cwd, error_details)
    return False


def suggest_improvement(config: dict, suggestion: str, project_dir: str = None):
    """Add an improvement suggestion to TODO.md without implementing it."""
    cwd = project_dir or os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    todo_path = os.path.join(cwd, "TODO.md")
    date = datetime.utcnow().strftime("%Y-%m-%d")

    entry = (
        f"\n## Suggested Improvement - {date}\n"
        f"**What:** {suggestion}\n"
        f"**Why:** Identified during pipeline execution\n"
        f"**Risk:** low\n"
        f"**Approved:** [ ] (owner must check this box before implementation)\n"
    )

    with open(todo_path, "a") as f:
        if not os.path.exists(todo_path) or os.path.getsize(todo_path) == 0:
            f.write("# Board Game Discounts — TODO\n\n"
                    "Items below are suggested by the self-healing system.\n"
                    "Check the **Approved** box to authorize implementation.\n")
        f.write(entry)

    logger.info(f"Improvement suggestion added to TODO.md: {suggestion[:80]}")


def implement_approved_todos(config: dict, project_dir: str = None):
    """Check TODO.md for approved items and implement them via Claude Code."""
    heal_cfg = config.get("self_heal", {})
    if not heal_cfg.get("enabled", True):
        return

    claude_cmd = heal_cfg.get("claude_code_path", "claude")
    cwd = project_dir or os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    todo_path = os.path.join(cwd, "TODO.md")

    if not os.path.exists(todo_path):
        return

    with open(todo_path, "r") as f:
        content = f.read()

    # Look for approved items: [x] or [X]
    if "[x]" not in content.lower():
        return

    logger.info("Found approved TODO items — invoking Claude Code to implement")

    prompt = f"""You are maintaining the Board Game Discounts pipeline.

The owner has approved improvements in TODO.md. Read TODO.md and implement ONLY
the items where the **Approved** checkbox is checked ([x] or [X]).

After implementing each approved item:
1. Test with `python main.py run`
2. If tests pass, commit the changes
3. Update TODO.md: change the status to **Implemented** and add the date
4. If an approved item turns out to be infeasible, note why in TODO.md

Do NOT implement items that are not approved.
Read CLAUDE.md for project context before making changes.
"""

    model = config.get("models", {}).get("self_heal", "")
    try:
        cmd = [claude_cmd, "--dangerously-skip-permissions", "-p", prompt]
        if model:
            cmd.extend(["--model", model])
        result = subprocess.run(
            cmd, cwd=cwd, capture_output=True, text=True, timeout=600
        )
        if result.returncode == 0:
            logger.info("Approved TODO items implemented successfully")
        else:
            logger.warning(f"TODO implementation had issues: {result.stderr[:300]}")
    except (subprocess.TimeoutExpired, FileNotFoundError) as e:
        logger.error(f"Could not implement TODOs: {e}")


def _add_manual_review_todo(cwd: str, error_details: str):
    """When self-healing fails, add a detailed TODO for manual review."""
    todo_path = os.path.join(cwd, "TODO.md")
    date = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")

    entry = (
        f"\n## NEEDS MANUAL REVIEW - {date}\n"
        f"**What:** Self-healing failed to resolve pipeline error\n"
        f"**Error:** {error_details[:500]}\n"
        f"**Status:** Unresolved after max retries\n"
        f"**Action needed:** Manual investigation required\n"
    )

    with open(todo_path, "a") as f:
        if os.path.getsize(todo_path) == 0 if os.path.exists(todo_path) else True:
            f.write("# Board Game Discounts — TODO\n\n"
                    "Items below are suggested by the self-healing system.\n"
                    "Check the **Approved** box to authorize implementation.\n")
        f.write(entry)

    logger.info("Added manual review entry to TODO.md")
