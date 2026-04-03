import os
import logging
import subprocess

logger = logging.getLogger("bgd")


def triage_errors(config: dict, error_details: str, project_dir: str = None):
    """Invoke Claude Code CLI to diagnose and fix errors."""
    heal_cfg = config.get("self_heal", {})
    if not heal_cfg.get("enabled", True):
        logger.info("Self-healing disabled in config")
        return False

    claude_cmd = heal_cfg.get("claude_code_path", "claude")
    max_retries = heal_cfg.get("max_retries", 3)
    cwd = project_dir or os.path.dirname(os.path.dirname(__file__))
    log_file = config.get("logging", {}).get("file", "logs/bgd.log")

    # Read recent log entries for context
    log_tail = ""
    log_path = os.path.join(cwd, log_file)
    if os.path.exists(log_path):
        with open(log_path, "r") as f:
            lines = f.readlines()
            log_tail = "".join(lines[-50:])

    prompt = (
        f"The Board Game Discounts pipeline encountered errors during execution.\n\n"
        f"Error details:\n{error_details}\n\n"
        f"Recent log output:\n{log_tail}\n\n"
        f"Please:\n"
        f"1. Diagnose the root cause of the error\n"
        f"2. Fix the code if needed\n"
        f"3. If you identify improvements beyond the fix, add them to TODO.md "
        f"for my review — do NOT implement non-critical changes without approval\n"
        f"4. After fixing, run 'python main.py run' to verify the fix works\n"
        f"5. Commit any changes to git\n"
    )

    for attempt in range(1, max_retries + 1):
        logger.info(f"Self-heal attempt {attempt}/{max_retries}")
        try:
            result = subprocess.run(
                [claude_cmd, "--print", "--dangerously-skip-permissions", "-p", prompt],
                cwd=cwd, capture_output=True, text=True, timeout=300
            )
            if result.returncode == 0:
                logger.info(f"Self-heal attempt {attempt} completed")
                logger.info(f"Claude output: {result.stdout[:500]}")
                return True
            else:
                logger.warning(f"Self-heal attempt {attempt} failed: {result.stderr[:300]}")
        except subprocess.TimeoutExpired:
            logger.warning(f"Self-heal attempt {attempt} timed out")
        except FileNotFoundError:
            logger.error(f"Claude Code CLI not found at '{claude_cmd}'")
            return False

    logger.error(f"Self-healing failed after {max_retries} attempts")
    return False
