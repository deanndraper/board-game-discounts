import os
import logging
import subprocess
from datetime import datetime

logger = logging.getLogger("bgd")


def publish(config: dict, project_dir: str = None):
    """Commit and push docs/ to git."""
    pub_cfg = config.get("publish", {})
    if not pub_cfg.get("enabled", True):
        logger.info("Publishing disabled in config")
        return

    if not pub_cfg.get("auto_commit", True):
        logger.info("Auto-commit disabled in config")
        return

    cwd = project_dir or os.path.dirname(os.path.dirname(__file__))
    branch = pub_cfg.get("branch", "main")
    timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    msg = pub_cfg.get("commit_message", "Update deals - {timestamp}").format(timestamp=timestamp)

    try:
        # Check if there are changes to commit
        result = subprocess.run(
            ["git", "status", "--porcelain", "docs/"],
            cwd=cwd, capture_output=True, text=True
        )
        if not result.stdout.strip():
            logger.info("No changes in docs/ to publish")
            return

        subprocess.run(["git", "add", "docs/"], cwd=cwd, check=True,
                        capture_output=True, text=True)
        subprocess.run(["git", "commit", "-m", msg], cwd=cwd, check=True,
                        capture_output=True, text=True)
        logger.info(f"Committed: {msg}")

        # Push (may fail if no remote configured yet)
        result = subprocess.run(
            ["git", "push", "origin", branch], cwd=cwd,
            capture_output=True, text=True
        )
        if result.returncode == 0:
            logger.info(f"Pushed to origin/{branch}")
        else:
            logger.warning(f"Push failed (remote may not be configured): {result.stderr.strip()}")

    except subprocess.CalledProcessError as e:
        logger.error(f"Git operation failed: {e.stderr}")
        raise
