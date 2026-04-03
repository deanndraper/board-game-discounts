#!/usr/bin/env python3
"""Board Game Discounts - CLI entry point."""

import argparse
import sys
import traceback

import yaml

from bgd import db, rss, verify, html_gen, publisher, self_heal, enrich, deep_verify, bgg, classify
from bgd.logger import setup_logger


def load_config(path="config.yaml"):
    with open(path, "r") as f:
        return yaml.safe_load(f)


def cmd_fetch(config, conn, logger):
    """Fetch new deals from RSS."""
    deals = rss.fetch_deals(config)
    inserted = 0
    for deal in deals:
        row_id = db.insert_deal(conn, deal)
        if row_id:
            inserted += 1
            logger.info(f"New deal #{row_id}: {deal['title'][:60]}")
    logger.info(f"Fetch complete: {inserted} new deals from {len(deals)} posts")
    return inserted


def cmd_verify(config, conn, logger):
    """Verify all unverified and active deals."""
    stats = verify.verify_all(conn, config)
    return stats


def cmd_generate(config, conn, logger):
    """Generate HTML output."""
    output = html_gen.generate(conn, config)
    logger.info(f"Generated: {output}")
    return output


def cmd_publish(config, logger):
    """Publish docs/ to git."""
    publisher.publish(config)


def cmd_status(conn, logger):
    """Show current DB stats."""
    stats = db.get_db_stats(conn)
    logger.info("=== Database Status ===")
    logger.info(f"  Total deals: {stats['total']}")
    logger.info(f"  Active: {stats['active']}")
    logger.info(f"  Unverified: {stats['unverified']}")
    logger.info(f"  Expired: {stats['expired']}")
    logger.info(f"  Sold out: {stats['sold_out']}")
    logger.info("  --- Post Types ---")
    logger.info(f"  Specific deals: {stats.get('type_specific_deal', 0)}")
    logger.info(f"  Generic sales: {stats.get('type_generic_sale', 0)}")
    logger.info(f"  Discussions: {stats.get('type_discussion', 0)}")
    logger.info(f"  Questions: {stats.get('type_question', 0)}")
    logger.info(f"  Other: {stats.get('type_other', 0) + stats.get('type_meta', 0)}")
    logger.info(f"  Unclassified: {stats.get('type_unclassified', 0)}")
    if stats["last_run"]:
        lr = stats["last_run"]
        logger.info(f"  Last run: {lr.get('started_at', 'N/A')} — {lr.get('status', 'N/A')}")
    return stats


def cmd_classify(config, conn, logger):
    """Classify RSS posts by type (specific_deal, discussion, question, etc.)."""
    stats = classify.classify_deals(config, conn)
    return stats


def cmd_enrich(config, conn, logger):
    """Enrich deals with missing data via Claude Code CLI, then fetch BGG stats."""
    stats = enrich.enrich_deals(config, conn)
    bgg_stats = bgg.fetch_bgg_data(config, conn)
    stats["bgg_updated"] = bgg_stats.get("updated", 0)
    return stats


def cmd_bgg(config, conn, logger):
    """Fetch BGG ratings, ranks, and weight for deals with bgg_id."""
    stats = bgg.fetch_bgg_data(config, conn)
    return stats


def cmd_deep_verify(config, conn, logger):
    """Deep-verify deals using Claude Code CLI intelligence."""
    stats = deep_verify.deep_verify_deals(config, conn)
    return stats


def cmd_run(config, conn, logger):
    """Full pipeline: fetch → classify → enrich → bgg → verify → generate → publish."""
    run_id = db.start_run(conn)
    errors = []
    deals_found = 0
    verify_stats = {}

    # 1. Fetch
    try:
        deals_found = cmd_fetch(config, conn, logger)
    except Exception as e:
        logger.error(f"Fetch failed: {e}", exc_info=True)
        errors.append(f"Fetch: {traceback.format_exc()}")

    # 2. Classify (filter non-deals before spending tokens)
    try:
        cmd_classify(config, conn, logger)
    except Exception as e:
        logger.error(f"Classify failed: {e}", exc_info=True)
        errors.append(f"Classify: {traceback.format_exc()}")

    # 3. Verify (only specific_deal posts)
    try:
        verify_stats = cmd_verify(config, conn, logger)
    except Exception as e:
        logger.error(f"Verify failed: {e}", exc_info=True)
        errors.append(f"Verify: {traceback.format_exc()}")

    # 4. Generate
    try:
        cmd_generate(config, conn, logger)
    except Exception as e:
        logger.error(f"Generate failed: {e}", exc_info=True)
        errors.append(f"Generate: {traceback.format_exc()}")

    # 5. Publish
    try:
        cmd_publish(config, logger)
    except Exception as e:
        logger.error(f"Publish failed: {e}", exc_info=True)
        errors.append(f"Publish: {traceback.format_exc()}")

    # Finalize run log
    status = "success" if not errors else ("partial" if deals_found or verify_stats else "failed")
    error_text = "\n---\n".join(errors) if errors else None
    db.finish_run(
        conn, run_id,
        deals_found=deals_found,
        deals_verified=verify_stats.get("verified", 0),
        deals_expired=verify_stats.get("expired", 0) + verify_stats.get("sold_out", 0),
        errors=len(errors),
        status=status,
        error_details=error_text,
    )

    logger.info(f"Run #{run_id} finished: {status}")

    # 5. Self-heal if errors occurred
    if errors:
        logger.info("Errors detected — invoking self-healing...")
        self_heal.triage_errors(config, error_text)

    # 6. Check for approved TODO items to implement
    try:
        self_heal.implement_approved_todos(config)
    except Exception as e:
        logger.warning(f"TODO implementation check failed: {e}")

    return {"run_id": run_id, "status": status, "errors": errors}


def main():
    parser = argparse.ArgumentParser(description="Board Game Discounts")
    parser.add_argument("command", choices=["run", "fetch", "classify", "verify", "generate", "publish",
                                             "status", "enrich", "deep-verify", "bgg"],
                        help="Command to execute")
    parser.add_argument("--config", default="config.yaml", help="Config file path")
    args = parser.parse_args()

    config = load_config(args.config)
    logger = setup_logger(config)
    conn = db.init_db()

    logger.info(f"=== BGD {args.command} ===")

    try:
        if args.command == "run":
            result = cmd_run(config, conn, logger)
            sys.exit(0 if result["status"] != "failed" else 1)
        elif args.command == "fetch":
            cmd_fetch(config, conn, logger)
        elif args.command == "classify":
            cmd_classify(config, conn, logger)
        elif args.command == "verify":
            cmd_verify(config, conn, logger)
        elif args.command == "generate":
            cmd_generate(config, conn, logger)
        elif args.command == "publish":
            cmd_publish(config, logger)
        elif args.command == "status":
            cmd_status(conn, logger)
        elif args.command == "enrich":
            cmd_enrich(config, conn, logger)
        elif args.command == "deep-verify":
            cmd_deep_verify(config, conn, logger)
        elif args.command == "bgg":
            cmd_bgg(config, conn, logger)
    except Exception as e:
        logger.error(f"Fatal error in '{args.command}': {e}", exc_info=True)
        error_context = (
            f"Command: python main.py {args.command}\n"
            f"Traceback:\n{traceback.format_exc()}"
        )
        self_heal.triage_errors(config, error_context)
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
