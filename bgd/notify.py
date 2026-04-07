"""Deal notification via iMessage (macOS) and email.

Standalone module — queries the DB for undelivered active deals per recipient
and sends notifications. If more than a threshold number of deals are pending,
sends a single summary message instead.
"""

import subprocess
import logging

from bgd import db

logger = logging.getLogger("bgd")

SITE_URL = "https://deals.transformativehelp.com"


def _send_imessage(phone_number, message):
    """Send an iMessage via AppleScript. Returns True on success."""
    script = f'''
    tell application "Messages"
        set targetService to 1st account whose service type = iMessage
        set targetBuddy to participant "{phone_number}" of targetService
        send "{message}" to targetBuddy
    end tell
    '''
    try:
        subprocess.run(
            ["osascript", "-e", script],
            check=True, capture_output=True, timeout=30,
        )
        return True
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as e:
        logger.error(f"iMessage send failed to {phone_number}: {e}")
        return False


def _format_deal_message(deal):
    """Format a single deal into a notification message."""
    parts = []
    game = deal["game_name"] or deal["title"]
    parts.append(game)

    if deal["original_price"] and deal["sale_price"]:
        parts.append(f"Was ${deal['original_price']:.2f}, now ${deal['sale_price']:.2f}")
    elif deal["sale_price"]:
        parts.append(f"${deal['sale_price']:.2f}")

    if deal["discount_pct"]:
        parts.append(f"({deal['discount_pct']:.0f}% off)")

    parts.append(SITE_URL)
    return " — ".join(parts)


def _format_summary_message(count):
    """Format a summary message when there are too many deals."""
    return (
        f"{count} new board game deals found! "
        f"Check them out: {SITE_URL}"
    )


def send_notifications(config, conn):
    """Main entry point. Check each active recipient for undelivered deals."""
    notify_cfg = config.get("notify", {})
    if not notify_cfg.get("enabled", False):
        logger.info("Notifications disabled in config")
        return {"sent": 0, "skipped": 0}

    max_individual = notify_cfg.get("max_individual", 10)

    recipients = db.get_active_recipients(conn)
    if not recipients:
        logger.info("No active recipients configured")
        return {"sent": 0, "skipped": 0}

    total_sent = 0
    total_skipped = 0

    for recipient in recipients:
        deals = db.get_unnotified_deals_for_recipient(conn, recipient["id"])
        if not deals:
            logger.info(f"No new deals for {recipient['name']}")
            continue

        if len(deals) > max_individual:
            # Too many — send summary and mark all as delivered
            logger.info(
                f"{len(deals)} deals for {recipient['name']} — sending summary"
            )
            sent = _notify_recipient_summary(recipient, len(deals))
            if sent:
                db.mark_deals_notified_bulk(
                    conn,
                    [d["id"] for d in deals],
                    recipient["id"],
                    method=sent,
                )
                total_sent += 1
            else:
                total_skipped += len(deals)
        else:
            for deal in deals:
                msg = _format_deal_message(deal)
                sent = _notify_recipient(recipient, msg)
                if sent:
                    db.record_notification(
                        conn, deal["id"], recipient["id"], method=sent
                    )
                    total_sent += 1
                else:
                    total_skipped += 1

    logger.info(f"Notifications complete: {total_sent} sent, {total_skipped} skipped")
    return {"sent": total_sent, "skipped": total_skipped}


def _notify_recipient(recipient, message):
    """Send a message to a recipient via their preferred channel(s).
    Returns the method used, or None on failure."""
    if recipient["cell"]:
        if _send_imessage(recipient["cell"], message):
            return "imessage"

    # Email could be added here in the future
    # if recipient["email"]:
    #     if _send_email(recipient["email"], message):
    #         return "email"

    return None


def _notify_recipient_summary(recipient, count):
    """Send a summary message to a recipient. Returns method or None."""
    msg = _format_summary_message(count)
    return _notify_recipient(recipient, msg)
