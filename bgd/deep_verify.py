"""Deep verification of deals using Claude Code CLI.

Uses LLM intelligence to:
- Resolve actual deal URLs from vague Reddit posts
- Check if deals are still active by understanding page content
- Handle bot-blocked sites where simple scraping fails
- Determine sale end dates from context clues

Processes deals in batches to minimize token usage.
"""

import json
import logging
import os
import subprocess
from datetime import datetime

from bgd import db

logger = logging.getLogger("bgd")

BATCH_SIZE = 10  # Smaller batches since verification context is larger


def _build_verify_prompt(deals_batch):
    """Build a prompt for Claude to verify a batch of deals."""
    deal_summaries = []
    for d in deals_batch:
        deal_summaries.append({
            "id": d["id"],
            "title": d["title"],
            "game_name": d["game_name"],
            "url": d["url"],
            "retailer": d["retailer"],
            "sale_price": d["sale_price"],
            "status": d["status"],
            "posted_at": d["posted_at"],
            "notes": d["notes"],
        })

    return f"""You are a board game deal verification assistant.

I need you to check whether these board game deals are still active.
For each deal, use your reasoning to determine the likely status.

DEALS TO VERIFY:
{json.dumps(deal_summaries, indent=2)}

For each deal, determine:
1. **status**: One of "active", "expired", "sold_out"
   - Consider the posted date — very old deals are likely expired
   - Consider the retailer — daily flash sales expire within 24h
   - Known patterns: "Daily Deal" sites rotate daily, Amazon prices fluctuate
   - If the title mentions a specific end date, check if it has passed
   - Board Haven "daily flash sale" deals expire same day

2. **url**: If the current URL is reddit.com or i.redd.it, determine the actual
   product URL. Use the title to figure out the retailer and product.
   Examples:
   - "[Amazon] Catan $25" → construct Amazon search URL
   - "Splendor marvel 30 percent off Amazon" → Amazon search URL for Marvel Splendor
   - "US Amazon catan energies 50 percent off" → Amazon search URL for Catan Energies

3. **retailer**: Correct the retailer name if you can determine it from the title.

4. **reason**: Brief explanation of your reasoning (1 sentence).

Today's date is {datetime.utcnow().strftime("%Y-%m-%d")}.

IMPORTANT:
- Return ONLY valid JSON — no markdown, no explanation
- Use null for fields you don't want to change
- Be conservative — if unsure, keep the deal as "active"

Return a JSON array:
[
  {{"id": 1, "status": "active", "url": null, "retailer": null, "reason": "Amazon deal still within typical sale window"}},
  ...
]"""


from bgd.enrich import parse_claude_json


def deep_verify_deals(config, conn):
    """Use Claude CLI to intelligently verify all active/unverified deals."""
    deals = db.get_deals_for_deep_verify(conn)
    if not deals:
        logger.info("No deals to deep-verify")
        return {"verified": 0, "expired": 0, "resolved_urls": 0}

    logger.info(f"Deep-verifying {len(deals)} deals...")

    claude_cmd = config.get("self_heal", {}).get("claude_code_path", "claude")
    cwd = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    now = datetime.utcnow().isoformat()

    stats = {"verified": 0, "expired": 0, "sold_out": 0, "resolved_urls": 0}

    for i in range(0, len(deals), BATCH_SIZE):
        batch = deals[i:i + BATCH_SIZE]
        prompt = _build_verify_prompt(batch)

        logger.info(f"Deep-verify batch {i // BATCH_SIZE + 1} ({len(batch)} deals)...")

        try:
            result = subprocess.run(
                [claude_cmd, "--print", "-p", prompt],
                cwd=cwd, capture_output=True, text=True, timeout=120
            )

            if result.returncode != 0:
                logger.warning(f"Claude deep-verify failed: {result.stderr[:200]}")
                continue

            updates = parse_claude_json(result.stdout)
            if not updates:
                continue

            for update in updates:
                deal_id = update.get("id")
                if not deal_id:
                    continue

                new_status = update.get("status")
                new_url = update.get("url")
                new_retailer = update.get("retailer")
                reason = update.get("reason", "")

                # Find the original deal
                original = next((d for d in batch if d["id"] == deal_id), None)
                if not original:
                    continue

                old_status = original["status"]
                update_kwargs = {"last_verified_at": now}

                if new_status and new_status != old_status:
                    update_kwargs["status"] = new_status
                    if new_status == "expired":
                        stats["expired"] += 1
                    elif new_status == "sold_out":
                        stats["sold_out"] += 1

                if new_url and new_url != original["url"]:
                    update_kwargs["url"] = new_url
                    stats["resolved_urls"] += 1

                if new_retailer:
                    update_kwargs["retailer"] = new_retailer

                if reason:
                    update_kwargs["notes"] = reason

                # Apply updates
                if "status" in update_kwargs:
                    db.update_deal_status(conn, deal_id, update_kwargs.pop("status"),
                                          **{k: v for k, v in update_kwargs.items()
                                             if k in ("last_verified_at", "verification_failures",
                                                       "sale_price", "original_price", "discount_pct",
                                                       "expires_at", "notes")})
                    # Handle url and retailer separately
                    extra = {}
                    if "url" in update_kwargs:
                        extra["url"] = update_kwargs["url"]
                    if "retailer" in update_kwargs:
                        extra["retailer"] = update_kwargs["retailer"]
                    if extra:
                        db.update_deal_fields(conn, deal_id, **extra)
                else:
                    db.update_deal_fields(conn, deal_id, **update_kwargs)
                    if new_url:
                        db.update_deal_fields(conn, deal_id, url=new_url)
                    if new_retailer:
                        db.update_deal_fields(conn, deal_id, retailer=new_retailer)

                db.log_verification(conn, deal_id, old_status,
                                    new_status or old_status,
                                    "deep_verify",
                                    json.dumps({"reason": reason}))

                stats["verified"] += 1
                if new_status and new_status != old_status:
                    logger.info(f"  Deal #{deal_id}: {old_status} → {new_status} ({reason})")
                elif new_url:
                    logger.info(f"  Deal #{deal_id}: URL resolved ({reason})")
                else:
                    logger.info(f"  Deal #{deal_id}: verified ({reason})")

        except subprocess.TimeoutExpired:
            logger.warning("Claude deep-verify timed out")
        except FileNotFoundError:
            logger.error(f"Claude Code CLI not found at '{claude_cmd}'")
            break

    logger.info(f"Deep-verify complete: {stats}")
    return stats
