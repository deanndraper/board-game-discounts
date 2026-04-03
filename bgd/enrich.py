"""Enrich deals with missing data using Claude Code CLI.

Batches deals into a single Claude call to minimize token usage.
Fills in: bgg_id, game_name, sale_price, original_price, discount_pct, url.
"""

import json
import logging
import os
import subprocess

from bgd import db

logger = logging.getLogger("bgd")

BATCH_SIZE = 15  # Max deals per Claude call to keep prompt manageable


def _build_enrich_prompt(deals_batch):
    """Build a prompt for Claude to enrich a batch of deals."""
    deal_summaries = []
    for d in deals_batch:
        missing = []
        if not d["bgg_id"]:
            missing.append("bgg_id")
        if not d["sale_price"]:
            missing.append("sale_price")
        if not d["original_price"]:
            missing.append("original_price")
        if not d["discount_pct"]:
            missing.append("discount_pct")
        if d["url"] and ("reddit.com" in d["url"] or "i.redd.it" in d["url"]):
            missing.append("url")

        deal_summaries.append({
            "id": d["id"],
            "title": d["title"],
            "game_name": d["game_name"],
            "url": d["url"],
            "retailer": d["retailer"],
            "sale_price": d["sale_price"],
            "original_price": d["original_price"],
            "missing": missing,
        })

    return f"""You are a board game deals data enrichment assistant.

I have board game deals with missing data. For each deal below, fill in the missing fields.

DEALS TO ENRICH:
{json.dumps(deal_summaries, indent=2)}

FOR EACH DEAL, provide:
- **bgg_id**: The BoardGameGeek game ID (integer). Search your knowledge for the game name.
  Example: "Catan" = 13, "Ticket to Ride" = 9209, "Wingspan" = 266192.
  If the deal is not a board game (e.g., a gaming table, accessories), set to null.
- **game_name**: Cleaned up game name (remove retailer tags, prices, etc.)
- **sale_price**: The sale/deal price in USD (float). Parse from the title if present.
- **original_price**: The original/MSRP price in USD (float). Use your knowledge of board game MSRPs.
- **discount_pct**: Percentage discount (float, e.g., 40.0 for 40% off).
- **url**: If the current URL is a reddit.com or i.redd.it link, figure out the actual
  retailer URL based on the title. For example, if the title says "[Amazon] Catan $25",
  the URL should be the Amazon product page. Construct a reasonable search/product URL.
  If you can't determine it, keep the existing URL.

IMPORTANT:
- Only fill in fields listed in "missing" for each deal
- Return ONLY valid JSON — no markdown, no explanation
- Use null for any field you genuinely cannot determine

Return a JSON array:
[
  {{"id": 1, "bgg_id": 12345, "game_name": "Game Name", "sale_price": 29.99, "original_price": 49.99, "discount_pct": 40.0, "url": "https://..."}},
  ...
]"""


def parse_claude_json(output):
    """Extract JSON array from Claude's response, handling markdown fences."""
    output = output.strip()

    # Strip markdown code fences
    if "```" in output:
        # Find content between first ``` and last ```
        lines = output.split("\n")
        in_fence = False
        json_lines = []
        for line in lines:
            if line.strip().startswith("```"):
                in_fence = not in_fence
                continue
            if in_fence:
                json_lines.append(line)
        if json_lines:
            output = "\n".join(json_lines).strip()

    import re

    # Clean common LLM JSON issues
    # Remove non-ASCII chars from numeric value positions
    output = re.sub(r':\s*[^\x00-\x7F]+(\d+)', r': \1', output)
    # Remove trailing commas before } or ]
    output = re.sub(r',\s*([}\]])', r'\1', output)

    # Try direct parse
    try:
        return json.loads(output)
    except json.JSONDecodeError:
        pass

    # Find all JSON arrays and merge them (LLMs sometimes output multiple arrays)
    all_items = []
    for match in re.finditer(r'\[.*?\]', output, re.DOTALL):
        try:
            items = json.loads(match.group())
            if isinstance(items, list):
                all_items.extend(items)
        except json.JSONDecodeError:
            continue

    if all_items:
        return all_items

    logger.warning("Could not parse Claude response as JSON")
    return None


def enrich_deals(config, conn):
    """Find deals with missing data and enrich them via Claude Code CLI."""
    deals = db.get_deals_needing_enrichment(conn)
    if not deals:
        logger.info("No deals need enrichment")
        return {"enriched": 0, "total": 0}

    logger.info(f"Found {len(deals)} deals needing enrichment")

    claude_cmd = config.get("self_heal", {}).get("claude_code_path", "claude")
    model = config.get("models", {}).get("enrich", "haiku")
    cwd = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    enriched_count = 0

    # Process in batches
    for i in range(0, len(deals), BATCH_SIZE):
        batch = deals[i:i + BATCH_SIZE]
        prompt = _build_enrich_prompt(batch)

        logger.info(f"Enriching batch {i // BATCH_SIZE + 1} ({len(batch)} deals)...")

        try:
            cmd = [claude_cmd, "--print", "-p", prompt]
            if model:
                cmd.extend(["--model", model])
            result = subprocess.run(
                cmd, cwd=cwd, capture_output=True, text=True, timeout=120
            )

            if result.returncode != 0:
                logger.warning(f"Claude enrichment failed: {result.stderr[:200]}")
                continue

            updates = parse_claude_json(result.stdout)
            if not updates:
                continue

            for update in updates:
                deal_id = update.get("id")
                if not deal_id:
                    continue

                kwargs = {}
                for field in ("bgg_id", "game_name", "sale_price", "original_price",
                              "discount_pct", "url"):
                    val = update.get(field)
                    if val is not None:
                        kwargs[field] = val

                # Recalculate discount if we now have both prices
                if "sale_price" in kwargs and "original_price" in kwargs:
                    sp = kwargs["sale_price"]
                    op = kwargs["original_price"]
                    if op and op > 0:
                        kwargs["discount_pct"] = round((1 - sp / op) * 100, 1)

                if kwargs:
                    db.update_deal_fields(conn, deal_id, **kwargs)
                    enriched_count += 1
                    logger.info(f"  Enriched deal #{deal_id}: {list(kwargs.keys())}")

        except subprocess.TimeoutExpired:
            logger.warning("Claude enrichment timed out")
        except FileNotFoundError:
            logger.error(f"Claude Code CLI not found at '{claude_cmd}'")
            break

    logger.info(f"Enrichment complete: {enriched_count}/{len(deals)} deals updated")
    return {"enriched": enriched_count, "total": len(deals)}
