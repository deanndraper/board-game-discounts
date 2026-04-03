"""Fetch BGG game data (rating, rank, weight) via API or Claude CLI fallback.

BGG XML API requires an auth token (since late 2025).
If configured, uses the API in batches of 20. Otherwise falls back to Claude CLI.
"""

import json
import logging
import os
import subprocess

import requests
from bs4 import BeautifulSoup

from bgd import db
from bgd.enrich import parse_claude_json

logger = logging.getLogger("bgd")

BGG_API_URL = "https://boardgamegeek.com/xmlapi2/thing"
BGG_BATCH_SIZE = 20  # API supports up to 20 IDs per request
CLAUDE_BATCH_SIZE = 8


def _fetch_from_api(bgg_ids, token):
    """Fetch game data from BGG XML API. Returns dict of {bgg_id: data}."""
    results = {}
    for i in range(0, len(bgg_ids), BGG_BATCH_SIZE):
        batch = bgg_ids[i:i + BGG_BATCH_SIZE]
        ids_str = ",".join(str(bid) for bid in batch)

        try:
            resp = requests.get(
                BGG_API_URL,
                params={"id": ids_str, "stats": "1"},
                headers={
                    "Authorization": f"Bearer {token}",
                    "Accept": "application/xml",
                },
                timeout=30,
            )

            if resp.status_code == 401:
                logger.warning("BGG API returned 401 — token may be invalid or expired")
                return None  # Signal to fall back to Claude
            if resp.status_code != 200:
                logger.warning(f"BGG API returned {resp.status_code}")
                continue

            soup = BeautifulSoup(resp.text, "xml")
            for item in soup.find_all("item"):
                bgg_id = int(item["id"])
                data = _parse_bgg_item(item, bgg_id)
                if data:
                    results[bgg_id] = data

        except requests.RequestException as e:
            logger.warning(f"BGG API request failed: {e}")
            continue

    return results


def _parse_bgg_item(item, bgg_id):
    """Parse a single BGG XML item into a data dict."""
    ratings = item.find("ratings")
    if not ratings:
        return None

    avg = ratings.find("average")
    weight = ratings.find("averageweight")

    rank_val = None
    for rank_tag in ratings.find_all("rank"):
        if rank_tag.get("name") == "boardgame":
            val = rank_tag.get("value")
            if val and val != "Not Ranked":
                rank_val = int(val)

    rating_val = None
    if avg and avg.get("value"):
        try:
            rating_val = round(float(avg["value"]), 2)
        except ValueError:
            pass

    weight_val = None
    if weight and weight.get("value"):
        try:
            weight_val = round(float(weight["value"]), 2)
        except ValueError:
            pass

    return {
        "bgg_id": bgg_id,
        "bgg_rating": rating_val,
        "bgg_rank": rank_val,
        "bgg_weight": weight_val,
        "bgg_url": f"https://boardgamegeek.com/boardgame/{bgg_id}",
    }


def _fetch_from_claude(deals, config):
    """Fetch BGG data via Claude CLI. Returns dict of {deal_id: data}."""
    claude_cmd = config.get("self_heal", {}).get("claude_code_path", "claude")
    model = config.get("models", {}).get("bgg", "haiku")
    cwd = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    results = {}

    for i in range(0, len(deals), CLAUDE_BATCH_SIZE):
        batch = deals[i:i + CLAUDE_BATCH_SIZE]
        deal_summaries = []
        for d in batch:
            deal_summaries.append({
                "id": d["id"],
                "bgg_id": d["bgg_id"],
                "game_name": d["game_name"],
                "title": d["title"],
            })

        prompt = f"""You are a board game data assistant. I need BGG (BoardGameGeek) data for these games.

GAMES:
{json.dumps(deal_summaries, indent=2)}

For each game, provide:
- **bgg_id**: The BGG game ID (confirm or correct the one provided)
- **bgg_rating**: The BGG average rating (1-10 scale, e.g., 7.85)
- **bgg_rank**: The overall BGG Board Game Rank (integer, e.g., 42)
- **bgg_weight**: The BGG complexity/weight rating (1-5 scale, e.g., 2.35)
- **bgg_url**: The BGG page URL

Use your knowledge of BoardGameGeek data. If a game is not on BGG (accessories, tables, etc.), set all fields to null.

Return ONLY a JSON array, no markdown:
[
  {{"id": 1, "bgg_id": 13, "bgg_rating": 7.15, "bgg_rank": 400, "bgg_weight": 2.32, "bgg_url": "https://boardgamegeek.com/boardgame/13"}},
  ...
]"""

        try:
            cmd = [claude_cmd, "--print", "-p", prompt]
            if model:
                cmd.extend(["--model", model])
            result = subprocess.run(
                cmd, cwd=cwd, capture_output=True, text=True, timeout=180,
            )

            if result.returncode != 0:
                logger.warning(f"Claude BGG lookup failed: {result.stderr[:200]}")
                continue

            updates = parse_claude_json(result.stdout)
            if updates:
                for item in updates:
                    deal_id = item.get("id")
                    if deal_id:
                        results[deal_id] = item

        except subprocess.TimeoutExpired:
            logger.warning("Claude BGG lookup timed out")
        except FileNotFoundError:
            logger.error(f"Claude Code CLI not found at '{claude_cmd}'")
            break

    return results


def fetch_bgg_data(config, conn):
    """Fetch BGG stats for all deals that have a bgg_id but missing data."""
    deals = db.get_deals_needing_bgg_data(conn)
    if not deals:
        # Also check deals without bgg_id that need enrichment
        deals = conn.execute("""
            SELECT * FROM deals
            WHERE status IN ('active', 'unverified')
              AND bgg_id IS NOT NULL
              AND (bgg_rating IS NULL OR bgg_rank IS NULL OR bgg_weight IS NULL)
            ORDER BY id
        """).fetchall()

    if not deals:
        logger.info("No deals need BGG data")
        return {"updated": 0}

    logger.info(f"Fetching BGG data for {len(deals)} deals...")

    bgg_token = config.get("bgg", {}).get("api_token")
    updated = 0

    if bgg_token:
        # Try API first
        bgg_ids = [d["bgg_id"] for d in deals if d["bgg_id"]]
        api_results = _fetch_from_api(bgg_ids, bgg_token)

        if api_results is not None:
            # API worked — apply results
            for deal in deals:
                bgg_id = deal["bgg_id"]
                if bgg_id and bgg_id in api_results:
                    data = api_results[bgg_id]
                    db.update_deal_fields(conn, deal["id"], **data)
                    updated += 1
                    logger.info(f"  Deal #{deal['id']}: BGG data from API "
                                f"(rating={data.get('bgg_rating')}, "
                                f"rank={data.get('bgg_rank')}, "
                                f"weight={data.get('bgg_weight')})")

            # Find deals the API didn't cover
            remaining = [d for d in deals if d["bgg_id"] not in api_results]
            if remaining:
                logger.info(f"  {len(remaining)} deals not found via API, trying Claude...")
                claude_results = _fetch_from_claude(remaining, config)
                for deal in remaining:
                    if deal["id"] in claude_results:
                        data = claude_results[deal["id"]]
                        kwargs = {k: v for k, v in data.items()
                                  if k in ("bgg_id", "bgg_rating", "bgg_rank", "bgg_weight", "bgg_url")
                                  and v is not None}
                        if kwargs:
                            db.update_deal_fields(conn, deal["id"], **kwargs)
                            updated += 1
        else:
            # API auth failed — fall through to Claude
            logger.info("BGG API auth failed, using Claude CLI fallback")
            bgg_token = None  # Force Claude path below

    if not bgg_token:
        # Claude-only path
        claude_results = _fetch_from_claude(deals, config)
        for deal in deals:
            if deal["id"] in claude_results:
                data = claude_results[deal["id"]]
                kwargs = {k: v for k, v in data.items()
                          if k in ("bgg_id", "bgg_rating", "bgg_rank", "bgg_weight", "bgg_url")
                          and v is not None}
                if kwargs:
                    db.update_deal_fields(conn, deal["id"], **kwargs)
                    updated += 1
                    logger.info(f"  Deal #{deal['id']}: BGG data from Claude "
                                f"(rating={kwargs.get('bgg_rating')}, "
                                f"rank={kwargs.get('bgg_rank')}, "
                                f"weight={kwargs.get('bgg_weight')})")

    logger.info(f"BGG data complete: {updated}/{len(deals)} deals updated")
    return {"updated": updated}
