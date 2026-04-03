"""Fetch BGG game data (ID, URL, rating, rank, weight).

Three-tier approach:
1. DuckDuckGo web search → get correct BGG ID and URL (free, no auth)
2. BGG XML API → get rating/rank/weight (if token configured)
3. Claude CLI (haiku) → fallback for stats when API unavailable
"""

import json
import logging
import os
import re
import subprocess
import time
from urllib.parse import unquote

import requests
from bs4 import BeautifulSoup

from bgd import db
from bgd.enrich import parse_claude_json

logger = logging.getLogger("bgd")

BGG_API_URL = "https://boardgamegeek.com/xmlapi2/thing"
BGG_BATCH_SIZE = 20
CLAUDE_BATCH_SIZE = 8
DDG_SEARCH_URL = "https://html.duckduckgo.com/html/"
DDG_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
}


# --- Tier 1: DuckDuckGo search for correct BGG ID/URL ---

_ddg_session = None


def _get_ddg_session():
    """Get or create a persistent session for DDG searches."""
    global _ddg_session
    if _ddg_session is None:
        _ddg_session = requests.Session()
        _ddg_session.headers.update(DDG_HEADERS)
    return _ddg_session


def _search_bgg_id(game_name):
    """Search DuckDuckGo for the correct BGG page. Returns (bgg_id, bgg_url) or (None, None)."""
    if not game_name:
        return None, None

    # Clean game name — remove prices, retailer tags, etc.
    clean_name = re.sub(r"\$\d+(?:\.\d{2})?", "", game_name)
    clean_name = re.sub(r"\[.*?\]", "", clean_name)
    clean_name = re.sub(r"\(.*?\)", "", clean_name)
    clean_name = re.sub(r"\d+\s*%\s*off", "", clean_name, flags=re.IGNORECASE)
    clean_name = re.sub(r"(ebay|amazon|deal|free shipping|bullseye deals?|seller).*", "",
                        clean_name, flags=re.IGNORECASE)
    clean_name = clean_name.strip(" -–—,:;")
    if not clean_name or len(clean_name) < 3:
        return None, None

    query = f"boardgamegeek.com {clean_name} board game"
    session = _get_ddg_session()
    try:
        resp = session.get(DDG_SEARCH_URL, params={"q": query}, timeout=10)
        if resp.status_code != 200:
            return None, None

        soup = BeautifulSoup(resp.text, "html.parser")

        # Extract result URLs
        for result in soup.select(".result__url"):
            url_text = result.get_text(strip=True)
            match = re.search(r"boardgamegeek\.com/boardgame/(\d+)", url_text)
            if match:
                bgg_id = int(match.group(1))
                # Reconstruct clean URL from the text
                bgg_url = f"https://{url_text.split('?')[0].strip()}"
                if not bgg_url.startswith("https://boardgamegeek"):
                    bgg_url = f"https://boardgamegeek.com/boardgame/{bgg_id}"
                return bgg_id, bgg_url

        # Fallback: check result links
        for link in soup.select(".result__a"):
            href = link.get("href", "")
            # DDG wraps URLs in redirects
            bgg_match = re.search(r"boardgamegeek\.com%2Fboardgame%2F(\d+)", href)
            if bgg_match:
                bgg_id = int(bgg_match.group(1))
                return bgg_id, f"https://boardgamegeek.com/boardgame/{bgg_id}"
            bgg_match = re.search(r"boardgamegeek\.com/boardgame/(\d+)", href)
            if bgg_match:
                bgg_id = int(bgg_match.group(1))
                return bgg_id, f"https://boardgamegeek.com/boardgame/{bgg_id}"

    except requests.RequestException as e:
        logger.debug(f"DDG search failed for '{game_name}': {e}")

    return None, None


def _verify_and_update_bgg_ids(deals, conn):
    """Use web search to verify/correct BGG IDs for deals. Returns count updated."""
    updated = 0
    delay = 5  # Start with 5s between requests
    consecutive_fails = 0

    for deal in deals:
        game_name = deal["game_name"] or deal["title"]
        if not game_name:
            continue

        bgg_id, bgg_url = _search_bgg_id(game_name)
        if bgg_id:
            old_id = deal["bgg_id"]
            kwargs = {"bgg_id": bgg_id, "bgg_url": bgg_url}

            if old_id and old_id != bgg_id:
                logger.info(f"  Deal #{deal['id']}: BGG ID corrected {old_id} → {bgg_id} ({game_name})")
            elif not old_id:
                logger.info(f"  Deal #{deal['id']}: BGG ID found {bgg_id} ({game_name})")

            db.update_deal_fields(conn, deal["id"], **kwargs)
            updated += 1
            consecutive_fails = 0
            delay = max(5, delay - 1)  # Gradually speed up on success
        else:
            consecutive_fails += 1
            if consecutive_fails >= 3:
                logger.info(f"  DDG rate-limited after {updated} lookups, stopping web search")
                break
            delay = min(15, delay + 3)  # Back off on failure

        time.sleep(delay)

    return updated


# --- Tier 2: BGG XML API for stats ---

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
                headers={"Authorization": f"Bearer {token}", "Accept": "application/xml"},
                timeout=30,
            )

            if resp.status_code == 401:
                logger.warning("BGG API returned 401 — token may be invalid")
                return None
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
        "bgg_rating": rating_val,
        "bgg_rank": rank_val,
        "bgg_weight": weight_val,
    }


# --- Tier 3: Claude CLI for stats ---

def _fetch_ids_from_claude(deals, config):
    """Use Claude CLI to find BGG IDs for games. Returns dict of {deal_id: data}."""
    claude_cmd = config.get("self_heal", {}).get("claude_code_path", "claude")
    model = config.get("models", {}).get("bgg", "haiku")
    cwd = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    results = {}

    for i in range(0, len(deals), CLAUDE_BATCH_SIZE):
        batch = deals[i:i + CLAUDE_BATCH_SIZE]
        summaries = [{"id": d["id"], "game_name": d["game_name"], "title": d["title"]}
                     for d in batch]

        prompt = f"""Find the BoardGameGeek ID and URL for each board game.

GAMES:
{json.dumps(summaries, indent=2)}

For each game, provide:
- **bgg_id**: The BGG game ID (integer from the URL boardgamegeek.com/boardgame/ID)
- **bgg_url**: The full BGG URL (e.g., https://boardgamegeek.com/boardgame/13/catan)

If the item is not a board game (accessories, tables, etc.), set both to null.

Return ONLY a JSON array:
[{{"id": 1, "bgg_id": 13, "bgg_url": "https://boardgamegeek.com/boardgame/13/catan"}}, ...]"""

        try:
            cmd = [claude_cmd, "--print", "-p", prompt]
            if model:
                cmd.extend(["--model", model])
            result = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True, timeout=120)

            if result.returncode != 0:
                continue

            updates = parse_claude_json(result.stdout)
            if updates:
                for item in updates:
                    deal_id = item.get("id")
                    if deal_id:
                        results[deal_id] = item
        except (subprocess.TimeoutExpired, FileNotFoundError):
            break

    return results


def _fetch_stats_from_claude(deals, config):
    """Fetch BGG stats via Claude CLI. Returns dict of {deal_id: data}."""
    claude_cmd = config.get("self_heal", {}).get("claude_code_path", "claude")
    model = config.get("models", {}).get("bgg", "haiku")
    cwd = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    results = {}

    for i in range(0, len(deals), CLAUDE_BATCH_SIZE):
        batch = deals[i:i + CLAUDE_BATCH_SIZE]
        deal_summaries = [{
            "id": d["id"],
            "bgg_id": d["bgg_id"],
            "game_name": d["game_name"],
        } for d in batch]

        prompt = f"""You are a board game data assistant. I need BGG stats for these games.
The BGG IDs have been verified via web search — do NOT change them.

GAMES:
{json.dumps(deal_summaries, indent=2)}

For each game, provide the BGG stats:
- **bgg_rating**: Average rating (1-10 scale, e.g., 7.85)
- **bgg_rank**: Overall Board Game Rank (integer, e.g., 42)
- **bgg_weight**: Complexity/weight (1-5 scale, e.g., 2.35)

If a game is not a board game (accessories, tables, etc.), set all to null.

Return ONLY a JSON array, no markdown:
[{{"id": 1, "bgg_rating": 7.15, "bgg_rank": 400, "bgg_weight": 2.32}}, ...]"""

        try:
            cmd = [claude_cmd, "--print", "-p", prompt]
            if model:
                cmd.extend(["--model", model])
            result = subprocess.run(
                cmd, cwd=cwd, capture_output=True, text=True, timeout=180,
            )

            if result.returncode != 0:
                logger.warning(f"Claude BGG stats failed: {result.stderr[:200]}")
                continue

            updates = parse_claude_json(result.stdout)
            if updates:
                for item in updates:
                    deal_id = item.get("id")
                    if deal_id:
                        results[deal_id] = item

        except subprocess.TimeoutExpired:
            logger.warning("Claude BGG stats timed out")
        except FileNotFoundError:
            logger.error(f"Claude Code CLI not found at '{claude_cmd}'")
            break

    return results


# --- Main entry point ---

def fetch_bgg_data(config, conn):
    """Fetch BGG data for deals. Web search for IDs, then API or Claude for stats."""
    deals = db.get_deals_needing_bgg_data(conn)

    # Also include deals that have no bgg_id at all
    deals_no_id = conn.execute("""
        SELECT * FROM deals
        WHERE bgg_id IS NULL
          AND status IN ('active', 'unverified')
          AND (post_type IS NULL OR post_type = 'specific_deal')
        ORDER BY id
    """).fetchall()

    all_deals = list(deals) + [d for d in deals_no_id if d["id"] not in {dd["id"] for dd in deals}]

    if not all_deals:
        logger.info("No deals need BGG data")
        return {"updated": 0, "ids_found": 0}

    logger.info(f"Fetching BGG data for {len(all_deals)} deals...")

    # Step 1: Web search to verify/find correct BGG IDs
    deals_needing_id = [d for d in all_deals if not d["bgg_id"] or not d["bgg_url"]]
    ids_found = 0
    if deals_needing_id:
        logger.info(f"  Searching web for BGG IDs ({len(deals_needing_id)} deals)...")
        ids_found = _verify_and_update_bgg_ids(deals_needing_id, conn)

        # If web search was rate-limited, use Claude to find remaining IDs
        remaining_no_id = [d for d in deals_needing_id
                           if not db.update_deal_fields  # just reload
                           or True]
        # Re-check which deals still need IDs after web search
        still_no_id = conn.execute("""
            SELECT * FROM deals WHERE bgg_id IS NULL
              AND status IN ('active', 'unverified')
              AND (post_type IS NULL OR post_type = 'specific_deal')
            ORDER BY id
        """).fetchall()
        if still_no_id:
            logger.info(f"  {len(still_no_id)} deals still need BGG IDs, using Claude...")
            claude_id_results = _fetch_ids_from_claude(still_no_id, config)
            for deal in still_no_id:
                if deal["id"] in claude_id_results:
                    data = claude_id_results[deal["id"]]
                    kwargs = {k: v for k, v in data.items()
                              if k in ("bgg_id", "bgg_url") and v is not None}
                    if kwargs:
                        db.update_deal_fields(conn, deal["id"], **kwargs)
                        ids_found += 1

    # Reload deals with updated IDs
    deals_needing_stats = db.get_deals_needing_bgg_data(conn)
    if not deals_needing_stats:
        logger.info(f"BGG data complete: {ids_found} IDs found, no stats needed")
        return {"updated": ids_found, "ids_found": ids_found}

    # Step 2: Try BGG API for stats
    bgg_token = config.get("bgg", {}).get("api_token")
    updated = 0

    if bgg_token:
        bgg_ids = [d["bgg_id"] for d in deals_needing_stats if d["bgg_id"]]
        api_results = _fetch_from_api(bgg_ids, bgg_token)

        if api_results is not None:
            for deal in deals_needing_stats:
                bgg_id = deal["bgg_id"]
                if bgg_id and bgg_id in api_results:
                    db.update_deal_fields(conn, deal["id"], **api_results[bgg_id])
                    updated += 1
                    data = api_results[bgg_id]
                    logger.info(f"  Deal #{deal['id']}: stats from API "
                                f"(rating={data.get('bgg_rating')}, rank={data.get('bgg_rank')}, "
                                f"weight={data.get('bgg_weight')})")

            remaining = [d for d in deals_needing_stats
                         if d["bgg_id"] not in api_results and d["bgg_id"]]
            if remaining:
                logger.info(f"  {len(remaining)} deals not in API, using Claude...")
                deals_needing_stats = remaining
            else:
                deals_needing_stats = []
        else:
            logger.info("BGG API auth failed, using Claude for stats")

    # Step 3: Claude CLI for remaining stats
    if deals_needing_stats:
        claude_results = _fetch_stats_from_claude(deals_needing_stats, config)
        for deal in deals_needing_stats:
            if deal["id"] in claude_results:
                data = claude_results[deal["id"]]
                kwargs = {k: v for k, v in data.items()
                          if k in ("bgg_rating", "bgg_rank", "bgg_weight") and v is not None}
                if kwargs:
                    db.update_deal_fields(conn, deal["id"], **kwargs)
                    updated += 1
                    logger.info(f"  Deal #{deal['id']}: stats from Claude "
                                f"(rating={kwargs.get('bgg_rating')}, "
                                f"rank={kwargs.get('bgg_rank')}, "
                                f"weight={kwargs.get('bgg_weight')})")

    total = ids_found + updated
    logger.info(f"BGG data complete: {ids_found} IDs found, {updated} stats updated")
    return {"updated": total, "ids_found": ids_found}
