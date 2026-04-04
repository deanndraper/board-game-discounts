"""Classify Reddit posts as specific deals, generic sales, discussions, etc.

Two-tier approach:
1. Regex heuristics (zero tokens) for obvious cases
2. Claude CLI batch (haiku) for ambiguous remainder
"""

import json
import logging
import os
import re
import subprocess

from bgd import db
from bgd.enrich import parse_claude_json

logger = logging.getLogger("bgd")

# --- Heuristic patterns ---

DISCUSSION_TAGS = re.compile(r"\[(discussion|psa|info|guide|tip|fyi)\]", re.IGNORECASE)
META_TAGS = re.compile(r"\[(meta|mod|announcement|rule|megathread)\]", re.IGNORECASE)
QUESTION_STARTERS = re.compile(
    r"^(how|what|where|when|why|which|has anyone|is there|does anyone|can someone|should i|any)\b",
    re.IGNORECASE,
)
GENERIC_SALE_SIGNALS = re.compile(
    r"(multiple\s+(deals|sales|games)|sitewide|store.?wide|clearance\s+sale|"
    r"various\s+games|list\s+of\s+deals|weekly\s+deals|deal\s+roundup|"
    r"multiple\s+website|spring\s+sale\b|summer\s+sale\b|holiday\s+sale\b)",
    re.IGNORECASE,
)
SPECIFIC_DEAL_BRACKET = re.compile(r"\[.+?\].*\$\d+")  # [Retailer] ... $price
PRICE_PATTERN = re.compile(r"\$\d+(?:\.\d{2})?")
PERCENT_OFF = re.compile(r"(\d+)\s*(%\s*off|percent\s*off)", re.IGNORECASE)
IMAGE_DOMAINS = {"i.redd.it", "preview.redd.it", "imgur.com"}

# Non-English signals
NON_ENGLISH_CURRENCIES = re.compile(r"[€£¥₹₩]")
NON_ENGLISH_URL_PATHS = re.compile(r"/(?:fr|de|es|it|jp|nl|pt|pl|ru|ko|zh)/", re.IGNORECASE)
NON_ENGLISH_RETAILERS = {"philibertnet.com", "fantasywelt.de", "brettspielversand.de",
                         "philibert.com", "milan-spiele.de", "spieletaxi.de"}

# Multi-step / tag detection patterns
MULTISTEP_PATTERNS = re.compile(
    r"(subscribe|newsletter|sign\s*up|membership|member\s*only|"
    r"add\s*to\s*cart|log\s*in\s*to\s*see|register\s*to)",
    re.IGNORECASE,
)
COUPON_PATTERNS = re.compile(
    r"(coupon|promo\s*code|discount\s*code|use\s*code|code[:\s])",
    re.IGNORECASE,
)
LIMITED_PATTERNS = re.compile(r"(limited\s*(quantity|stock|time)|only\s*\d+\s*left|while\s*supplies)", re.IGNORECASE)
PREORDER_PATTERNS = re.compile(r"(pre.?order|preorder)", re.IGNORECASE)
BUNDLE_PATTERNS = re.compile(r"(bundle|\+.*\+|buy\s*\d+\s*get)", re.IGNORECASE)
USED_PATTERNS = re.compile(r"(used|open\s*box|like\s*new|pre.?owned)", re.IGNORECASE)

CLASSIFY_BATCH_SIZE = 20


def _detect_tags(title, url):
    """Detect deal tags from title and URL. Returns a list of tag strings."""
    tags = []

    # Non-English
    if NON_ENGLISH_CURRENCIES.search(title):
        tags.append("non-english")
    if url:
        from urllib.parse import urlparse
        domain = urlparse(url).netloc.lower().replace("www.", "")
        if domain in NON_ENGLISH_RETAILERS:
            if "non-english" not in tags:
                tags.append("non-english")
        if NON_ENGLISH_URL_PATHS.search(url):
            if "non-english" not in tags:
                tags.append("non-english")

    if MULTISTEP_PATTERNS.search(title):
        tags.append("multistep")
    if COUPON_PATTERNS.search(title):
        tags.append("coupon")
    if LIMITED_PATTERNS.search(title):
        tags.append("limited")
    if PREORDER_PATTERNS.search(title):
        tags.append("preorder")
    if BUNDLE_PATTERNS.search(title):
        tags.append("bundle")
    if USED_PATTERNS.search(title):
        tags.append("used")

    return tags


def _heuristic_classify(title, url):
    """Try to classify a post using regex heuristics.
    Returns (post_type, confidence) or (None, 0) for ambiguous."""
    title = title.strip()
    has_price = bool(PRICE_PATTERN.search(title))

    # 0. Non-English sites → other
    if url:
        from urllib.parse import urlparse
        domain = urlparse(url).netloc.lower().replace("www.", "")
        if domain in NON_ENGLISH_RETAILERS:
            return "other", 0.95
    if NON_ENGLISH_CURRENCIES.search(title) and not has_price:
        # Has € but no $ — likely non-English deal
        return "other", 0.85
    has_percent = bool(PERCENT_OFF.search(title))

    # 1. Meta tags
    if META_TAGS.search(title):
        return "meta", 0.95

    # 2. Discussion tags
    if DISCUSSION_TAGS.search(title):
        return "discussion", 0.95

    # 3. Question (ends with ? AND no price/retailer signals)
    if title.endswith("?") and not has_price:
        return "question", 0.9

    # 4. Question starters without price
    if QUESTION_STARTERS.match(title) and not has_price:
        return "question", 0.85

    # 5. Specific deal: [Retailer] Game $XX.XX
    if SPECIFIC_DEAL_BRACKET.search(title):
        return "specific_deal", 0.9

    # 6. Has price + percent off = likely specific deal
    if has_price and has_percent:
        return "specific_deal", 0.8

    # 7. Has just a price with a game-like title (no generic signals)
    if has_price and not GENERIC_SALE_SIGNALS.search(title):
        return "specific_deal", 0.7

    # 8. Generic sale signals
    if GENERIC_SALE_SIGNALS.search(title):
        return "generic_sale", 0.8

    # 9. Image-only post with no price
    if url:
        from urllib.parse import urlparse
        domain = urlparse(url).netloc.lower().replace("www.", "")
        if domain in IMAGE_DOMAINS and not has_price:
            # Image post with no price — could be a screenshot deal or other
            if has_percent:
                return "specific_deal", 0.6
            return "other", 0.6

    # 10. Percent off without explicit price — likely a deal mention
    if has_percent:
        return "specific_deal", 0.6

    # Ambiguous
    return None, 0


def _build_classify_prompt(deals_batch):
    """Build prompt for Claude to classify ambiguous posts."""
    summaries = []
    for d in deals_batch:
        summaries.append({
            "id": d["id"],
            "title": d["title"],
            "url": d["url"],
            "has_price": bool(PRICE_PATTERN.search(d["title"])),
        })

    return f"""Classify each Reddit post from r/boardgamedeals into one of these categories:

- specific_deal: A specific board game at a specific discounted price from a specific retailer
- generic_sale: A sale/event involving multiple games or vague pricing (still deal-related)
- discussion: Community discussion about deals, strategies, recommendations
- question: Someone asking a question
- meta: Subreddit meta posts, mod announcements
- other: Screenshots without context, memes, non-deal content

Posts to classify:
{json.dumps(summaries, indent=2)}

Return ONLY a JSON array, no markdown:
[{{"id": 1, "post_type": "specific_deal"}}, ...]"""


def classify_deals(config, conn):
    """Classify all unclassified deals. Heuristics first, LLM for ambiguous."""
    deals = db.get_unclassified_deals(conn)
    if not deals:
        logger.info("No deals need classification")
        return {"classified": 0, "heuristic": 0, "llm": 0}

    logger.info(f"Classifying {len(deals)} deals...")

    stats = {"classified": 0, "heuristic": 0, "llm": 0}
    ambiguous = []

    # Tier 1: heuristics
    for deal in deals:
        post_type, confidence = _heuristic_classify(deal["title"], deal["url"])
        tags = _detect_tags(deal["title"], deal["url"])
        tags_str = ",".join(tags) if tags else None

        if post_type and confidence >= 0.6:
            db.update_deal_fields(conn, deal["id"], post_type=post_type, tags=tags_str)
            stats["classified"] += 1
            stats["heuristic"] += 1
            tag_info = f" tags=[{tags_str}]" if tags_str else ""
            logger.info(f"  #{deal['id']}: {post_type} (heuristic, {confidence:.0%}){tag_info} — {deal['title'][:50]}")
        else:
            ambiguous.append(deal)

    logger.info(f"Heuristics classified {stats['heuristic']}/{len(deals)}, {len(ambiguous)} ambiguous")

    # Tier 2: LLM for ambiguous
    if ambiguous:
        claude_cmd = config.get("self_heal", {}).get("claude_code_path", "claude")
        model = config.get("models", {}).get("classify", "haiku")
        cwd = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

        for i in range(0, len(ambiguous), CLASSIFY_BATCH_SIZE):
            batch = ambiguous[i:i + CLASSIFY_BATCH_SIZE]
            prompt = _build_classify_prompt(batch)

            try:
                cmd = [claude_cmd, "--print", "-p", prompt]
                if model:
                    cmd.extend(["--model", model])

                result = subprocess.run(
                    cmd, cwd=cwd, capture_output=True, text=True, timeout=60,
                )

                if result.returncode != 0:
                    logger.warning(f"Claude classify failed: {result.stderr[:200]}")
                    continue

                updates = parse_claude_json(result.stdout)
                if updates:
                    for item in updates:
                        deal_id = item.get("id")
                        post_type = item.get("post_type")
                        if deal_id and post_type:
                            db.update_deal_fields(conn, deal_id, post_type=post_type)
                            stats["classified"] += 1
                            stats["llm"] += 1
                            logger.info(f"  #{deal_id}: {post_type} (LLM)")

            except subprocess.TimeoutExpired:
                logger.warning("Claude classify timed out")
            except FileNotFoundError:
                logger.error(f"Claude Code CLI not found at '{claude_cmd}'")
                break

    logger.info(f"Classification complete: {stats}")
    return stats
