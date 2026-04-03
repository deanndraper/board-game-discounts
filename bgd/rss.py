import re
import logging
from datetime import datetime
from urllib.parse import urlparse

import feedparser
import requests

logger = logging.getLogger("bgd")

KNOWN_RETAILERS = {
    "amazon.com": "Amazon",
    "target.com": "Target",
    "walmart.com": "Walmart",
    "miniaturemarket.com": "Miniature Market",
    "coolstuffinc.com": "CoolStuffInc",
    "gamenerdz.com": "GameNerdz",
    "boardgameatlas.com": "Board Game Atlas",
    "gamestop.com": "GameStop",
    "barnesandnoble.com": "Barnes & Noble",
    "cardhaus.com": "Cardhaus",
    "boardlandia.com": "Boardlandia",
    "shop.asmodee.com": "Asmodee",
}

PRICE_PATTERN = re.compile(r"\$(\d+(?:\.\d{2})?)")


def extract_retailer(url: str) -> str:
    if not url:
        return "Unknown"
    domain = urlparse(url).netloc.lower().replace("www.", "")
    for key, name in KNOWN_RETAILERS.items():
        if key in domain:
            return name
    return domain


def extract_prices(title: str):
    """Try to extract prices from the deal title. Returns (original, sale, discount_pct)."""
    prices = [float(p) for p in PRICE_PATTERN.findall(title)]
    if len(prices) >= 2:
        original = max(prices)
        sale = min(prices)
        discount = round((1 - sale / original) * 100, 1) if original > 0 else None
        return original, sale, discount
    elif len(prices) == 1:
        return None, prices[0], None
    return None, None, None


def extract_game_name(title: str) -> str:
    """Best-effort game name extraction from the deal title."""
    # Remove price patterns and common prefixes
    cleaned = PRICE_PATTERN.sub("", title)
    cleaned = re.sub(r"\[.*?\]", "", cleaned)  # Remove [tags]
    cleaned = re.sub(r"\(.*?\)", "", cleaned)   # Remove (parentheticals)
    cleaned = re.sub(r"\s*[-–—]\s*.*$", "", cleaned)  # Remove trailing dash sections
    cleaned = cleaned.strip(" -–—,;:")
    return cleaned if cleaned else title


def fetch_deals(config: dict) -> list[dict]:
    """Fetch deals from the Reddit RSS feed."""
    reddit_cfg = config.get("reddit", {})
    feed_url = reddit_cfg.get("feed_url", "https://www.reddit.com/r/boardgamedeals/new/.rss")
    max_posts = reddit_cfg.get("max_posts", 50)

    logger.info(f"Fetching RSS feed: {feed_url}")
    # Pre-fetch with proper User-Agent since Reddit blocks default feedparser UA
    resp = requests.get(feed_url, headers={"User-Agent": "BoardGameDeals/1.0"}, timeout=15)
    feed = feedparser.parse(resp.text)

    if feed.bozo:
        logger.warning(f"RSS feed parse warning: {feed.bozo_exception}")

    deals = []
    for entry in feed.entries[:max_posts]:
        # The link in the RSS entry is to the Reddit post.
        # The actual deal URL is often in the post content or the link itself.
        reddit_url = entry.get("link", "")
        post_id = entry.get("id", reddit_url)

        # Try to find the actual deal URL from the content
        deal_url = reddit_url
        content = entry.get("content", [{}])[0].get("value", "") if entry.get("content") else ""
        if not content:
            content = entry.get("summary", "")

        # Look for external links in content
        link_match = re.search(r'href="(https?://(?!(?:www\.)?reddit\.com)[^"]+)"', content)
        if link_match:
            deal_url = link_match.group(1)

        title = entry.get("title", "")
        original, sale, discount = extract_prices(title)
        retailer = extract_retailer(deal_url)
        game_name = extract_game_name(title)

        posted_at = None
        if hasattr(entry, "published_parsed") and entry.published_parsed:
            posted_at = datetime(*entry.published_parsed[:6]).isoformat()
        elif hasattr(entry, "updated_parsed") and entry.updated_parsed:
            posted_at = datetime(*entry.updated_parsed[:6]).isoformat()

        deals.append({
            "reddit_post_id": post_id,
            "title": title,
            "url": deal_url,
            "retailer": retailer,
            "original_price": original,
            "sale_price": sale,
            "discount_pct": discount,
            "game_name": game_name,
            "posted_at": posted_at,
        })

    logger.info(f"Parsed {len(deals)} deals from RSS feed")
    return deals
