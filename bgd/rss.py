import re
import logging
from datetime import datetime
from email.utils import parsedate
from urllib.parse import urlparse
from xml.etree import ElementTree

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
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    resp = requests.get(feed_url, headers=headers, timeout=15)
    if resp.status_code == 403 and "reddit.com" in feed_url:
        # Try old.reddit.com as fallback
        fallback_url = feed_url.replace("www.reddit.com", "old.reddit.com")
        logger.info(f"Got 403, retrying with old.reddit.com: {fallback_url}")
        resp = requests.get(fallback_url, headers=headers, timeout=15)
    resp.raise_for_status()

    # Parse Atom/RSS with stdlib ElementTree
    root = ElementTree.fromstring(resp.content)
    ns = {}
    # Detect namespace from root tag
    if root.tag.startswith("{"):
        ns_uri = root.tag[1:root.tag.index("}")]
        ns = {"atom": ns_uri}

    # Support both Atom (<entry>) and RSS 2.0 (<item>) feeds
    if ns:
        entries = root.findall(".//atom:entry", ns)
    else:
        entries = root.findall(".//item")

    def _text(el, tag, ns_map):
        child = el.find(tag, ns_map) if ns_map else el.find(tag)
        return child.text.strip() if child is not None and child.text else ""

    def _attr(el, tag, attr, ns_map):
        child = el.find(tag, ns_map) if ns_map else el.find(tag)
        return child.get(attr, "") if child is not None else ""

    def _parse_date(s):
        if not s:
            return None
        try:
            # Atom: 2024-01-01T12:00:00+00:00
            return datetime.fromisoformat(s.replace("Z", "+00:00")).replace(tzinfo=None).isoformat()
        except ValueError:
            pass
        try:
            t = parsedate(s)
            if t:
                return datetime(*t[:6]).isoformat()
        except Exception:
            pass
        return None

    deals = []
    for entry in entries[:max_posts]:
        if ns:
            # Atom feed
            post_id = _text(entry, "atom:id", ns)
            title = _text(entry, "atom:title", ns)
            reddit_url = _attr(entry, "atom:link[@rel='alternate']", "href", ns) or \
                         _attr(entry, "atom:link", "href", ns)
            content = _text(entry, "atom:content", ns) or _text(entry, "atom:summary", ns)
            posted_at = _parse_date(_text(entry, "atom:published", ns) or _text(entry, "atom:updated", ns))
        else:
            # RSS 2.0 feed
            post_id = _text(entry, "guid", {})
            title = _text(entry, "title", {})
            reddit_url = _text(entry, "link", {})
            content = _text(entry, "description", {})
            posted_at = _parse_date(_text(entry, "pubDate", {}))

        if not reddit_url:
            reddit_url = post_id

        # Extract all external links from content
        deal_url = reddit_url
        all_links = re.findall(r'href="(https?://(?!(?:www\.)?reddit\.com)[^"]+)"', content)

        non_retailer = {"boardgamegeek.com", "bgg.cc", "wikipedia.org", "imgur.com",
                        "youtube.com", "youtu.be", "twitter.com", "x.com",
                        "i.redd.it", "preview.redd.it", "v.redd.it"}

        if all_links:
            retailer_links = [u for u in all_links
                              if not any(nr in u.lower() for nr in non_retailer)]
            known_retailer_links = [u for u in retailer_links
                                    if extract_retailer(u) != urlparse(u).netloc.lower().replace("www.", "")]
            if known_retailer_links:
                deal_url = known_retailer_links[0]
            elif retailer_links:
                deal_url = retailer_links[0]
            elif all_links:
                deal_url = all_links[0]

        original, sale, discount = extract_prices(title)
        retailer = extract_retailer(deal_url)
        game_name = extract_game_name(title)

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
