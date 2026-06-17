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
# Shipping threshold patterns like "$35+" to strip before price extraction
_SHIPPING_THRESHOLD_RE = re.compile(r"\$\d+\+\s*(orders?|shipping)?", re.IGNORECASE)


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
    title = _SHIPPING_THRESHOLD_RE.sub("", title)
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


NON_RETAILER_DOMAINS = {"boardgamegeek.com", "bgg.cc", "wikipedia.org", "imgur.com",
                        "youtube.com", "youtu.be", "twitter.com", "x.com",
                        "i.redd.it", "preview.redd.it", "v.redd.it", "slickdeals.net",
                        "reddit.com"}


def _extract_deal_url(post_url: str, content: str) -> str:
    """Find the best retailer URL from post URL and content text."""
    # href= links (HTML content)
    href_links = re.findall(r'href="(https?://[^"]+)"', content)
    # plain-text URLs (common in SlickDeals descriptions)
    plain_links = re.findall(r'(?<!["\'])https?://\S+', content)
    all_links = href_links + [u for u in plain_links if u not in href_links]

    # Filter noise domains
    candidates = [u for u in all_links
                  if not any(nd in u.lower() for nd in NON_RETAILER_DOMAINS)]

    known = [u for u in candidates
             if extract_retailer(u) != urlparse(u).netloc.lower().replace("www.", "")]
    if known:
        return known[0]
    if candidates:
        return candidates[0]
    # Fall back to the post URL if it's not a noise domain
    if not any(nd in post_url.lower() for nd in NON_RETAILER_DOMAINS):
        return post_url
    return post_url


def _parse_rss_content(content: bytes, max_posts: int, source_url: str = "") -> list[dict]:
    """Parse Atom or RSS 2.0 feed content into deal dicts."""
    root = ElementTree.fromstring(content)
    ns = {}
    if root.tag.startswith("{"):
        ns_uri = root.tag[1:root.tag.index("}")]
        ns = {"atom": ns_uri}

    entries = root.findall(".//atom:entry", ns) if ns else root.findall(".//item")

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
            post_id = _text(entry, "atom:id", ns)
            title = _text(entry, "atom:title", ns)
            post_url = _attr(entry, "atom:link[@rel='alternate']", "href", ns) or \
                       _attr(entry, "atom:link", "href", ns)
            body = _text(entry, "atom:content", ns) or _text(entry, "atom:summary", ns)
            posted_at = _parse_date(_text(entry, "atom:published", ns) or _text(entry, "atom:updated", ns))
        else:
            post_id = _text(entry, "guid", {})
            title = _text(entry, "title", {})
            post_url = _text(entry, "link", {})
            body = _text(entry, "description", {})
            posted_at = _parse_date(_text(entry, "pubDate", {}))

        if not post_url:
            post_url = post_id

        deal_url = _extract_deal_url(post_url, body)
        original, sale, discount = extract_prices(title)
        retailer = extract_retailer(deal_url)
        game_name = extract_game_name(title)

        deals.append({
            "reddit_post_id": post_id or post_url,
            "title": title,
            "url": deal_url,
            "retailer": retailer,
            "original_price": original,
            "sale_price": sale,
            "discount_pct": discount,
            "game_name": game_name,
            "posted_at": posted_at,
        })

    return deals


def _try_feed(url: str, max_posts: int) -> list[dict] | None:
    """Fetch and parse a single RSS/Atom feed URL. Returns None on failure."""
    headers = {"User-Agent": "board-game-discounts/1.0 (deal tracker script)"}
    try:
        resp = requests.get(url, headers=headers, timeout=15)
        if not resp.ok:
            logger.warning(f"Feed {url} returned {resp.status_code}")
            return None
        deals = _parse_rss_content(resp.content, max_posts, source_url=url)
        logger.info(f"Parsed {len(deals)} deals from {url}")
        return deals
    except Exception as exc:
        logger.warning(f"Feed {url} failed: {exc}")
        return None


def fetch_deals(config: dict) -> list[dict]:
    """Fetch deals from configured RSS feeds, trying each in order until one succeeds."""
    reddit_cfg = config.get("reddit", {})
    primary_url = reddit_cfg.get("feed_url", "https://www.reddit.com/r/boardgamedeals/new/.rss")
    alt_feeds = reddit_cfg.get("alternative_feeds", [])
    max_posts = reddit_cfg.get("max_posts", 50)

    for url in [primary_url] + list(alt_feeds):
        logger.info(f"Trying feed: {url}")
        deals = _try_feed(url, max_posts)
        if deals is not None:
            return deals

    raise RuntimeError("All configured RSS feeds failed — no deals fetched")
