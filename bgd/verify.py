import re
import json
import logging
from datetime import datetime

import requests
from bs4 import BeautifulSoup

from bgd import db

logger = logging.getLogger("bgd")

SOLD_OUT_PATTERNS = [
    r"sold\s*out",
    r"out\s*of\s*stock",
    r"currently\s*unavailable",
    r"no\s*longer\s*available",
    r"discontinued",
    r"item\s*not\s*found",
]
SOLD_OUT_RE = re.compile("|".join(SOLD_OUT_PATTERNS), re.IGNORECASE)

EXPIRED_PATTERNS = [
    r"deal\s*(has\s*)?expired",
    r"sale\s*(has\s*)?ended",
    r"offer\s*(has\s*)?expired",
    r"promotion\s*(has\s*)?ended",
    r"price.*back\s*to",
]
EXPIRED_RE = re.compile("|".join(EXPIRED_PATTERNS), re.IGNORECASE)

SALE_END_PATTERNS = [
    r"(?:sale|deal|offer)\s*ends?\s*(\w+\s+\d+)",
    r"expires?\s*(\d{1,2}/\d{1,2}(?:/\d{2,4})?)",
    r"valid\s*(?:through|until)\s*(\w+\s+\d+(?:,?\s*\d{4})?)",
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}


def http_check(url: str, timeout: int = 30) -> dict:
    """Quick HTTP check — returns status info."""
    result = {"method": "http_check", "url": url}
    try:
        resp = requests.get(url, headers=HEADERS, timeout=timeout, allow_redirects=True)
        result["status_code"] = resp.status_code
        result["final_url"] = resp.url
        result["ok"] = resp.status_code < 400
        return result
    except requests.RequestException as e:
        result["ok"] = False
        result["error"] = str(e)
        return result


def browser_check(url: str, timeout: int = 30) -> dict:
    """Full page content check for price/stock/expiry signals."""
    result = {"method": "browser", "url": url}
    try:
        resp = requests.get(url, headers=HEADERS, timeout=timeout, allow_redirects=True)
        result["status_code"] = resp.status_code
        result["final_url"] = resp.url

        if resp.status_code >= 400:
            result["ok"] = False
            result["reason"] = f"HTTP {resp.status_code}"
            return result

        soup = BeautifulSoup(resp.text, "html.parser")
        text = soup.get_text(separator=" ", strip=True).lower()

        # Check for sold out
        if SOLD_OUT_RE.search(text):
            result["ok"] = False
            result["reason"] = "sold_out"
            return result

        # Check for expired deal
        if EXPIRED_RE.search(text):
            result["ok"] = False
            result["reason"] = "expired"
            return result

        # Try to find sale end date
        for pattern in SALE_END_PATTERNS:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                result["expires_hint"] = match.group(1)
                break

        # Try to extract current price from page
        price_elements = soup.select("[class*=price], [class*=Price], [data-price]")
        prices = []
        for el in price_elements:
            price_text = el.get_text(strip=True)
            price_match = re.search(r"\$(\d+(?:\.\d{2})?)", price_text)
            if price_match:
                prices.append(float(price_match.group(1)))

        if prices:
            result["prices_found"] = prices

        result["ok"] = True
        return result

    except requests.RequestException as e:
        result["ok"] = False
        result["error"] = str(e)
        return result


def verify_deal(conn, deal, config: dict) -> str:
    """Verify a single deal. Returns the new status."""
    verify_cfg = config.get("verification", {})
    timeout = verify_cfg.get("timeout_seconds", 30)
    max_failures = verify_cfg.get("max_failures", 3)
    methods = verify_cfg.get("methods", ["http_check", "browser"])

    deal_id = deal["id"]
    url = deal["url"]
    old_status = deal["status"]
    now = datetime.utcnow().isoformat()

    logger.info(f"Verifying deal {deal_id}: {deal['title'][:60]}...")

    # URLs that can't be verified as deal pages
    unverifiable_domains = ("reddit.com", "i.redd.it", "preview.redd.it")
    if any(d in (url or "") for d in unverifiable_domains):
        logger.info(f"  Deal {deal_id}: non-retailer URL ({url[:50]}) — marking active (unverifiable)")
        db.update_deal_status(conn, deal_id, "active", last_verified_at=now,
                              notes="No retailer link found — deal from Reddit post only")
        db.log_verification(conn, deal_id, old_status, "active",
                            "skip", '{"reason": "non_retailer_url"}')
        return "active"

    new_status = old_status
    details = {}
    http_ok = False

    # Step 1: HTTP check
    http_definitive_fail = False
    # Retailers that commonly return false 404s (bot-detection, URL rotation)
    false_404_domains = ("amazon.com", "walmart.com", "target.com")
    is_false_404_prone = any(d in (url or "") for d in false_404_domains)

    if "http_check" in methods:
        check = http_check(url, timeout)
        details["http_check"] = check
        if not check["ok"]:
            status_code = check.get("status_code", 0)
            if status_code in (404, 410) and not is_false_404_prone:
                new_status = "expired"
                http_definitive_fail = True
                logger.info(f"  Deal {deal_id}: HTTP {status_code} → expired")
            elif status_code in (404, 410) and is_false_404_prone:
                # Major retailers often return false 404s — don't treat as definitive
                logger.info(f"  Deal {deal_id}: HTTP {status_code} from {url[:40]} (false-404-prone retailer), will try browser")
            elif status_code in (403, 503):
                # Bot-blocked — don't count as failure, still try browser check
                logger.info(f"  Deal {deal_id}: HTTP {status_code} (likely bot-blocked), will try browser")
            else:
                # Genuine failure — increment counter but still try browser check
                logger.info(f"  Deal {deal_id}: HTTP {status_code or 'error'}, will try browser")
        else:
            http_ok = True

    # Step 2: Browser check (skip only if HTTP gave a definitive answer)
    if "browser" in methods and not http_definitive_fail:
        check = browser_check(url, timeout)
        details["browser"] = check
        if not check["ok"]:
            reason = check.get("reason", "unknown")
            if reason == "sold_out":
                new_status = "sold_out"
                logger.info(f"  Deal {deal_id}: sold out detected")
            elif reason == "expired":
                new_status = "expired"
                logger.info(f"  Deal {deal_id}: expired detected via browser")
            elif http_ok:
                # Browser check failed (timeout, blocked, etc.) but HTTP was fine.
                # Trust the HTTP check — mark as active.
                new_status = "active"
                logger.info(f"  Deal {deal_id}: browser check failed but HTTP OK → active")
            else:
                # Both checks failed — but if it's just connectivity/bot issues
                # (no definitive sold_out/expired signal), let the unreachable handler
                # below manage the failure count instead of immediately expiring.
                logger.info(f"  Deal {deal_id}: both checks inconclusive")
        else:
            new_status = "active"
            # Update price if we found one
            prices = check.get("prices_found", [])
            if prices:
                current_price = min(prices)
                if deal["sale_price"] and current_price > deal["sale_price"] * 1.2:
                    new_status = "expired"
                    logger.info(f"  Deal {deal_id}: price increased → expired")

    # If only HTTP check ran and passed, mark active
    if http_ok and new_status == old_status:
        new_status = "active"

    # If both checks were inconclusive (bot-blocked), handle gracefully
    if new_status == old_status and not http_ok and not http_definitive_fail:
        failures = deal["verification_failures"] + 1
        if failures >= max_failures:
            new_status = "expired"
            logger.info(f"  Deal {deal_id}: {failures} consecutive unreachable → expired")
        else:
            # Keep current status, just log the failure
            logger.info(f"  Deal {deal_id}: unreachable ({failures}/{max_failures}), keeping {old_status}")
            db.update_deal_status(conn, deal_id, old_status,
                                  last_verified_at=now,
                                  verification_failures=failures)
            db.log_verification(conn, deal_id, old_status, old_status,
                                "unreachable", json.dumps(details))
            return old_status

    # Update the deal
    update_kwargs = {
        "last_verified_at": now,
        "verification_failures": 0 if new_status == "active" else deal["verification_failures"],
    }

    # Set expiry if we found a hint
    expires_hint = details.get("browser", {}).get("expires_hint")
    if expires_hint:
        update_kwargs["notes"] = f"Sale ends: {expires_hint}"

    db.update_deal_status(conn, deal_id, new_status, **update_kwargs)
    db.log_verification(conn, deal_id, old_status, new_status,
                        "full", json.dumps(details))

    if new_status != old_status:
        logger.info(f"  Deal {deal_id}: {old_status} → {new_status}")

    return new_status


def verify_all(conn, config: dict) -> dict:
    """Verify all deals that need checking. Returns summary stats."""
    deals = db.get_all_active_deals(conn)
    logger.info(f"Verifying {len(deals)} deals...")

    stats = {"verified": 0, "expired": 0, "sold_out": 0, "active": 0, "errors": 0}

    for deal in deals:
        try:
            new_status = verify_deal(conn, deal, config)
            stats["verified"] += 1
            if new_status == "expired":
                stats["expired"] += 1
            elif new_status == "sold_out":
                stats["sold_out"] += 1
            elif new_status == "active":
                stats["active"] += 1
        except Exception as e:
            stats["errors"] += 1
            logger.error(f"Error verifying deal {deal['id']}: {e}", exc_info=True)

    logger.info(f"Verification complete: {stats}")
    return stats
