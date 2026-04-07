"""Fetch BGG data for specific games."""
import requests
import json

GAMES = [172933, 318084, 1897, 412194, 276225, 382689, 418028]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json",
}


def try_endpoints(bgg_id):
    urls = [
        f"https://boardgamegeek.com/api/geekitems/thing/{bgg_id}",
        f"https://boardgamegeek.com/api/things/{bgg_id}",
        f"https://api.geekdo.com/api/things/{bgg_id}",
    ]
    for url in urls:
        try:
            resp = requests.get(url, headers=HEADERS, timeout=15)
            print(f"  {url} -> {resp.status_code}: {resp.text[:300]}")
        except Exception as e:
            print(f"  {url} -> ERROR: {e}")


if __name__ == "__main__":
    print("Testing endpoints for BGG ID 172933:")
    try_endpoints(172933)
