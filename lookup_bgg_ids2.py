"""Look up BGG IDs via BGG search and BGG XML API."""
import requests
import re
import time
from xml.etree import ElementTree

headers = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}
session = requests.Session()
session.headers.update(headers)

games = [
    ("Camel Up", 4),
    ("Tokaido", 5),
    ("Star Wars: Outer Rim", 9),
]

for game_name, gid in games:
    resp = session.get(
        "https://boardgamegeek.com/xmlapi2/search",
        params={"query": game_name, "type": "boardgame"},
        timeout=15
    )
    print("Status for " + game_name + ": " + str(resp.status_code))
    if resp.status_code == 200:
        root = ElementTree.fromstring(resp.text)
        for item in root.findall("item"):
            item_id = item.get("id")
            name_el = item.find("name")
            year_el = item.find("yearpublished")
            name_val = name_el.get("value") if name_el is not None else "?"
            year_val = year_el.get("value") if year_el is not None else "?"
            print("  id=" + str(item_id) + " name=" + name_val + " year=" + str(year_val))
    else:
        print("  Response: " + resp.text[:200])
    time.sleep(3)
