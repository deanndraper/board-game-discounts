"""One-off script to look up BGG IDs via DuckDuckGo."""
import requests
from bs4 import BeautifulSoup
import re
import time

DDG_SEARCH_URL = "https://html.duckduckgo.com/html/"
DDG_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
}

games = [
    ("Camel Up 2nd Edition", 4),
    ("Tokaido", 5),
    ("Star Wars Outer Rim", 9),
]

session = requests.Session()
session.headers.update(DDG_HEADERS)

for game_name, gid in games:
    query = "boardgamegeek.com " + game_name + " board game"
    resp = session.get(DDG_SEARCH_URL, params={"q": query}, timeout=10)
    soup = BeautifulSoup(resp.text, "html.parser")

    found = False
    for result in soup.select(".result__url"):
        url_text = result.get_text(strip=True)
        match = re.search(r"boardgamegeek\.com/boardgame/(\d+)", url_text)
        if match:
            bgg_id = int(match.group(1))
            print("FOUND id=" + str(gid) + " game=" + game_name + " bgg_id=" + str(bgg_id) + " url=" + url_text)
            found = True
            break

    if not found:
        for link in soup.select(".result__a"):
            href = link.get("href", "")
            bgg_match = re.search(r"boardgamegeek\.com%2Fboardgame%2F(\d+)", href)
            if not bgg_match:
                bgg_match = re.search(r"boardgamegeek\.com/boardgame/(\d+)", href)
            if bgg_match:
                bgg_id = int(bgg_match.group(1))
                text = link.get_text(strip=True)
                print("FOUND id=" + str(gid) + " game=" + game_name + " bgg_id=" + str(bgg_id) + " title=" + text)
                found = True
                break

    if not found:
        print("NOT FOUND id=" + str(gid) + " game=" + game_name)

    time.sleep(5)
