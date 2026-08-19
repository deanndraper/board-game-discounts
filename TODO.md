# Board Game Discounts — TODO

Items below are suggested by the daily pipeline review. Check the **Approved** box to authorize implementation.

---

## IMPROVEMENTS — 2026-08-19

### P1 · Correctness / Data Quality

- [ ] **Approved** — **Expand NON_TABLETOP_SIGNALS classifier regex** (`bgd/classify.py`)

  Four non-board-game deals are currently classified as `specific_deal` and shown to users:
  - #195 Acer Gateway Chromebook (`laptop|chromebook` not in regex)
  - #202 PC Games bundle (`\bpc game\b` pattern is case-sensitive — `(PC Games)` slips through)
  - #209 Hasbro Game Night for Nintendo Switch (digital/console game)
  - #211 FURINNO Study Table (`study table|desk|furniture` not in regex)
  
  Add to the existing `NON_TABLETOP_SIGNALS` pattern:
  ```
  \bchromebook\b | \blaptop\b | \btablet\b | study\s+table | office\s+desk |
  \bfurniture\b | (?:hd|4k)\s+digital | (?:nintendo\s+)?switch\s*\) |
  (pc|xbox|playstation|ps[45])\s+game\b
  ```
  Also make the `\bpc game\b` sub-pattern case-insensitive (`re.IGNORECASE` already applies to the whole pattern — confirm the flags are set).

- [ ] **Approved** — **Auto-expire old unverified reddit-only deals** (`bgd/verify.py`)

  75 of 121 active/unverified deals are **over 30 days old**. Many are Reddit-URL deals that can never be verified and so stay `active` forever. Add a rule: if a deal has `status = unverified` and `posted_at` is older than `max_age_days` (suggest 21 days, configurable in `config.yaml`), mark it `expired` during the verification pass. Active deals with verified retailer links should get a longer window (suggest 45 days).

- [ ] **Approved** — **Fix case-sensitivity in NON_TABLETOP_SIGNALS** (`bgd/classify.py`)

  `re.compile(..., re.IGNORECASE)` is already set on the pattern — but verify that `\bpc game\b` is included in the same compiled expression rather than a separate uncompiled check.

---

### P2 · Performance

- [ ] **Approved** — **Parallel deal verification** (`bgd/verify.py`)

  `verify_all()` is sequential: 74 deals × ~5 s each ≈ 6 minutes. Use `concurrent.futures.ThreadPoolExecutor(max_workers=8)` to run `verify_deal()` concurrently. Each deal touches a different URL so there are no shared-state conflicts; just pass the connection per-thread or use a lock around DB writes.

- [ ] **Approved** — **Persist venv between sessions** (root-level)

  The daily pipeline recreates `venv/` and reinstalls all packages on every run because the environment is ephemeral. Add a `setup.sh` that creates the venv only if it doesn't already exist, and note in CLAUDE.md to call it rather than sourcing venv directly. This saves ~30 s of pip install on every run.

---

### P3 · Reliability

- [ ] **Approved** — **Suppress duplicate self-heal entries in TODO.md** (`bgd/self_heal.py`)

  The self-heal system added 60+ near-identical failure notices between 2026-04-04 and 2026-06-17, making TODO.md unreadable. Add a check: before appending a new `NEEDS MANUAL REVIEW` block, read the last N lines of TODO.md and skip if an identical `Error:` line was added within the last 7 days. Alternatively, consolidate repeats into a single entry that updates a "Last seen" timestamp.

- [ ] **Approved** — **GameNerdz / rate-limited retailers: exponential backoff** (`bgd/verify.py`)

  Deals #185, #142, #125 (all GameNerdz) return 429 on every run and stay in limbo. Add a per-domain retry delay: on 429, wait 2 s and retry once before counting as inconclusive. Also consider a domain-level consecutive-failure counter separate from the deal-level one, so a bot-blocked domain doesn't eventually expire valid deals.

- [ ] **Approved** — **Reddit RSS: add a browser-like User-Agent** (`bgd/rss.py`)

  The 2026-04-04 through 2026-06-17 outage was caused by Reddit blocking the custom `User-Agent` (`board-game-discounts/1.0`). The current code still uses this. Replace with a real browser User-Agent (e.g., Firefox on macOS) as a primary and keep `board-game-discounts/1.0` only in a secondary header. The feed is currently working — this is a preventive fix.

---

### P4 · Enrichment / BGG Data

- [ ] **Approved** — **BGG Search API for missing IDs** (`bgd/bgg.py` or new `bgd/enrich.py` step)

  BGG exposes a free XML search API: `https://boardgamegeek.com/xmlapi2/search?query=<game>&type=boardgame`. This requires no auth and returns IDs without needing a Claude CLI call or web search. Use it as a first pass for games missing `bgg_id` before falling back to LLM. Confirm the ID by checking the returned game name matches reasonably well (fuzzy string match).

- [ ] **Approved** — **Deal value score column** (`bgd/db.py`, `bgd/html_gen.py`)

  Add a computed `value_score` = `bgg_rating × (discount_pct / 100)` for deals where both fields exist. Expose it as a sortable column in the HTML table (Tabulator already supports custom sorters). This gives users a quick "best bang for your buck" ordering. Store it in the DB so it's queryable.

- [ ] **Approved** — **Age cap for enrichment attempts** (`bgd/enrich.py`)

  Currently `enrich` runs on any active/unverified deal missing data, regardless of age. Deals over 14 days old are unlikely to benefit from enrichment (the game may have sold out by the time a user sees it). Add a `WHERE posted_at > datetime('now', '-14 days')` filter to the enrichment query.

---

### P5 · Housekeeping

- [ ] **Approved** — **Archive old self-heal log entries** (TODO.md)

  The April–June 2026 RSS-403 failure entries have been superseded — the feed is working again as of the current run. Archive or delete entries older than 90 days from TODO.md on each daily run. Suggested rule: self-heal entries older than 90 days are moved to `TODO_archive.md` instead of staying in the main file.

- [ ] **Approved** — **Index `posted_at` and `discovered_at` columns** (`bgd/db.py`)

  The age-based expiry and enrichment queries filter on `posted_at` which has no index. Add `CREATE INDEX IF NOT EXISTS idx_deals_posted_at ON deals(posted_at)` to the schema. With 224+ rows today this is cosmetic, but the table will grow.

- [ ] **Approved** — **Deduplicate deals at fetch time** (`bgd/rss.py` / `bgd/db.py`)

  Deals #196 and #201 are both "Elbow Room Games 3-in-1 Chess, Checkers & Backgammon" at nearly the same price from the same retailer, posted a few days apart. Add a soft-duplicate check at insert time: if a deal with the same `game_name` and `retailer` already exists and was posted within 7 days, log a warning and skip the insert (or merge into the existing record).

---

*Note: Items are for owner review — do not implement without checking the Approved box.*
