# Board Game Discounts

Python CLI tool that monitors r/boardgamedeals via RSS, verifies deals on retailer sites, stores data in SQLite, and publishes a static HTML page via GitHub Pages.

## Project Structure

- `main.py` — CLI entry point
- `bgd/` — Python package
  - `db.py` — SQLite schema and CRUD operations (deals.db)
  - `rss.py` — Reddit RSS feed parser
  - `classify.py` — Two-tier post classification (heuristics + LLM)
  - `verify.py` — Deal verification (HTTP check + page content analysis)
  - `enrich.py` — Fill missing deal data via Claude CLI (BGG IDs, prices, URLs)
  - `bgg.py` — Fetch BGG ratings, ranks, weight via API or Claude CLI
  - `deep_verify.py` — Intelligent deal verification via Claude CLI
  - `html_gen.py` — Tabulator.js HTML generator (matches digital-board-gamer style)
  - `publisher.py` — Git commit/push for GitHub Pages
  - `logger.py` — Rotating file logger
  - `self_heal.py` — Error triage via Claude Code CLI
- `config.yaml` — All configuration (including model selection per step)
- `docs/` — GitHub Pages output
- `logs/bgd.log` — Application log

## Commands

```
python main.py run          # Full pipeline: fetch → classify → verify → generate → publish
python main.py fetch        # Fetch new deals from RSS
python main.py classify     # Classify posts (specific_deal, generic_sale, question, etc.)
python main.py verify       # Verify all active/unverified specific deals
python main.py enrich       # Fill missing data via Claude CLI + fetch BGG stats
python main.py bgg          # Fetch BGG ratings, ranks, weight
python main.py deep-verify  # Intelligent verification via Claude CLI
python main.py generate     # Regenerate HTML
python main.py publish      # Git commit & push docs/
python main.py status       # Show DB stats + classification breakdown
```

## Pipeline Flow

```
fetch → classify → verify → generate → publish
```

- **classify** filters non-deals (questions, discussions, generic sales) before downstream steps
- All LLM steps (enrich, bgg, deep-verify) only process `specific_deal` posts
- Every run re-verifies all active deals

## Model Selection

Each LLM step uses the cheapest model that can handle the task:
- `classify` / `enrich` / `bgg` → **haiku** (factual/classification tasks)
- `deep-verify` → **sonnet** (needs reasoning about retailer patterns)
- `self-heal` → **opus** (needs full coding ability)

Configured in `config.yaml` under `models:` section.

## Key Design Patterns

- **Post classification**: heuristics first (zero tokens), LLM only for ambiguous
- **Deal statuses**: unverified → active → expired/sold_out
- **Post types**: specific_deal, generic_sale, discussion, question, meta, other
- **Verification**: HTTP check → browser content analysis → graduated failure counting
- **Self-healing**: on errors, Claude Code CLI diagnoses, fixes, and re-runs
- **TODO.md workflow**: improvements suggested by AI, approved by owner before implementation

## Virtual Environment

```
source venv/bin/activate
```

## When Making Changes

- Always test with `python main.py run` after changes
- Log output goes to `logs/bgd.log` — check it for errors
- Database is `deals.db` in project root
- HTML output goes to `docs/index.html`
- If adding improvement ideas, put them in `TODO.md` for owner approval
