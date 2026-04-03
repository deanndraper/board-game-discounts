# Board Game Discounts

Python CLI tool that monitors r/boardgamedeals via RSS, verifies deals on retailer sites, stores data in SQLite, and publishes a static HTML page via GitHub Pages.

## Project Structure

- `main.py` — CLI entry point (run, fetch, verify, generate, publish, status)
- `bgd/` — Python package
  - `db.py` — SQLite schema and CRUD operations (deals.db)
  - `rss.py` — Reddit RSS feed parser
  - `verify.py` — Deal verification (HTTP check + page content analysis)
  - `html_gen.py` — Jinja2 HTML generator
  - `publisher.py` — Git commit/push for GitHub Pages
  - `logger.py` — Rotating file logger
  - `self_heal.py` — Error triage via Claude Code CLI
- `config.yaml` — All configuration
- `templates/index.html.j2` — HTML template
- `docs/` — GitHub Pages output
- `logs/bgd.log` — Application log

## Commands

```
python main.py run        # Full pipeline
python main.py fetch      # Fetch new deals from RSS
python main.py verify     # Verify all active/unverified deals
python main.py generate   # Regenerate HTML
python main.py publish    # Git commit & push docs/
python main.py status     # Show DB stats
```

## Key Design Patterns

- **Every run re-verifies all active deals**, not just new ones
- Deal statuses: unverified → active → expired/sold_out
- Verification uses escalating methods: HTTP status check → full page content analysis
- After 3 consecutive verification failures, a deal is marked expired
- Self-healing: on errors, Claude Code CLI is invoked to diagnose and fix

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
