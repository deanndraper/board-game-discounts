"""Generate a static HTML site from deals data using Tabulator.

Follows the same approach as digital-board-gamer: inline JSON data,
Tabulator.js for interactive table, Inter font, hero header with stats.
"""

import json
import os
import logging
from datetime import datetime

from bgd import db

logger = logging.getLogger("bgd")

HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>Board Game Deals</title>
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">
    <link rel="stylesheet" href="https://unpkg.com/tabulator-tables@6/dist/css/tabulator_simple.min.css">
    <style>
        :root {
            --bg-dark: #0f172a;
            --bg-card: #1e293b;
            --bg-body: #f1f5f9;
            --accent: #6366f1;
            --accent-light: #818cf8;
            --green: #22c55e;
            --yellow: #eab308;
            --red: #ef4444;
            --text-primary: #1e293b;
            --text-muted: #64748b;
            --text-light: #cbd5e1;
        }
        * { box-sizing: border-box; margin: 0; padding: 0; }
        body {
            font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
            background: var(--bg-body);
            color: var(--text-primary);
            min-height: 100vh;
        }

        /* Hero header */
        .hero {
            background: linear-gradient(135deg, var(--bg-dark) 0%, #1e1b4b 50%, #312e81 100%);
            color: #fff;
            padding: 2.5rem 2rem 2rem;
            text-align: center;
        }
        .hero h1 {
            font-size: 2.2rem;
            font-weight: 700;
            letter-spacing: -0.02em;
            margin-bottom: 0.4rem;
        }
        .hero h1 span { color: var(--accent-light); }
        .hero p {
            color: var(--text-light);
            font-size: 1rem;
            font-weight: 400;
            max-width: 600px;
            margin: 0 auto;
        }

        /* Stats bar */
        .stats-bar {
            display: flex;
            justify-content: center;
            gap: 1px;
            background: rgba(255,255,255,0.1);
            border-radius: 12px;
            overflow: hidden;
            max-width: 700px;
            margin: 1.5rem auto 0;
        }
        .stats-bar .stat {
            flex: 1;
            padding: 1rem 0.5rem;
            text-align: center;
            background: rgba(255,255,255,0.05);
            transition: background 0.2s;
        }
        .stats-bar .stat:hover { background: rgba(255,255,255,0.1); }
        .stats-bar .stat strong {
            display: block;
            font-size: 1.5rem;
            font-weight: 700;
            color: #fff;
        }
        .stats-bar .stat span {
            font-size: 0.75rem;
            color: var(--text-light);
            text-transform: uppercase;
            letter-spacing: 0.05em;
        }

        /* Main content */
        .content {
            max-width: 1400px;
            margin: 0 auto;
            padding: 1.5rem;
        }

        /* Toolbar */
        .toolbar {
            display: flex;
            gap: 0.5rem;
            margin-bottom: 1rem;
            flex-wrap: wrap;
        }
        .toolbar button {
            padding: 0.5rem 1rem;
            border: none;
            border-radius: 6px;
            font-family: 'Inter', sans-serif;
            font-size: 0.8rem;
            font-weight: 500;
            cursor: pointer;
            transition: all 0.15s;
        }
        .btn-primary {
            background: var(--accent);
            color: #fff;
        }
        .btn-primary:hover { background: var(--accent-light); }
        .btn-secondary {
            background: #fff;
            color: var(--text-primary);
            box-shadow: 0 1px 2px rgba(0,0,0,0.08);
        }
        .btn-secondary:hover { background: #f8fafc; }

        /* Table container */
        #deal-table {
            background: #fff;
            border-radius: 12px;
            overflow: hidden;
            box-shadow: 0 1px 3px rgba(0,0,0,0.08), 0 4px 12px rgba(0,0,0,0.04);
        }

        /* Tabulator overrides */
        .tabulator {
            font-family: 'Inter', sans-serif;
            font-size: 0.85rem;
            border: none;
            background: transparent;
        }
        .tabulator .tabulator-header {
            background: var(--bg-dark);
            color: #e2e8f0;
            border-bottom: 2px solid var(--accent);
        }
        .tabulator .tabulator-header .tabulator-col {
            border-right: 1px solid rgba(255,255,255,0.08);
            background: transparent;
        }
        .tabulator .tabulator-header .tabulator-col .tabulator-col-content {
            padding: 10px 12px;
        }
        .tabulator .tabulator-header .tabulator-col .tabulator-col-title {
            font-weight: 600;
            font-size: 0.8rem;
            text-transform: uppercase;
            letter-spacing: 0.04em;
            color: #cbd5e1;
        }
        .tabulator .tabulator-header .tabulator-col .tabulator-header-filter {
            padding: 4px 8px 8px;
        }
        .tabulator .tabulator-header .tabulator-col .tabulator-header-filter input,
        .tabulator .tabulator-header .tabulator-col .tabulator-header-filter select {
            width: 100%;
            padding: 5px 8px;
            font-size: 0.78rem;
            font-family: 'Inter', sans-serif;
            border: 1px solid #e2e8f0;
            border-radius: 4px;
            background: #fff;
            color: var(--text-primary);
        }
        .tabulator .tabulator-header .tabulator-col .tabulator-header-filter input::placeholder {
            color: #94a3b8;
        }
        .tabulator-row .tabulator-cell {
            padding: 10px 12px;
            border-right: 1px solid #f1f5f9;
            border-bottom: 1px solid #f1f5f9;
        }
        .tabulator-row { background: #fff; }
        .tabulator-row.tabulator-row-even { background: #f8fafc; }
        .tabulator-row:hover { background: #eef2ff !important; }

        /* Discount pill */
        .discount-pill {
            display: inline-block;
            padding: 2px 8px;
            border-radius: 4px;
            font-weight: 600;
            font-size: 0.82rem;
        }
        .discount-high { background: #dcfce7; color: #166534; }
        .discount-mid { background: #fef9c3; color: #854d0e; }
        .discount-low { background: #e0e7ff; color: #3730a3; }

        /* Price styling */
        .price-sale {
            font-weight: 700;
            color: #166534;
            font-size: 0.9rem;
        }
        .price-original {
            text-decoration: line-through;
            color: var(--text-muted);
            font-size: 0.8rem;
        }

        /* Retailer tag */
        .retailer-tag {
            display: inline-block;
            padding: 2px 8px;
            border-radius: 12px;
            font-size: 0.78rem;
            font-weight: 500;
            background: #f1f5f9;
            color: var(--text-muted);
        }

        /* Status badge */
        .status-active { background: #dcfce7; color: #166534; padding: 2px 8px; border-radius: 4px; font-weight: 600; font-size: 0.78rem; }
        .status-expired { background: #fee2e2; color: #991b1b; padding: 2px 8px; border-radius: 4px; font-weight: 600; font-size: 0.78rem; }
        .status-sold_out { background: #fef9c3; color: #854d0e; padding: 2px 8px; border-radius: 4px; font-weight: 600; font-size: 0.78rem; }
        .status-unverified { background: #e0e7ff; color: #3730a3; padding: 2px 8px; border-radius: 4px; font-weight: 600; font-size: 0.78rem; }

        /* Link button */
        a.deal-link {
            display: inline-flex;
            align-items: center;
            justify-content: center;
            width: 28px;
            height: 28px;
            border-radius: 6px;
            background: #e0e7ff;
            color: var(--accent);
            text-decoration: none;
            font-size: 0.9rem;
            transition: all 0.15s;
        }
        a.deal-link:hover { background: var(--accent); color: #fff; }

        /* BGG link */
        a.bgg-link {
            color: var(--accent);
            text-decoration: none;
            font-weight: 600;
        }
        a.bgg-link:hover { text-decoration: underline; }

        /* Footer */
        .tabulator .tabulator-footer {
            background: #f8fafc;
            border-top: 1px solid #e2e8f0;
            font-size: 0.82rem;
            color: var(--text-muted);
        }
        .tabulator .tabulator-footer .tabulator-page {
            border-radius: 4px;
            margin: 0 2px;
        }
        .tabulator .tabulator-footer .tabulator-page.active {
            background: var(--accent);
            color: #fff;
        }
        footer {
            text-align: center;
            font-size: 0.78rem;
            color: var(--text-muted);
            padding: 1.5rem 0;
        }
        footer a { color: var(--accent); text-decoration: none; }
        footer a:hover { text-decoration: underline; }

        @media (max-width: 768px) {
            .hero { padding: 1.5rem 1rem 1.5rem; }
            .hero h1 { font-size: 1.5rem; }
            .stats-bar { flex-wrap: wrap; }
            .stats-bar .stat { min-width: 45%; }
            .content { padding: 1rem; }
        }
    </style>
</head>
<body>
    <div class="hero">
        <h1>Board Game <span>Deals</span></h1>
        <p>Live deals from r/boardgamedeals — verified and updated automatically</p>
        <div class="stats-bar">
            <div class="stat"><strong>__ACTIVE_COUNT__</strong><span>Active Deals</span></div>
            <div class="stat"><strong>__EXPIRED_COUNT__</strong><span>Expired</span></div>
            <div class="stat"><strong>__SOLD_OUT_COUNT__</strong><span>Sold Out</span></div>
            <div class="stat"><strong>__TOTAL_COUNT__</strong><span>Total Tracked</span></div>
        </div>
    </div>

    <div class="content">
        <div class="toolbar">
            <button class="btn-primary" onclick="table.download('csv', 'board_game_deals.csv')">Export CSV</button>
            <button class="btn-secondary" onclick="clearFilters()">Clear Filters</button>
            <button class="btn-secondary" onclick="showAll()">Show All Deals</button>
            <button class="btn-secondary" onclick="showActive()">Active Only</button>
        </div>

        <div id="deal-table"></div>
    </div>

    <footer>
        <p>Deals sourced from <a href="https://reddit.com/r/boardgamedeals">r/boardgamedeals</a> &middot; Updated __TIMESTAMP__</p>
    </footer>

    <script src="https://unpkg.com/tabulator-tables@6/dist/js/tabulator.min.js"></script>
    <script>
        var DATA = __TABLE_DATA__;
        var RETAILERS = __RETAILER_VALUES__;
        var STATUSES = {"active":"Active","expired":"Expired","sold_out":"Sold Out","unverified":"Unverified"};

        function discountClass(v) {
            if (v >= 40) return "discount-high";
            if (v >= 20) return "discount-mid";
            return "discount-low";
        }

        var table = new Tabulator("#deal-table", {
            data: DATA,
            layout: "fitColumns",
            pagination: true,
            paginationSize: 50,
            paginationSizeSelector: [25, 50, 100, true],
            movableColumns: true,
            placeholder: "No matching deals found",
            initialSort: [
                {column: "discount_pct", dir: "desc"}
            ],
            initialFilter: [
                {field: "status", type: "=", value: "active"}
            ],
            columns: [
                {
                    title: "Game",
                    field: "game_name",
                    widthGrow: 2,
                    minWidth: 160,
                    headerFilter: "input",
                    headerFilterPlaceholder: "Search",
                    formatter: function(cell) {
                        var row = cell.getRow().getData();
                        var name = cell.getValue() || row.title || "";
                        var bgg = row.bgg_id;
                        if (bgg) {
                            return '<a class="bgg-link" href="https://boardgamegeek.com/boardgame/' + bgg + '" target="_blank" title="View on BGG">' + name + '</a>';
                        }
                        return "<strong>" + name + "</strong>";
                    }
                },
                {
                    title: "Retailer",
                    field: "retailer",
                    width: 130,
                    headerFilter: "list",
                    headerFilterParams: {values: RETAILERS, clearable: true},
                    formatter: function(cell) {
                        return '<span class="retailer-tag">' + (cell.getValue() || "Unknown") + '</span>';
                    }
                },
                {
                    title: "Sale",
                    field: "sale_price",
                    width: 85,
                    hozAlign: "center",
                    sorter: "number",
                    formatter: function(cell) {
                        var v = cell.getValue();
                        if (v == null) return "";
                        return '<span class="price-sale">$' + v.toFixed(2) + '</span>';
                    }
                },
                {
                    title: "Was",
                    field: "original_price",
                    width: 85,
                    hozAlign: "center",
                    sorter: "number",
                    formatter: function(cell) {
                        var v = cell.getValue();
                        if (v == null) return "";
                        return '<span class="price-original">$' + v.toFixed(2) + '</span>';
                    }
                },
                {
                    title: "Discount",
                    field: "discount_pct",
                    width: 95,
                    hozAlign: "center",
                    sorter: "number",
                    headerFilter: "number",
                    headerFilterPlaceholder: "Min %",
                    headerFilterFunc: ">=",
                    formatter: function(cell) {
                        var v = cell.getValue();
                        if (v == null) return "";
                        return '<span class="discount-pill ' + discountClass(v) + '">' + Math.round(v) + '% off</span>';
                    }
                },
                {
                    title: "Status",
                    field: "status",
                    width: 100,
                    hozAlign: "center",
                    headerFilter: "list",
                    headerFilterParams: {values: STATUSES, clearable: true},
                    formatter: function(cell) {
                        var v = cell.getValue();
                        var cls = "status-" + (v || "unverified");
                        var label = STATUSES[v] || v;
                        return '<span class="' + cls + '">' + label + '</span>';
                    }
                },
                {
                    title: "Posted",
                    field: "posted_at",
                    width: 100,
                    sorter: "string",
                    formatter: function(cell) {
                        var v = cell.getValue();
                        if (!v) return "";
                        return v.substring(0, 10);
                    }
                },
                {
                    title: "",
                    field: "url",
                    width: 50,
                    hozAlign: "center",
                    headerSort: false,
                    formatter: function(cell) {
                        var url = cell.getValue();
                        if (url && url.indexOf("reddit.com") === -1 && url.indexOf("i.redd.it") === -1) {
                            return '<a class="deal-link" href="' + url + '" target="_blank" title="View deal">&#8599;</a>';
                        }
                        return "";
                    }
                }
            ]
        });

        function clearFilters() { table.clearHeaderFilter(); table.clearFilter(); }
        function showAll() { table.clearFilter(); }
        function showActive() { table.setFilter("status", "=", "active"); }
    </script>
</body>
</html>"""


def generate(conn, config: dict):
    """Generate static HTML from deals data."""
    html_cfg = config.get("html", {})
    output_dir = html_cfg.get("output_dir", "docs")
    max_deals = html_cfg.get("max_deals_shown", 200)

    # Get all deals (not just active) for the table
    deals = conn.execute("""
        SELECT * FROM deals ORDER BY discount_pct DESC, discovered_at DESC LIMIT ?
    """, (max_deals,)).fetchall()
    deals_list = [dict(d) for d in deals]

    # Stats
    stats = db.get_db_stats(conn)
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")

    # Retailer values for filter dropdown
    retailers = sorted({d["retailer"] for d in deals_list if d.get("retailer")})
    retailer_values = {r: r for r in retailers}

    logger.info(f"Generating HTML with {len(deals_list)} deals ({stats['active']} active)")

    html = HTML_TEMPLATE
    html = html.replace("__ACTIVE_COUNT__", str(stats["active"]))
    html = html.replace("__EXPIRED_COUNT__", str(stats["expired"]))
    html = html.replace("__SOLD_OUT_COUNT__", str(stats["sold_out"]))
    html = html.replace("__TOTAL_COUNT__", str(stats["total"]))
    html = html.replace("__TIMESTAMP__", now)
    html = html.replace("__TABLE_DATA__", json.dumps(deals_list))
    html = html.replace("__RETAILER_VALUES__", json.dumps(retailer_values))

    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, "index.html")
    with open(output_file, "w") as f:
        f.write(html)

    logger.info(f"HTML written to {output_file}")
    return output_file
