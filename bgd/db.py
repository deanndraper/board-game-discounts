import sqlite3
import os
from datetime import datetime


DB_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "deals.db")

SCHEMA = """
CREATE TABLE IF NOT EXISTS deals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    reddit_post_id TEXT UNIQUE,
    title TEXT NOT NULL,
    url TEXT,
    retailer TEXT,
    original_price REAL,
    sale_price REAL,
    discount_pct REAL,
    game_name TEXT,
    posted_at DATETIME,
    discovered_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    last_verified_at DATETIME,
    status TEXT DEFAULT 'unverified',
    verification_failures INTEGER DEFAULT 0,
    expires_at DATETIME,
    notes TEXT
);

CREATE TABLE IF NOT EXISTS verification_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    deal_id INTEGER NOT NULL,
    checked_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    status_before TEXT,
    status_after TEXT,
    method TEXT,
    details TEXT,
    FOREIGN KEY (deal_id) REFERENCES deals(id)
);

CREATE TABLE IF NOT EXISTS run_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at DATETIME,
    finished_at DATETIME,
    deals_found INTEGER DEFAULT 0,
    deals_verified INTEGER DEFAULT 0,
    deals_expired INTEGER DEFAULT 0,
    errors INTEGER DEFAULT 0,
    status TEXT,
    error_details TEXT
);
"""


def get_connection(db_path=None):
    path = db_path or DB_PATH
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db(db_path=None):
    conn = get_connection(db_path)
    conn.executescript(SCHEMA)
    conn.commit()
    return conn


def insert_deal(conn, deal: dict):
    """Insert a new deal. Returns the row id or None if it already exists."""
    try:
        cursor = conn.execute("""
            INSERT INTO deals (reddit_post_id, title, url, retailer,
                               original_price, sale_price, discount_pct,
                               game_name, posted_at, discovered_at, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'unverified')
        """, (
            deal.get("reddit_post_id"),
            deal.get("title"),
            deal.get("url"),
            deal.get("retailer"),
            deal.get("original_price"),
            deal.get("sale_price"),
            deal.get("discount_pct"),
            deal.get("game_name"),
            deal.get("posted_at"),
            datetime.utcnow().isoformat(),
        ))
        conn.commit()
        return cursor.lastrowid
    except sqlite3.IntegrityError:
        return None


def get_deals_to_verify(conn, max_age_hours=24):
    """Get all deals that need verification: unverified OR active deals
    that haven't been checked recently."""
    return conn.execute("""
        SELECT * FROM deals
        WHERE status = 'unverified'
           OR (status = 'active'
               AND (last_verified_at IS NULL
                    OR last_verified_at < datetime('now', ? || ' hours')))
        ORDER BY discovered_at DESC
    """, (f"-{max_age_hours}",)).fetchall()


def get_all_active_deals(conn):
    """Get all active deals for re-verification every run."""
    return conn.execute("""
        SELECT * FROM deals
        WHERE status IN ('unverified', 'active')
        ORDER BY discovered_at DESC
    """).fetchall()


def update_deal_status(conn, deal_id: int, status: str,
                       last_verified_at=None, verification_failures=None,
                       sale_price=None, original_price=None, discount_pct=None,
                       expires_at=None, notes=None):
    """Update deal status and related fields."""
    fields = ["status = ?"]
    values = [status]

    if last_verified_at is not None:
        fields.append("last_verified_at = ?")
        values.append(last_verified_at)
    if verification_failures is not None:
        fields.append("verification_failures = ?")
        values.append(verification_failures)
    if sale_price is not None:
        fields.append("sale_price = ?")
        values.append(sale_price)
    if original_price is not None:
        fields.append("original_price = ?")
        values.append(original_price)
    if discount_pct is not None:
        fields.append("discount_pct = ?")
        values.append(discount_pct)
    if expires_at is not None:
        fields.append("expires_at = ?")
        values.append(expires_at)
    if notes is not None:
        fields.append("notes = ?")
        values.append(notes)

    values.append(deal_id)
    conn.execute(f"UPDATE deals SET {', '.join(fields)} WHERE id = ?", values)
    conn.commit()


def log_verification(conn, deal_id: int, status_before: str, status_after: str,
                     method: str, details: str = None):
    conn.execute("""
        INSERT INTO verification_log (deal_id, checked_at, status_before,
                                       status_after, method, details)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (deal_id, datetime.utcnow().isoformat(), status_before,
          status_after, method, details))
    conn.commit()


def start_run(conn):
    cursor = conn.execute("""
        INSERT INTO run_log (started_at, status) VALUES (?, 'running')
    """, (datetime.utcnow().isoformat(),))
    conn.commit()
    return cursor.lastrowid


def finish_run(conn, run_id: int, deals_found=0, deals_verified=0,
               deals_expired=0, errors=0, status="success", error_details=None):
    conn.execute("""
        UPDATE run_log SET finished_at=?, deals_found=?, deals_verified=?,
                           deals_expired=?, errors=?, status=?, error_details=?
        WHERE id=?
    """, (datetime.utcnow().isoformat(), deals_found, deals_verified,
          deals_expired, errors, status, error_details, run_id))
    conn.commit()


def get_active_deals_for_html(conn, limit=100):
    """Get active deals sorted by discount for HTML generation."""
    return conn.execute("""
        SELECT * FROM deals
        WHERE status = 'active'
        ORDER BY discount_pct DESC, discovered_at DESC
        LIMIT ?
    """, (limit,)).fetchall()


def get_db_stats(conn):
    stats = {}
    for status in ("active", "unverified", "expired", "sold_out"):
        row = conn.execute("SELECT COUNT(*) FROM deals WHERE status=?", (status,)).fetchone()
        stats[status] = row[0]
    row = conn.execute("SELECT COUNT(*) FROM deals").fetchone()
    stats["total"] = row[0]
    last_run = conn.execute("SELECT * FROM run_log ORDER BY id DESC LIMIT 1").fetchone()
    stats["last_run"] = dict(last_run) if last_run else None
    return stats
