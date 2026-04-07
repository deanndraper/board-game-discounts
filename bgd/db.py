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
    bgg_id INTEGER,
    bgg_rating REAL,
    bgg_rank INTEGER,
    bgg_weight REAL,
    bgg_url TEXT,
    post_type TEXT,
    tags TEXT,
    notes TEXT
);

CREATE INDEX IF NOT EXISTS idx_deals_status ON deals(status);

CREATE TABLE IF NOT EXISTS recipients (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    email TEXT,
    cell TEXT,
    active INTEGER DEFAULT 1,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS deal_notifications (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    deal_id INTEGER NOT NULL,
    recipient_id INTEGER NOT NULL,
    notified_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    method TEXT,
    FOREIGN KEY (deal_id) REFERENCES deals(id),
    FOREIGN KEY (recipient_id) REFERENCES recipients(id),
    UNIQUE(deal_id, recipient_id)
);

CREATE INDEX IF NOT EXISTS idx_deal_notifications_deal ON deal_notifications(deal_id);
CREATE INDEX IF NOT EXISTS idx_deal_notifications_recipient ON deal_notifications(recipient_id);

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
    migrate_db(conn)
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
    """Get all active deals for re-verification every run.
    Only includes specific_deal posts (or not-yet-classified)."""
    return conn.execute("""
        SELECT * FROM deals
        WHERE status IN ('unverified', 'active')
          AND (post_type IS NULL OR post_type = 'specific_deal')
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


def get_deals_needing_enrichment(conn):
    """Get specific_deal posts with missing data."""
    return conn.execute("""
        SELECT * FROM deals
        WHERE status IN ('active', 'unverified')
          AND (post_type IS NULL OR post_type = 'specific_deal')
          AND (bgg_id IS NULL
               OR sale_price IS NULL
               OR original_price IS NULL
               OR discount_pct IS NULL
               OR url LIKE '%reddit.com%'
               OR url LIKE '%i.redd.it%')
        ORDER BY id
    """).fetchall()


def get_deals_for_deep_verify(conn):
    """Get specific_deal posts that need intelligent verification."""
    return conn.execute("""
        SELECT * FROM deals
        WHERE status IN ('active', 'unverified')
          AND (post_type IS NULL OR post_type = 'specific_deal')
        ORDER BY last_verified_at ASC NULLS FIRST
    """).fetchall()


def update_deal_fields(conn, deal_id: int, **kwargs):
    """Update arbitrary fields on a deal."""
    if not kwargs:
        return
    fields = []
    values = []
    for key, val in kwargs.items():
        if val is not None:
            fields.append(f"{key} = ?")
            values.append(val)
    if not fields:
        return
    values.append(deal_id)
    conn.execute(f"UPDATE deals SET {', '.join(fields)} WHERE id = ?", values)
    conn.commit()


def get_deals_needing_bgg_data(conn):
    """Get specific_deal posts with bgg_id but missing BGG stats."""
    return conn.execute("""
        SELECT * FROM deals
        WHERE bgg_id IS NOT NULL
          AND (bgg_rating IS NULL OR bgg_rank IS NULL OR bgg_weight IS NULL)
          AND (post_type IS NULL OR post_type = 'specific_deal')
        ORDER BY id
    """).fetchall()


def migrate_db(conn):
    """Add new columns if they don't exist (safe for existing DBs)."""
    cursor = conn.execute("PRAGMA table_info(deals)")
    columns = {row[1] for row in cursor.fetchall()}
    new_cols = {
        "bgg_id": "INTEGER",
        "bgg_rating": "REAL",
        "bgg_rank": "INTEGER",
        "bgg_weight": "REAL",
        "bgg_url": "TEXT",
        "post_type": "TEXT",
        "tags": "TEXT",
    }
    for col, col_type in new_cols.items():
        if col not in columns:
            conn.execute(f"ALTER TABLE deals ADD COLUMN {col} {col_type}")
    conn.commit()


def get_active_recipients(conn):
    """Get all active recipients."""
    return conn.execute("SELECT * FROM recipients WHERE active = 1").fetchall()


def get_unnotified_deals_for_recipient(conn, recipient_id):
    """Get active specific_deal posts that haven't been sent to this recipient."""
    return conn.execute("""
        SELECT d.* FROM deals d
        WHERE d.status = 'active'
          AND d.post_type = 'specific_deal'
          AND d.id NOT IN (
              SELECT dn.deal_id FROM deal_notifications dn
              WHERE dn.recipient_id = ?
          )
        ORDER BY d.discovered_at DESC
    """, (recipient_id,)).fetchall()


def record_notification(conn, deal_id, recipient_id, method):
    """Record that a deal notification was sent."""
    try:
        conn.execute("""
            INSERT INTO deal_notifications (deal_id, recipient_id, method)
            VALUES (?, ?, ?)
        """, (deal_id, recipient_id, method))
        conn.commit()
    except sqlite3.IntegrityError:
        pass  # already notified


def mark_deals_notified_bulk(conn, deal_ids, recipient_id, method):
    """Mark multiple deals as notified for a recipient."""
    for deal_id in deal_ids:
        record_notification(conn, deal_id, recipient_id, method)


def add_recipient(conn, name, email=None, cell=None):
    """Add a new recipient. Returns the row id."""
    cursor = conn.execute("""
        INSERT INTO recipients (name, email, cell) VALUES (?, ?, ?)
    """, (name, email, cell))
    conn.commit()
    return cursor.lastrowid


def get_unclassified_deals(conn):
    """Get deals that haven't been classified yet."""
    return conn.execute("""
        SELECT * FROM deals
        WHERE post_type IS NULL
        ORDER BY id
    """).fetchall()


def get_db_stats(conn):
    stats = {}
    for status in ("active", "unverified", "expired", "sold_out"):
        row = conn.execute("SELECT COUNT(*) FROM deals WHERE status=?", (status,)).fetchone()
        stats[status] = row[0]
    row = conn.execute("SELECT COUNT(*) FROM deals").fetchone()
    stats["total"] = row[0]
    # Post type breakdown
    for ptype in ("specific_deal", "generic_sale", "discussion", "question", "meta", "other"):
        row = conn.execute("SELECT COUNT(*) FROM deals WHERE post_type=?", (ptype,)).fetchone()
        stats[f"type_{ptype}"] = row[0]
    row = conn.execute("SELECT COUNT(*) FROM deals WHERE post_type IS NULL").fetchone()
    stats["type_unclassified"] = row[0]
    last_run = conn.execute("SELECT * FROM run_log ORDER BY id DESC LIMIT 1").fetchone()
    stats["last_run"] = dict(last_run) if last_run else None
    return stats
