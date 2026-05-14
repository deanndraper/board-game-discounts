BEGIN TRANSACTION;
CREATE TABLE deal_notifications (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    deal_id INTEGER NOT NULL,
    recipient_id INTEGER NOT NULL,
    notified_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    method TEXT,
    FOREIGN KEY (deal_id) REFERENCES deals(id),
    FOREIGN KEY (recipient_id) REFERENCES recipients(id),
    UNIQUE(deal_id, recipient_id)
);
CREATE TABLE deals (
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
    notes TEXT
, tags TEXT);