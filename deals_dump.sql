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
INSERT INTO "deals" VALUES(1,'t3_1sbjwg4','[Board Haven Games] Joyride: Survival of the Fastest $39.99','https://boardhavengames.com/pages/daily-flash-sale','boardhavengames.com',59.99,39.99,33.3,'Joyride: Survival of the Fastest','2026-04-03T17:25:26','2026-04-03T23:36:49.028657','2026-05-13T08:14:03.394438','active',0,NULL,371183,7.25,1847,1.98,'https://boardgamegeek.com/boardgame/371183/joyride-survival-of-the-fastest','specific_deal',NULL,NULL);
INSERT INTO "deals" VALUES(2,'t3_1sbfa7x','(Many Realms) Blood on the Clocktower $89.99','https://www.manyrealms.com.au/blood-on-the-clocktower','manyrealms.com.au',NULL,89.99,NULL,'Blood on the Clocktower','2026-04-03T11:22:01','2026-04-03T23:36:49.028657','2026-05-13T08:14:03.394438','active',0,NULL,331571,8.61,29,3.73,'https://boardgamegeek.com/boardgame/331571/blood-on-the-clocktower','specific_deal',NULL,NULL);
INSERT INTO "deals" VALUES(3,'t3_1sbcnep','[Asmodee] Marvel United is on sale from 13.49 on Amazon','https://www.amazon.com/dp/B085BCWQL6?tag=boardgamelinks-20&linkCode=osi&th=1','amazon.com',NULL,13.49,NULL,'Marvel United','2026-04-03T08:21:05','2026-04-03T23:36:49.028657','2026-05-13T08:14:03.394438','active',0,NULL,285564,7.01,NULL,2.17,'https://boardgamegeek.com/boardgame/285564/marvel-united','specific_deal',NULL,NULL);
INSERT INTO "deals" VALUES(4,'t3_1sb6ztr','[Asmodee/IDW] Teenage Mutant Ninja Turtles: Shadows of the Past $34.99 (Amazon)','https://www.amazon.com/dp/B01N1SXOAM','amazon.com',64.99,34.99,46.1,'Teenage Mutant Ninja Turtles: Shadows of the Past','2026-04-03T02:27:43','2026-04-03T23:36:49.028657','2026-05-13T08:14:03.394438','active',0,NULL,228346,7.3,NULL,2.62,'https://boardgamegeek.com/boardgame/228346/teenage-mutant-ninja-turtles-shadows-of-the-past','specific_deal',NULL,NULL);
INSERT INTO "deals" VALUES(5,'t3_1sb5l7p','[Amazon] Ticket to Ride Europe (2025 edition) $34.99 (30% off)','https://www.amazon.com/dp/B0DT3GX7DX','amazon.com',49.99,34.99,30.0,'Ticket to Ride Europe','2026-04-03T01:23:53','2026-04-03T23:36:49.028657','2026-05-13T08:14:03.394438','active',0,NULL,14996,7.49,164,2.42,'https://boardgamegeek.com/boardgame/14996/ticket-to-ride-europe','specific_deal',NULL,NULL);
INSERT INTO "deals" VALUES(6,'t3_1sb1j3w','[GameNerdz] Paleo $29.99 (50% off retail $59.99)','https://www.gamenerdz.com/paleo','gamenerdz.com',59.99,29.99,50.0,'Paleo','2026-04-02T21:48:07','2026-04-02T23:54:30.826478','2026-05-13T08:14:03.394438','active',0,NULL,300531,7.66,172,2.34,'https://boardgamegeek.com/boardgame/300531/paleo','specific_deal',NULL,NULL);
INSERT INTO "deals" VALUES(7,'t3_1saz4h4','[Target] Rising Sun $29.99 (50% off)','https://www.target.com/p/rising-sun/-/A-53461900','target.com',59.99,29.99,50.0,'Rising Sun','2026-04-02T18:09:03','2026-04-02T23:54:30.826478','2026-05-13T08:14:03.394438','active',0,NULL,205896,7.86,127,3.55,'https://boardgamegeek.com/boardgame/205896/rising-sun','specific_deal',NULL,NULL);
INSERT INTO "deals" VALUES(8,'t3_1saywt9','[Amazon] Spirit Island $44.99 (40% off retail $74.99)','https://www.amazon.com/dp/B07HB9VRMY','amazon.com',74.99,44.99,40.0,'Spirit Island','2026-04-02T17:56:06','2026-04-02T23:54:30.826478','2026-05-13T08:14:03.394438','active',0,NULL,162886,8.06,12,3.86,'https://boardgamegeek.com/boardgame/162886/spirit-island','specific_deal',NULL,NULL);
INSERT INTO "deals" VALUES(9,'t3_1saoeqh','[Amazon] Kingdom Death Monster 1.5 $378 (~16% off $449)','https://www.amazon.com/dp/B07R8BXXMQ','amazon.com',449.0,378.0,15.8,'Kingdom Death: Monster','2026-04-02T11:01:40','2026-04-02T23:54:30.826478','2026-05-13T08:14:03.394438','active',0,NULL,55690,8.61,4,4.29,'https://boardgamegeek.com/boardgame/55690/kingdom-death-monster','specific_deal',NULL,NULL);
INSERT INTO "deals" VALUES(10,'t3_1saje1x','[Target] Wingspan $29.99 (50% off retail $60)','https://www.target.com/p/wingspan/-/A-77852685','target.com',60.0,29.99,50.0,'Wingspan','2026-04-02T06:08:12','2026-04-02T23:54:30.826478','2026-05-13T08:14:03.394438','active',0,NULL,266192,7.97,27,2.46,'https://boardgamegeek.com/boardgame/266192/wingspan','specific_deal',NULL,NULL);
INSERT INTO "deals" VALUES(11,'t3_1sai8hs','[Amazon] Suburbia $34.99 (42% off $59.99)','https://www.amazon.com/dp/B09QMM7QGF','amazon.com',59.99,34.99,41.7,'Suburbia','2026-04-02T04:48:13','2026-04-02T23:54:30.826478','2026-05-13T08:14:03.394438','active',0,NULL,123260,7.31,396,2.65,'https://boardgamegeek.com/boardgame/123260/suburbia','specific_deal',NULL,NULL);
INSERT INTO "deals" VALUES(12,'t3_1sa96pw','[coolstuffinc] Endless Winter: Paleoamericans $35.99 (55% off $79.99)','https://www.coolstuffinc.com/p/282693','coolstuffinc.com',79.99,35.99,55.0,'Endless Winter: Paleoamericans','2026-04-01T22:11:24','2026-04-01T22:22:17.813524','2026-05-13T08:14:03.394438','active',0,NULL,305096,7.87,100,3.65,'https://boardgamegeek.com/boardgame/305096/endless-winter-paleoamericans','specific_deal',NULL,NULL);
INSERT INTO "deals" VALUES(13,'t3_1sa5moo','[Amazon] 7 Wonders Duel $19.96 (33% off $29.99)','https://www.amazon.com/dp/B00REGY1XE','amazon.com',29.99,19.96,33.4,'7 Wonders Duel','2026-04-01T18:39:04','2026-04-01T22:22:17.813524','2026-05-13T08:14:03.394438','active',0,NULL,173346,8.08,3,2.26,'https://boardgamegeek.com/boardgame/173346/7-wonders-duel','specific_deal',NULL,NULL);
INSERT INTO "deals" VALUES(14,'t3_1s9y34z','[Walmart] Catan $27.00 (46% off $49.99)','https://www.walmart.com/ip/Catan/604448173','walmart.com',49.99,27.0,45.9,'Catan','2026-04-01T11:09:21','2026-04-01T22:22:17.813524','2026-05-13T08:14:03.394438','active',0,NULL,13,7.14,606,2.31,'https://boardgamegeek.com/boardgame/13/catan','specific_deal',NULL,NULL);
INSERT INTO "deals" VALUES(15,'t3_1s9r91a','[GameNerdz] Robinson Crusoe $23.99 (52% off $49.99)','https://www.gamenerdz.com/robinson-crusoe-adventures-on-the-cursed-island','gamenerdz.com',49.99,23.99,52.0,'Robinson Crusoe: Adventures on the Cursed Island','2026-04-01T04:15:36','2026-04-01T22:22:17.813524','2026-05-13T08:14:03.394438','active',0,NULL,121921,7.74,47,3.73,'https://boardgamegeek.com/boardgame/121921/robinson-crusoe-adventures-on-the-cursed-island','specific_deal',NULL,NULL);
INSERT INTO "deals" VALUES(16,'t3_1s9p75c','[Amazon] Imperial Settlers $16.49 (45% off $29.99)','https://www.amazon.com/dp/B00LGGH11M','amazon.com',29.99,16.49,45.0,'Imperial Settlers','2026-04-01T01:52:45','2026-04-01T22:22:17.813524','2026-05-13T08:14:03.394438','active',0,NULL,154203,7.27,452,2.52,'https://boardgamegeek.com/boardgame/154203/imperial-settlers','specific_deal',NULL,NULL);
INSERT INTO "deals" VALUES(17,'t3_1s9i6tl','[Amazon] Everdell $34.99 (42% off $59.99)','https://www.amazon.com/dp/B07JVJPWKF','amazon.com',59.99,34.99,41.7,'Everdell','2026-03-31T18:42:44','2026-03-31T22:03:01.012474','2026-05-13T08:14:03.394438','active',0,NULL,199792,7.77,59,2.81,'https://boardgamegeek.com/boardgame/199792/everdell','specific_deal',NULL,NULL);
INSERT INTO "deals" VALUES(18,'t3_1s9fzg9','[Target] Betrayal at House on the Hill $22.49 (25% off $29.99)','https://www.target.com/p/betrayal-at-house-on-the-hill-3rd-edition/-/A-90127298','target.com',29.99,22.49,25.1,'Betrayal at House on the Hill','2026-03-31T16:26:56','2026-03-31T22:03:01.012474','2026-05-13T08:14:03.394438','active',0,NULL,10547,6.99,NULL,2.67,'https://boardgamegeek.com/boardgame/10547/betrayal-at-house-on-the-hill','specific_deal',NULL,NULL);
INSERT INTO "deals" VALUES(19,'t3_1s97nq5','[Miniature Market] Root $39.99 (43% off $69.99)','https://www.miniaturemarket.com/lel01000.html','miniaturemarket.com',69.99,39.99,42.9,'Root','2026-03-31T08:32:16','2026-03-31T22:03:01.012474','2026-05-13T08:14:03.394438','active',0,NULL,237182,8.07,14,3.56,'https://boardgamegeek.com/boardgame/237182/root','specific_deal',NULL,NULL);
INSERT INTO "deals" VALUES(20,'t3_1s93dvh','[Walmart] Pandemic $16.00 (47% off $29.99)','https://www.walmart.com/ip/Pandemic-Board-Game/35892330','walmart.com',29.99,16.0,46.6,'Pandemic','2026-03-31T04:41:29','2026-03-31T22:03:01.012474','2026-05-13T08:14:03.394438','active',0,NULL,30549,7.57,104,2.41,'https://boardgamegeek.com/boardgame/30549/pandemic','specific_deal',NULL,NULL);
INSERT INTO "deals" VALUES(21,'t3_1s8xiub','[Amazon] Great Western Trail $39.99 (50% off $79.99)','https://www.amazon.com/dp/B08P2ZFJG1','amazon.com',79.99,39.99,50.0,'Great Western Trail','2026-03-31T00:11:38','2026-03-31T22:03:01.012474','2026-05-13T08:14:03.394438','active',0,NULL,341169,8.26,8,3.7,'https://boardgamegeek.com/boardgame/341169/great-western-trail','specific_deal',NULL,NULL);
INSERT INTO "deals" VALUES(22,'t3_1s8twwb','[GameNerdz] Architects of the West Kingdom $29.99 (40% off $49.99)','https://www.gamenerdz.com/architects-of-the-west-kingdom','gamenerdz.com',49.99,29.99,40.0,'Architects of the West Kingdom','2026-03-30T20:38:43','2026-03-30T22:20:09.029985','2026-05-13T08:14:03.394438','active',0,NULL,236457,7.77,116,2.81,'https://boardgamegeek.com/boardgame/236457/architects-of-the-west-kingdom','specific_deal',NULL,NULL);
INSERT INTO "deals" VALUES(23,'t3_1s8q9ec','[Amazon] Gloomhaven $89.99 (40% off $149.99)','https://www.amazon.com/dp/B01LPHILLC','amazon.com',149.99,89.99,40.0,'Gloomhaven','2026-03-30T17:43:40','2026-03-30T22:20:09.029985','2026-05-13T08:14:03.394438','active',0,NULL,174430,8.77,1,3.86,'https://boardgamegeek.com/boardgame/174430/gloomhaven','specific_deal',NULL,NULL);
INSERT INTO "deals" VALUES(24,'t3_1s8miqw','[coolstuffinc] Terraforming Mars $44.99 (25% off $59.99)','https://www.coolstuffinc.com/p/218630','coolstuffinc.com',59.99,44.99,25.0,'Terraforming Mars','2026-03-30T14:02:37','2026-03-30T22:20:09.029985','2026-05-13T08:14:03.394438','active',0,NULL,167791,7.71,30,3.24,'https://boardgamegeek.com/boardgame/167791/terraforming-mars','specific_deal',NULL,NULL);
INSERT INTO "deals" VALUES(25,'t3_1s8jaiv','[Target] Azul $19.99 (43% off $34.99)','https://www.target.com/p/azul-board-game/-/A-52474200','target.com',34.99,19.99,42.8,'Azul','2026-03-30T11:22:09','2026-03-30T22:20:09.029985','2026-05-13T08:14:03.394438','active',0,NULL,230802,7.82,39,1.77,'https://boardgamegeek.com/boardgame/230802/azul','specific_deal',NULL,NULL);
INSERT INTO "deals" VALUES(26,'t3_1s8fz7m','[Amazon] Viticulture Essential Edition $34.99 (42% off $59.99)','https://www.amazon.com/dp/B00M0J3GOK','amazon.com',59.99,34.99,41.7,'Viticulture Essential Edition','2026-03-30T08:05:49','2026-03-30T22:20:09.029985','2026-05-13T08:14:03.394438','active',0,NULL,183394,8.1,22,2.89,'https://boardgamegeek.com/boardgame/183394/viticulture-essential-edition','specific_deal',NULL,NULL);
INSERT INTO "deals" VALUES(27,'t3_1s8c93m','[Miniature Market] Cascadia $24.99 (38% off $39.99)','https://www.miniaturemarket.com/smgcsc01.html','miniaturemarket.com',39.99,24.99,37.5,'Cascadia','2026-03-30T05:07:24','2026-03-30T22:20:09.029985','2026-05-13T08:14:03.394438','active',0,NULL,295947,7.67,56,1.89,'https://boardgamegeek.com/boardgame/295947/cascadia','specific_deal',NULL,NULL);
INSERT INTO "deals" VALUES(28,'t3_1s87l1p','[Amazon] Dominant Species $44.99 (44% off $79.99)','https://www.amazon.com/dp/B003PY8HMK','amazon.com',79.99,44.99,43.8,'Dominant Species','2026-03-30T01:08:43','2026-03-30T22:20:09.029985','2026-05-13T08:14:03.394438','active',0,NULL,62219,7.79,78,4.06,'https://boardgamegeek.com/boardgame/62219/dominant-species','specific_deal',NULL,NULL);
INSERT INTO "deals" VALUES(29,'t3_1s83zxc','[Amazon] Pandemic Legacy Season 1 $44.99 (43% off $79.99)','https://www.amazon.com/dp/B00VFZGMX4','amazon.com',79.99,44.99,43.7,'Pandemic Legacy: Season 1','2026-03-29T21:54:17','2026-03-29T22:08:02.195040','2026-05-13T08:14:03.394438','active',0,NULL,161936,8.63,2,2.84,'https://boardgamegeek.com/boardgame/161936/pandemic-legacy-season-1','specific_deal',NULL,NULL);
INSERT INTO "deals" VALUES(30,'t3_1s7zqxv','[Target] Codenames $12.99 (35% off $19.99)','https://www.target.com/p/codenames/-/A-51563279','target.com',19.99,12.99,35.0,'Codenames','2026-03-29T18:22:41','2026-03-29T22:08:02.195040','2026-05-13T08:14:03.394438','active',0,NULL,178900,7.59,197,1.25,'https://boardgamegeek.com/boardgame/178900/codenames','specific_deal',NULL,NULL);
INSERT INTO "deals" VALUES(31,'t3_1s7vl3q','[Amazon] Brass: Birmingham $44.99 (36% off $69.99)','https://www.amazon.com/dp/B07QVTQB1V','amazon.com',69.99,44.99,35.7,'Brass: Birmingham','2026-03-29T14:48:16','2026-03-29T22:08:02.195040','2026-05-13T08:14:03.394438','active',0,NULL,224517,8.66,5,3.9,'https://boardgamegeek.com/boardgame/224517/brass-birmingham','specific_deal',NULL,NULL);
INSERT INTO "deals" VALUES(32,'t3_1s7r8mk','[GameNerdz] Agricola $34.99 (42% off $59.99)','https://www.gamenerdz.com/agricola','gamenerdz.com',59.99,34.99,41.7,'Agricola','2026-03-29T11:11:52','2026-03-29T22:08:02.195040','2026-05-13T08:14:03.394438','active',0,NULL,31260,7.93,28,3.63,'https://boardgamegeek.com/boardgame/31260/agricola','specific_deal',NULL,NULL);
INSERT INTO "deals" VALUES(33,'t3_1s7nber','[Amazon] Ticket to Ride $29.99 (40% off $49.99)','https://www.amazon.com/dp/B00004W7RR','amazon.com',49.99,29.99,40.0,'Ticket to Ride','2026-03-29T07:43:21','2026-03-29T22:08:02.195040','2026-05-13T08:14:03.394438','active',0,NULL,9209,7.42,294,1.86,'https://boardgamegeek.com/boardgame/9209/ticket-to-ride','specific_deal',NULL,NULL);
INSERT INTO "deals" VALUES(34,'t3_1s7jptz','[Amazon] Scythe $49.99 (44% off $89.99)','https://www.amazon.com/dp/B07Y4LKRQ6','amazon.com',89.99,49.99,44.4,'Scythe','2026-03-29T04:18:07','2026-03-29T22:08:02.195040','2026-05-13T08:14:03.394438','active',0,NULL,169786,8.25,17,3.42,'https://boardgamegeek.com/boardgame/169786/scythe','specific_deal',NULL,NULL);
INSERT INTO "deals" VALUES(35,'t3_1s7f0py','[Amazon] Dominion $24.99 (38% off $39.99)','https://www.amazon.com/dp/B01LYLS8ZX','amazon.com',39.99,24.99,37.5,'Dominion','2026-03-29T00:54:13','2026-03-29T22:08:02.195040','2026-05-13T08:14:03.394438','active',0,NULL,36218,7.6,37,2.34,'https://boardgamegeek.com/boardgame/36218/dominion','specific_deal',NULL,NULL);
INSERT INTO "deals" VALUES(36,'t3_1s7awbc','[Amazon] Concordia $38.99 (35% off $59.99)','https://www.amazon.com/dp/B00J3GHQPQ','amazon.com',59.99,38.99,35.0,'Concordia','2026-03-28T21:18:54','2026-03-28T22:06:14.178754','2026-05-13T08:14:03.394438','active',0,NULL,111341,8.02,31,2.99,'https://boardgamegeek.com/boardgame/111341/concordia','specific_deal',NULL,NULL);
INSERT INTO "deals" VALUES(37,'t3_1s76m3q','[Amazon] Arkham Horror LCG Core Set $27.99 (44% off $49.99)','https://www.amazon.com/dp/B01J4NB6CO','amazon.com',49.99,27.99,44.0,'Arkham Horror: The Card Game','2026-03-28T17:51:42','2026-03-28T22:06:14.178754','2026-05-13T08:14:03.394438','active',0,NULL,215512,8.26,20,3.56,'https://boardgamegeek.com/boardgame/215512/arkham-horror-the-card-game','specific_deal',NULL,NULL);
CREATE TABLE recipients (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    contact TEXT NOT NULL,
    method TEXT NOT NULL CHECK(method IN ('imessage', 'email')),
    active INTEGER DEFAULT 1,
    min_discount_pct REAL DEFAULT 20.0,
    min_bgg_rating REAL DEFAULT 7.0,
    UNIQUE(contact, method)
);
CREATE TABLE run_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    step TEXT,
    status TEXT,
    deals_processed INTEGER,
    notes TEXT
);
INSERT INTO "run_log" VALUES(1,'2026-05-13T08:07:08.760423','full_pipeline','success',37,NULL);
INSERT INTO "run_log" VALUES(2,'2026-05-13T08:14:03.394438','full_pipeline','success',37,NULL);
CREATE TABLE verification_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    deal_id INTEGER NOT NULL,
    checked_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    method TEXT,
    result TEXT,
    http_status INTEGER,
    notes TEXT,
    FOREIGN KEY (deal_id) REFERENCES deals(id)
);
INSERT INTO "verification_log" VALUES(1,1,'2026-04-03T23:36:49.029089','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(2,2,'2026-04-03T23:36:49.029089','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(3,3,'2026-04-03T23:36:49.029089','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(4,4,'2026-04-03T23:36:49.029089','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(5,5,'2026-04-03T23:36:49.029089','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(6,6,'2026-04-03T23:36:49.029089','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(7,7,'2026-04-03T23:36:49.029089','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(8,8,'2026-04-03T23:36:49.029089','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(9,9,'2026-04-03T23:36:49.029089','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(10,10,'2026-04-03T23:36:49.029089','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(11,11,'2026-04-03T23:36:49.029089','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(12,12,'2026-04-03T23:36:49.029089','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(13,13,'2026-04-03T23:36:49.029089','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(14,14,'2026-04-03T23:36:49.029089','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(15,15,'2026-04-03T23:36:49.029089','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(16,16,'2026-04-03T23:36:49.029089','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(17,17,'2026-04-03T23:36:49.029089','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(18,18,'2026-04-03T23:36:49.029089','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(19,19,'2026-04-03T23:36:49.029089','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(20,20,'2026-04-03T23:36:49.029089','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(21,21,'2026-04-03T23:36:49.029089','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(22,22,'2026-04-03T23:36:49.029089','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(23,23,'2026-04-03T23:36:49.029089','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(24,24,'2026-04-03T23:36:49.029089','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(25,25,'2026-04-03T23:36:49.029089','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(26,26,'2026-04-03T23:36:49.029089','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(27,27,'2026-04-03T23:36:49.029089','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(28,28,'2026-04-03T23:36:49.029089','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(29,29,'2026-04-03T23:36:49.029089','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(30,30,'2026-04-03T23:36:49.029089','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(31,31,'2026-04-03T23:36:49.029089','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(32,32,'2026-04-03T23:36:49.029089','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(33,33,'2026-04-03T23:36:49.029089','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(34,34,'2026-04-03T23:36:49.029089','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(35,35,'2026-04-03T23:36:49.029089','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(36,36,'2026-04-03T23:36:49.029089','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(37,37,'2026-04-03T23:36:49.029089','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(38,1,'2026-04-03T23:40:15.374817','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(39,2,'2026-04-03T23:40:15.374817','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(40,3,'2026-04-03T23:40:15.374817','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(41,4,'2026-04-03T23:40:15.374817','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(42,5,'2026-04-03T23:40:15.374817','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(43,6,'2026-04-03T23:40:15.374817','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(44,7,'2026-04-03T23:40:15.374817','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(45,8,'2026-04-03T23:40:15.374817','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(46,9,'2026-04-03T23:40:15.374817','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(47,10,'2026-04-03T23:40:15.374817','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(48,11,'2026-04-03T23:40:15.374817','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(49,12,'2026-04-03T23:40:15.374817','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(50,13,'2026-04-03T23:40:15.374817','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(51,14,'2026-04-03T23:40:15.374817','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(52,15,'2026-04-03T23:40:15.374817','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(53,16,'2026-04-03T23:40:15.374817','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(54,17,'2026-04-03T23:40:15.374817','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(55,18,'2026-04-03T23:40:15.374817','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(56,19,'2026-04-03T23:40:15.374817','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(57,20,'2026-04-03T23:40:15.374817','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(58,21,'2026-04-03T23:40:15.374817','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(59,22,'2026-04-03T23:40:15.374817','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(60,23,'2026-04-03T23:40:15.374817','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(61,24,'2026-04-03T23:40:15.374817','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(62,25,'2026-04-03T23:40:15.374817','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(63,26,'2026-04-03T23:40:15.374817','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(64,27,'2026-04-03T23:40:15.374817','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(65,28,'2026-04-03T23:40:15.374817','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(66,29,'2026-04-03T23:40:15.374817','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(67,30,'2026-04-03T23:40:15.374817','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(68,31,'2026-04-03T23:40:15.374817','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(69,32,'2026-04-03T23:40:15.374817','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(70,33,'2026-04-03T23:40:15.374817','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(71,34,'2026-04-03T23:40:15.374817','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(72,35,'2026-04-03T23:40:15.374817','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(73,36,'2026-04-03T23:40:15.374817','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(74,37,'2026-04-03T23:40:15.374817','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(75,1,'2026-04-04T00:27:20.009617','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(76,2,'2026-04-04T00:27:20.009617','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(77,3,'2026-04-04T00:27:20.009617','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(78,4,'2026-04-04T00:27:20.009617','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(79,5,'2026-04-04T00:27:20.009617','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(80,6,'2026-04-04T00:27:20.009617','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(81,7,'2026-04-04T00:27:20.009617','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(82,8,'2026-04-04T00:27:20.009617','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(83,9,'2026-04-04T00:27:20.009617','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(84,10,'2026-04-04T00:27:20.009617','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(85,11,'2026-04-04T00:27:20.009617','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(86,12,'2026-04-04T00:27:20.009617','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(87,13,'2026-04-04T00:27:20.009617','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(88,14,'2026-04-04T00:27:20.009617','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(89,15,'2026-04-04T00:27:20.009617','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(90,16,'2026-04-04T00:27:20.009617','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(91,17,'2026-04-04T00:27:20.009617','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(92,18,'2026-04-04T00:27:20.009617','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(93,19,'2026-04-04T00:27:20.009617','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(94,20,'2026-04-04T00:27:20.009617','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(95,21,'2026-04-04T00:27:20.009617','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(96,22,'2026-04-04T00:27:20.009617','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(97,23,'2026-04-04T00:27:20.009617','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(98,24,'2026-04-04T00:27:20.009617','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(99,25,'2026-04-04T00:27:20.009617','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(100,26,'2026-04-04T00:27:20.009617','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(101,27,'2026-04-04T00:27:20.009617','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(102,28,'2026-04-04T00:27:20.009617','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(103,29,'2026-04-04T00:27:20.009617','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(104,30,'2026-04-04T00:27:20.009617','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(105,31,'2026-04-04T00:27:20.009617','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(106,32,'2026-04-04T00:27:20.009617','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(107,33,'2026-04-04T00:27:20.009617','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(108,34,'2026-04-04T00:27:20.009617','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(109,35,'2026-04-04T00:27:20.009617','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(110,36,'2026-04-04T00:27:20.009617','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(111,37,'2026-04-04T00:27:20.009617','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(112,1,'2026-04-04T05:24:33.879588','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(113,2,'2026-04-04T05:24:33.879588','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(114,3,'2026-04-04T05:24:33.879588','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(115,4,'2026-04-04T05:24:33.879588','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(116,5,'2026-04-04T05:24:33.879588','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(117,6,'2026-04-04T05:24:33.879588','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(118,7,'2026-04-04T05:24:33.879588','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(119,8,'2026-04-04T05:24:33.879588','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(120,9,'2026-04-04T05:24:33.879588','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(121,10,'2026-04-04T05:24:33.879588','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(122,11,'2026-04-04T05:24:33.879588','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(123,12,'2026-04-04T05:24:33.879588','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(124,13,'2026-04-04T05:24:33.879588','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(125,14,'2026-04-04T05:24:33.879588','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(126,15,'2026-04-04T05:24:33.879588','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(127,16,'2026-04-04T05:24:33.879588','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(128,17,'2026-04-04T05:24:33.879588','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(129,18,'2026-04-04T05:24:33.879588','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(130,19,'2026-04-04T05:24:33.879588','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(131,20,'2026-04-04T05:24:33.879588','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(132,21,'2026-04-04T05:24:33.879588','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(133,22,'2026-04-04T05:24:33.879588','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(134,23,'2026-04-04T05:24:33.879588','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(135,24,'2026-04-04T05:24:33.879588','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(136,25,'2026-04-04T05:24:33.879588','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(137,26,'2026-04-04T05:24:33.879588','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(138,27,'2026-04-04T05:24:33.879588','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(139,28,'2026-04-04T05:24:33.879588','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(140,29,'2026-04-04T05:24:33.879588','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(141,30,'2026-04-04T05:24:33.879588','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(142,31,'2026-04-04T05:24:33.879588','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(143,32,'2026-04-04T05:24:33.879588','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(144,33,'2026-04-04T05:24:33.879588','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(145,34,'2026-04-04T05:24:33.879588','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(146,35,'2026-04-04T05:24:33.879588','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(147,36,'2026-04-04T05:24:33.879588','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(148,37,'2026-04-04T05:24:33.879588','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(149,1,'2026-04-04T05:55:47.052571','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(150,2,'2026-04-04T05:55:47.052571','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(151,3,'2026-04-04T05:55:47.052571','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(152,4,'2026-04-04T05:55:47.052571','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(153,5,'2026-04-04T05:55:47.052571','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(154,6,'2026-04-04T05:55:47.052571','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(155,7,'2026-04-04T05:55:47.052571','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(156,8,'2026-04-04T05:55:47.052571','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(157,9,'2026-04-04T05:55:47.052571','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(158,10,'2026-04-04T05:55:47.052571','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(159,11,'2026-04-04T05:55:47.052571','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(160,12,'2026-04-04T05:55:47.052571','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(161,13,'2026-04-04T05:55:47.052571','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(162,14,'2026-04-04T05:55:47.052571','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(163,15,'2026-04-04T05:55:47.052571','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(164,16,'2026-04-04T05:55:47.052571','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(165,17,'2026-04-04T05:55:47.052571','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(166,18,'2026-04-04T05:55:47.052571','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(167,19,'2026-04-04T05:55:47.052571','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(168,20,'2026-04-04T05:55:47.052571','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(169,21,'2026-04-04T05:55:47.052571','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(170,22,'2026-04-04T05:55:47.052571','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(171,23,'2026-04-04T05:55:47.052571','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(172,24,'2026-04-04T05:55:47.052571','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(173,25,'2026-04-04T05:55:47.052571','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(174,26,'2026-04-04T05:55:47.052571','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(175,27,'2026-04-04T05:55:47.052571','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(176,28,'2026-04-04T05:55:47.052571','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(177,29,'2026-04-04T05:55:47.052571','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(178,30,'2026-04-04T05:55:47.052571','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(179,31,'2026-04-04T05:55:47.052571','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(180,32,'2026-04-04T05:55:47.052571','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(181,33,'2026-04-04T05:55:47.052571','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(182,34,'2026-04-04T05:55:47.052571','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(183,35,'2026-04-04T05:55:47.052571','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(184,36,'2026-04-04T05:55:47.052571','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(185,37,'2026-04-04T05:55:47.052571','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(186,1,'2026-05-13T08:07:08.760423','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(187,2,'2026-05-13T08:07:08.760423','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(188,3,'2026-05-13T08:07:08.760423','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(189,4,'2026-05-13T08:07:08.760423','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(190,5,'2026-05-13T08:07:08.760423','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(191,6,'2026-05-13T08:07:08.760423','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(192,7,'2026-05-13T08:07:08.760423','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(193,8,'2026-05-13T08:07:08.760423','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(194,9,'2026-05-13T08:07:08.760423','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(195,10,'2026-05-13T08:07:08.760423','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(196,11,'2026-05-13T08:07:08.760423','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(197,12,'2026-05-13T08:07:08.760423','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(198,13,'2026-05-13T08:07:08.760423','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(199,14,'2026-05-13T08:07:08.760423','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(200,15,'2026-05-13T08:07:08.760423','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(201,16,'2026-05-13T08:07:08.760423','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(202,17,'2026-05-13T08:07:08.760423','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(203,18,'2026-05-13T08:07:08.760423','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(204,19,'2026-05-13T08:07:08.760423','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(205,20,'2026-05-13T08:07:08.760423','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(206,21,'2026-05-13T08:07:08.760423','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(207,22,'2026-05-13T08:07:08.760423','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(208,23,'2026-05-13T08:07:08.760423','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(209,24,'2026-05-13T08:07:08.760423','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(210,25,'2026-05-13T08:07:08.760423','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(211,26,'2026-05-13T08:07:08.760423','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(212,27,'2026-05-13T08:07:08.760423','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(213,28,'2026-05-13T08:07:08.760423','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(214,29,'2026-05-13T08:07:08.760423','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(215,30,'2026-05-13T08:07:08.760423','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(216,31,'2026-05-13T08:07:08.760423','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(217,32,'2026-05-13T08:07:08.760423','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(218,33,'2026-05-13T08:07:08.760423','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(219,34,'2026-05-13T08:07:08.760423','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(220,35,'2026-05-13T08:07:08.760423','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(221,36,'2026-05-13T08:07:08.760423','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(222,37,'2026-05-13T08:07:08.760423','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(223,1,'2026-05-13T08:14:03.394438','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(224,2,'2026-05-13T08:14:03.394438','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(225,3,'2026-05-13T08:14:03.394438','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(226,4,'2026-05-13T08:14:03.394438','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(227,5,'2026-05-13T08:14:03.394438','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(228,6,'2026-05-13T08:14:03.394438','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(229,7,'2026-05-13T08:14:03.394438','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(230,8,'2026-05-13T08:14:03.394438','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(231,9,'2026-05-13T08:14:03.394438','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(232,10,'2026-05-13T08:14:03.394438','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(233,11,'2026-05-13T08:14:03.394438','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(234,12,'2026-05-13T08:14:03.394438','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(235,13,'2026-05-13T08:14:03.394438','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(236,14,'2026-05-13T08:14:03.394438','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(237,15,'2026-05-13T08:14:03.394438','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(238,16,'2026-05-13T08:14:03.394438','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(239,17,'2026-05-13T08:14:03.394438','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(240,18,'2026-05-13T08:14:03.394438','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(241,19,'2026-05-13T08:14:03.394438','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(242,20,'2026-05-13T08:14:03.394438','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(243,21,'2026-05-13T08:14:03.394438','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(244,22,'2026-05-13T08:14:03.394438','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(245,23,'2026-05-13T08:14:03.394438','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(246,24,'2026-05-13T08:14:03.394438','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(247,25,'2026-05-13T08:14:03.394438','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(248,26,'2026-05-13T08:14:03.394438','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(249,27,'2026-05-13T08:14:03.394438','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(250,28,'2026-05-13T08:14:03.394438','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(251,29,'2026-05-13T08:14:03.394438','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(252,30,'2026-05-13T08:14:03.394438','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(253,31,'2026-05-13T08:14:03.394438','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(254,32,'2026-05-13T08:14:03.394438','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(255,33,'2026-05-13T08:14:03.394438','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(256,34,'2026-05-13T08:14:03.394438','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(257,35,'2026-05-13T08:14:03.394438','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(258,36,'2026-05-13T08:14:03.394438','http_check','active',200,NULL);
INSERT INTO "verification_log" VALUES(259,37,'2026-05-13T08:14:03.394438','http_check','active',200,NULL);
DELETE FROM "sqlite_sequence";
INSERT INTO "sqlite_sequence" VALUES('deals',37);
INSERT INTO "sqlite_sequence" VALUES('run_log',2);
INSERT INTO "sqlite_sequence" VALUES('verification_log',240);
COMMIT;
