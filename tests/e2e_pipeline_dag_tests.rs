//! E2E tests for full multi-level DAG pipelines.
//!
//! Inspired by the project's external test-suite planning work.
//! Each scenario builds a realistic stream table DAG across 3-4 levels,
//! verifies initial population, then exercises INSERT / UPDATE / DELETE at
//! different points in the graph and confirms that changes cascade correctly
//! through every downstream layer.
//!
//! ## Scenarios
//!
//! ### 1  Nexmark Auction Pipeline  (`nx_*`)
//! Modelled on the Nexmark streaming benchmark (3 source tables).
//!
//! ```text
//! nx_persons ──────────────────────────────────────────────────────┐
//! nx_auctions ──────────┬─────────────────────────────────────────┐│
//! nx_bids ──────────────┘                                          ││
//!                        L1: nx_auction_bids                       ││
//!                             (bids JOIN auctions)                 ││
//!                                      │                           ││
//!                        L2a: nx_bidder_stats                      ││
//!                              (auction_bids JOIN persons, GROUP)  ││
//!                        L2b: nx_category_metrics                  ││
//!                              (auction_bids GROUP BY category)    ││
//! ```
//!
//! ### 2  E-commerce Order Analytics Pipeline  (`ec_*`)
//! Five source tables → four stream table layers.
//!
//! ```text
//! ec_categories ──────────────────────────────────────────────────┐
//! ec_products ─────────────────────────────────────────────────┐  │
//! ec_customers ──────────────────────────────────────────────┐  │  │
//! ec_orders ───────────────────────────────────────────────┐  │  │  │
//! ec_order_items ─────────────────────────────────────────┐│  │  │  │
//!                                                          ││  │  │  │
//!             L1a: ec_enriched_products ◄──────────────────┘│  │←─┘
//!                  (products JOIN categories)               │  │
//!             L1b: ec_order_headers ◄──────────────────────────┘
//!                  (orders JOIN customers)                  │
//!             L2:  ec_line_details ◄────────────────────────┘
//!                  (order_items JOIN ec_enriched_products)
//!             L3:  ec_order_totals
//!                  (ec_line_details JOIN ec_order_headers → SUM)
//!             L4:  ec_category_revenue
//!                  (ec_line_details GROUP BY category)
//! ```
//!
//! ### 3  IoT Telemetry Pipeline  (`tl_*`)
//! Three source tables → three stream table layers.
//!
//! ```text
//! tl_devices ─────────────────────────────────────────────────────┐
//! tl_sensors ──────────────────────────────────────────────────┐  │
//! tl_readings ────────────────────────────────────────────────┐│  │
//!                                                             ││  │
//!              L1: tl_sensor_readings ◄──────────────────────┘│  │
//!                   (readings JOIN sensors)                    │  │
//!              L2: tl_device_readings ◄─────────────────────────┘
//!                   (sensor_readings JOIN devices)             │
//!              L3: tl_device_stats ◄──────────────────────────┘
//!                   (aggregate device_readings by device)
//! ```
//!
//! Prerequisites: `./tests/build_e2e_image.sh`

mod e2e;

use e2e::E2eDb;

// ═══════════════════════════════════════════════════════════════════════════
// Scenario 1 — Nexmark Auction Pipeline
// ═══════════════════════════════════════════════════════════════════════════

/// Set up Nexmark base tables and seed data.
///
/// Schema mirrors the Nexmark benchmark specification:
/// persons, auctions, bids — the three core event streams.
async fn nx_setup_base_tables(db: &E2eDb) {
    // persons: people who sell and bid
    db.execute(
        "CREATE TABLE nx_persons (
            id      SERIAL PRIMARY KEY,
            name    TEXT NOT NULL,
            email   TEXT NOT NULL,
            city    TEXT NOT NULL
        )",
    )
    .await;

    // auctions: items listed for sale
    db.execute(
        "CREATE TABLE nx_auctions (
            id            SERIAL PRIMARY KEY,
            seller_id     INT NOT NULL REFERENCES nx_persons(id),
            category      TEXT NOT NULL,
            item_name     TEXT NOT NULL,
            reserve_price NUMERIC(10,2) NOT NULL
        )",
    )
    .await;

    // bids: bids placed on auctions
    db.execute(
        "CREATE TABLE nx_bids (
            id         SERIAL PRIMARY KEY,
            auction_id INT NOT NULL REFERENCES nx_auctions(id),
            bidder_id  INT NOT NULL REFERENCES nx_persons(id),
            price      NUMERIC(10,2) NOT NULL
        )",
    )
    .await;

    // Persons
    db.execute(
        "INSERT INTO nx_persons (id, name, email, city) VALUES
            (1, 'Alice',   'alice@nexmark.io',   'London'),
            (2, 'Bob',     'bob@nexmark.io',     'Paris'),
            (3, 'Charlie', 'charlie@nexmark.io', 'Berlin'),
            (4, 'Diana',   'diana@nexmark.io',   'London'),
            (5, 'Eve',     'eve@nexmark.io',     'Paris')",
    )
    .await;

    // Auctions
    db.execute(
        "INSERT INTO nx_auctions (id, seller_id, category, item_name, reserve_price) VALUES
            (1, 1, 'Electronics', 'Laptop Pro',        500.00),
            (2, 2, 'Electronics', 'Smartphone X',      200.00),
            (3, 3, 'Art',         'Oil Painting',      150.00),
            (4, 4, 'Art',         'Bronze Sculpture',  300.00),
            (5, 5, 'Books',       'Rare Manuscript',    50.00)",
    )
    .await;

    // Bids
    db.execute(
        "INSERT INTO nx_bids (id, auction_id, bidder_id, price) VALUES
            (1,  1, 2, 520.00),
            (2,  1, 3, 550.00),
            (3,  1, 4, 600.00),
            (4,  2, 1, 210.00),
            (5,  2, 3, 250.00),
            (6,  3, 2, 160.00),
            (7,  4, 1, 320.00),
            (8,  4, 3, 350.00),
            (9,  5, 2,  55.00),
            (10, 5, 4,  75.00)",
    )
    .await;
}

/// L1 — `nx_auction_bids`: join bids with auction metadata.
async fn nx_create_auction_bids(db: &E2eDb) {
    db.create_st(
        "nx_auction_bids",
        "SELECT
            b.id         AS bid_id,
            b.auction_id,
            b.bidder_id,
            b.price,
            a.seller_id,
            a.category,
            a.item_name,
            a.reserve_price
        FROM nx_bids b
        JOIN nx_auctions a ON a.id = b.auction_id",
        "1m",
        "DIFFERENTIAL",
    )
    .await;
}

/// L2a — `nx_bidder_stats`: aggregate bids per bidder, enriched with person name.
async fn nx_create_bidder_stats(db: &E2eDb) {
    db.create_st(
        "nx_bidder_stats",
        "SELECT
            p.id             AS person_id,
            p.name           AS bidder_name,
            p.city,
            COUNT(ab.bid_id) AS total_bids,
            SUM(ab.price)    AS total_spend,
            MAX(ab.price)    AS highest_bid
        FROM nx_persons p
        LEFT JOIN nx_auction_bids ab ON ab.bidder_id = p.id
        GROUP BY p.id, p.name, p.city",
        "1m",
        "DIFFERENTIAL",
    )
    .await;
}

/// L2b — `nx_category_metrics`: aggregate bids per auction category.
async fn nx_create_category_metrics(db: &E2eDb) {
    db.create_st(
        "nx_category_metrics",
        "SELECT
            category,
            COUNT(bid_id)       AS total_bids,
            SUM(price)          AS total_bid_volume,
            MAX(price)          AS highest_bid,
            AVG(price)          AS avg_bid_price
        FROM nx_auction_bids
        GROUP BY category",
        "1m",
        "DIFFERENTIAL",
    )
    .await;
}

// ── Nexmark tests ─────────────────────────────────────────────────────────

/// Initial population: L1 `nx_auction_bids` joins all 10 bids × auction metadata.
#[tokio::test]
async fn test_nx_pipeline_initial_population() {
    let db = E2eDb::new().await.with_extension().await;
    nx_setup_base_tables(&db).await;
    nx_create_auction_bids(&db).await;
    nx_create_bidder_stats(&db).await;
    nx_create_category_metrics(&db).await;

    // L1: 10 bids → 10 enriched rows
    assert_eq!(db.count("nx_auction_bids").await, 10);

    // L2a: 5 persons → 5 rows (Alice, Bob, Charlie, Diana, Eve)
    // Alice bid on auctions 4 and 2 → 2 bids, 530.00 total
    assert_eq!(db.count("nx_bidder_stats").await, 5);
    let (alice_bids, alice_spend): (i64, String) = sqlx::query_as(
        "SELECT total_bids::bigint, total_spend::text
         FROM nx_bidder_stats WHERE bidder_name = 'Alice'",
    )
    .fetch_one(&db.pool)
    .await
    .unwrap();
    assert_eq!(alice_bids, 2);
    assert_eq!(alice_spend, "530.00");

    // Charlie bid on auctions 1, 2, 4 → 3 bids, 1150.00 total
    let (charlie_bids, charlie_spend): (i64, String) = sqlx::query_as(
        "SELECT total_bids::bigint, total_spend::text
         FROM nx_bidder_stats WHERE bidder_name = 'Charlie'",
    )
    .fetch_one(&db.pool)
    .await
    .unwrap();
    assert_eq!(charlie_bids, 3);
    assert_eq!(charlie_spend, "1150.00");

    // L2b: 3 categories (Electronics, Art, Books)
    assert_eq!(db.count("nx_category_metrics").await, 3);
    let elec_bids: i64 = db
        .query_scalar(
            "SELECT total_bids::bigint FROM nx_category_metrics WHERE category = 'Electronics'",
        )
        .await;
    assert_eq!(elec_bids, 5); // bids 1-5

    // Eve is a seller only (id=5), never bid
    let eve_bids: i64 = db
        .query_scalar("SELECT total_bids::bigint FROM nx_bidder_stats WHERE bidder_name = 'Eve'")
        .await;
    assert_eq!(eve_bids, 0);

    // Status checks
    for st in ["nx_auction_bids", "nx_bidder_stats", "nx_category_metrics"] {
        let (status, mode, populated, errors) = db.pgt_status(st).await;
        assert_eq!(status, "ACTIVE", "{st} status");
        assert_eq!(mode, "DIFFERENTIAL", "{st} mode");
        assert!(populated, "{st} should be populated");
        assert_eq!(errors, 0, "{st} errors");
    }
}

/// New bid on an existing auction cascades through L1 → L2a and L2b.
#[tokio::test]
async fn test_nx_pipeline_insert_new_bid_cascades() {
    let db = E2eDb::new().await.with_extension().await;
    nx_setup_base_tables(&db).await;
    nx_create_auction_bids(&db).await;
    nx_create_bidder_stats(&db).await;
    nx_create_category_metrics(&db).await;

    // Pre-state: Eve has 0 bids; Electronics has 5 bids
    let pre_electronics_bids: i64 = db
        .query_scalar(
            "SELECT total_bids::bigint FROM nx_category_metrics WHERE category = 'Electronics'",
        )
        .await;
    assert_eq!(pre_electronics_bids, 5);

    // Eve places a bid on auction 1 (Electronics, Laptop Pro)
    db.execute("INSERT INTO nx_bids (id, auction_id, bidder_id, price) VALUES (11, 1, 5, 650.00)")
        .await;

    // Refresh in dependency order
    db.refresh_st("nx_auction_bids").await;
    db.refresh_st("nx_bidder_stats").await;
    db.refresh_st("nx_category_metrics").await;

    // L1: 10 → 11 rows
    assert_eq!(db.count("nx_auction_bids").await, 11);

    // L2a: Eve now has 1 bid, 650.00 spend
    let (eve_bids, eve_spend): (i64, String) = sqlx::query_as(
        "SELECT total_bids::bigint, total_spend::text
         FROM nx_bidder_stats WHERE bidder_name = 'Eve'",
    )
    .fetch_one(&db.pool)
    .await
    .unwrap();
    assert_eq!(eve_bids, 1);
    assert_eq!(eve_spend, "650.00");

    // L2b: Electronics 5 → 6 bids; new highest_bid = 650.00
    let (elec_bids, elec_highest): (i64, String) = sqlx::query_as(
        "SELECT total_bids::bigint, highest_bid::text
         FROM nx_category_metrics WHERE category = 'Electronics'",
    )
    .fetch_one(&db.pool)
    .await
    .unwrap();
    assert_eq!(elec_bids, 6);
    assert_eq!(elec_highest, "650.00");

    // Art and Books categories unchanged
    let art_bids: i64 = db
        .query_scalar("SELECT total_bids::bigint FROM nx_category_metrics WHERE category = 'Art'")
        .await;
    assert_eq!(art_bids, 3, "Art category should be unchanged");

    // DBSP invariant: L1 matches its defining query
    db.assert_st_matches_query(
        "nx_auction_bids",
        "SELECT b.id AS bid_id, b.auction_id, b.bidder_id,
                b.price::NUMERIC AS price, a.seller_id, a.category,
                a.item_name, a.reserve_price::NUMERIC AS reserve_price
         FROM nx_bids b
         JOIN nx_auctions a ON a.id = b.auction_id",
    )
    .await;
}

/// New auction added → `nx_auction_bids` gains rows when bids arrive for that auction.
/// Verifies that the DAG correctly handles a new node being added to the source set.
#[tokio::test]
async fn test_nx_pipeline_insert_new_auction_then_bid() {
    let db = E2eDb::new().await.with_extension().await;
    nx_setup_base_tables(&db).await;
    nx_create_auction_bids(&db).await;
    nx_create_bidder_stats(&db).await;
    nx_create_category_metrics(&db).await;

    // A new auction in a new category
    db.execute(
        "INSERT INTO nx_auctions (id, seller_id, category, item_name, reserve_price)
         VALUES (6, 1, 'Jewelry', 'Diamond Ring', 1000.00)",
    )
    .await;

    // No bids yet — L1 unchanged, category_metrics does NOT gain a 'Jewelry' row
    db.refresh_st("nx_auction_bids").await;
    db.refresh_st("nx_category_metrics").await;

    assert_eq!(db.count("nx_auction_bids").await, 10, "No new bids yet");
    let jewelry_count: i64 = db
        .query_scalar("SELECT count(*) FROM nx_category_metrics WHERE category = 'Jewelry'")
        .await;
    assert_eq!(
        jewelry_count, 0,
        "Jewelry not in metrics until a bid arrives"
    );

    // Now Bob bids on the new auction
    db.execute("INSERT INTO nx_bids (id, auction_id, bidder_id, price) VALUES (12, 6, 2, 1050.00)")
        .await;

    db.refresh_st("nx_auction_bids").await;
    db.refresh_st("nx_bidder_stats").await;
    db.refresh_st("nx_category_metrics").await;

    assert_eq!(db.count("nx_auction_bids").await, 11);

    // Jewelry now appears in category_metrics
    let jewelry_bids: i64 = db
        .query_scalar(
            "SELECT total_bids::bigint FROM nx_category_metrics WHERE category = 'Jewelry'",
        )
        .await;
    assert_eq!(jewelry_bids, 1);

    // Bob's stats updated
    let bob_bids: i64 = db
        .query_scalar("SELECT total_bids::bigint FROM nx_bidder_stats WHERE bidder_name = 'Bob'")
        .await;
    // Bob originally bid on auctions 1, 3, 5 → 3 bids; new bid on 6 = 4 total
    assert_eq!(bob_bids, 4);
}

/// Deleting a person who has bids cascades through L2a (bidder_stats).
/// The deleted person's row disappears; other persons unaffected.
#[tokio::test]
async fn test_nx_pipeline_delete_bidder_cascades() {
    let db = E2eDb::new().await.with_extension().await;
    nx_setup_base_tables(&db).await;
    nx_create_auction_bids(&db).await;
    nx_create_bidder_stats(&db).await;
    nx_create_category_metrics(&db).await;

    // Remove Charlie's bids first, then bids on Charlie's auction,
    // then Charlie's auction, then Charlie (FK cascade order).
    db.execute("DELETE FROM nx_bids WHERE bidder_id = 3").await;
    db.execute("DELETE FROM nx_bids WHERE auction_id = 3").await;
    db.execute("DELETE FROM nx_auctions WHERE seller_id = 3")
        .await;
    db.execute("DELETE FROM nx_persons WHERE id = 3").await;

    db.refresh_st("nx_auction_bids").await;
    db.refresh_st("nx_bidder_stats").await;
    db.refresh_st("nx_category_metrics").await;

    // L1: 4 fewer bids: 3 by Charlie (ids 2,5,8) + 1 on Charlie's auction (id 6)
    assert_eq!(db.count("nx_auction_bids").await, 6);

    // L2a: Charlie's row removed; other 4 persons still present
    assert_eq!(db.count("nx_bidder_stats").await, 4);
    let charlie_count: i64 = db
        .query_scalar("SELECT count(*) FROM nx_bidder_stats WHERE bidder_name = 'Charlie'")
        .await;
    assert_eq!(charlie_count, 0);

    // L2b: Electronics loses Charlie's bids (bid 2: 550.00, bid 5: 250.00)
    //       Art loses Charlie's bid (bid 8: 350.00) + Bob's bid on
    //       Charlie's auction (bid 6: 160.00), and auction 3 is gone.
    let elec_bids: i64 = db
        .query_scalar(
            "SELECT total_bids::bigint FROM nx_category_metrics WHERE category = 'Electronics'",
        )
        .await;
    assert_eq!(elec_bids, 3); // was 5, lost bids 2 and 5

    // Art: only bid 7 remains (Alice, auction 4)
    let art_bids: i64 = db
        .query_scalar("SELECT total_bids::bigint FROM nx_category_metrics WHERE category = 'Art'")
        .await;
    assert_eq!(art_bids, 1); // was 3, lost bids 6 and 8

    // DBSP invariant: all L2 tables match their defining queries
    db.assert_st_matches_query(
        "nx_bidder_stats",
        "SELECT p.id AS person_id, p.name AS bidder_name, p.city,
                COUNT(ab.bid_id) AS total_bids,
                SUM(ab.price)    AS total_spend,
                MAX(ab.price)    AS highest_bid
         FROM nx_persons p
         LEFT JOIN nx_auction_bids ab ON ab.bidder_id = p.id
         GROUP BY p.id, p.name, p.city",
    )
    .await;
    db.assert_st_matches_query(
        "nx_category_metrics",
        "SELECT category,
                COUNT(bid_id)    AS total_bids,
                SUM(price)       AS total_bid_volume,
                MAX(price)       AS highest_bid,
                AVG(price)       AS avg_bid_price
         FROM nx_auction_bids
         GROUP BY category",
    )
    .await;
}

/// UPDATE auction category renames a category across the full pipeline.
/// All stream tables containing that category name should reflect the change.
#[tokio::test]
async fn test_nx_pipeline_update_auction_category_cascades() {
    let db = E2eDb::new().await.with_extension().await;
    nx_setup_base_tables(&db).await;
    nx_create_auction_bids(&db).await;
    nx_create_category_metrics(&db).await;

    // Rename 'Books' → 'Rare Books'
    db.execute("UPDATE nx_auctions SET category = 'Rare Books' WHERE category = 'Books'")
        .await;

    db.refresh_st("nx_auction_bids").await;
    db.refresh_st("nx_category_metrics").await;

    // 'Books' should be gone, 'Rare Books' should be present
    let old_count: i64 = db
        .query_scalar("SELECT count(*) FROM nx_category_metrics WHERE category = 'Books'")
        .await;
    assert_eq!(old_count, 0, "'Books' should be renamed away");

    let new_bids: i64 = db
        .query_scalar(
            "SELECT total_bids::bigint FROM nx_category_metrics WHERE category = 'Rare Books'",
        )
        .await;
    assert_eq!(new_bids, 2, "'Rare Books' should have the same 2 bids");

    // L1 also updated
    let books_l1: i64 = db
        .query_scalar("SELECT count(*) FROM nx_auction_bids WHERE category = 'Books'")
        .await;
    assert_eq!(books_l1, 0);

    db.assert_st_matches_query(
        "nx_auction_bids",
        "SELECT b.id AS bid_id, b.auction_id, b.bidder_id,
                b.price::NUMERIC AS price, a.seller_id, a.category,
                a.item_name, a.reserve_price::NUMERIC AS reserve_price
         FROM nx_bids b
         JOIN nx_auctions a ON a.id = b.auction_id",
    )
    .await;
}

// ═══════════════════════════════════════════════════════════════════════════
// Scenario 2 — E-commerce Order Analytics Pipeline
// ═══════════════════════════════════════════════════════════════════════════

async fn ec_setup_base_tables(db: &E2eDb) {
    db.execute(
        "CREATE TABLE ec_categories (
            id   SERIAL PRIMARY KEY,
            name TEXT NOT NULL
        )",
    )
    .await;

    db.execute(
        "CREATE TABLE ec_products (
            id          SERIAL PRIMARY KEY,
            category_id INT NOT NULL REFERENCES ec_categories(id),
            name        TEXT NOT NULL,
            price       NUMERIC(10,2) NOT NULL
        )",
    )
    .await;

    db.execute(
        "CREATE TABLE ec_customers (
            id     SERIAL PRIMARY KEY,
            name   TEXT NOT NULL,
            region TEXT NOT NULL
        )",
    )
    .await;

    db.execute(
        "CREATE TABLE ec_orders (
            id          SERIAL PRIMARY KEY,
            customer_id INT NOT NULL REFERENCES ec_customers(id),
            status      TEXT NOT NULL DEFAULT 'pending'
        )",
    )
    .await;

    db.execute(
        "CREATE TABLE ec_order_items (
            id         SERIAL PRIMARY KEY,
            order_id   INT NOT NULL REFERENCES ec_orders(id),
            product_id INT NOT NULL REFERENCES ec_products(id),
            quantity   INT NOT NULL,
            unit_price NUMERIC(10,2) NOT NULL
        )",
    )
    .await;

    // Seed data
    db.execute(
        "INSERT INTO ec_categories (id, name) VALUES
            (1, 'Electronics'),
            (2, 'Apparel'),
            (3, 'Home & Garden')",
    )
    .await;

    db.execute(
        "INSERT INTO ec_products (id, category_id, name, price) VALUES
            (1, 1, 'Wireless Headphones',   79.99),
            (2, 1, 'USB-C Hub',             39.99),
            (3, 2, 'Winter Jacket',         129.99),
            (4, 2, 'Running Shoes',          89.99),
            (5, 3, 'Garden Hose',            24.99),
            (6, 3, 'Planting Pots (6-pack)', 19.99)",
    )
    .await;

    db.execute(
        "INSERT INTO ec_customers (id, name, region) VALUES
            (1, 'Alice', 'EMEA'),
            (2, 'Bob',   'APAC'),
            (3, 'Carol', 'AMER')",
    )
    .await;

    db.execute(
        "INSERT INTO ec_orders (id, customer_id, status) VALUES
            (1, 1, 'completed'),
            (2, 1, 'completed'),
            (3, 2, 'completed'),
            (4, 3, 'pending')",
    )
    .await;

    db.execute(
        "INSERT INTO ec_order_items (id, order_id, product_id, quantity, unit_price) VALUES
            (1,  1, 1, 1,  79.99),
            (2,  1, 2, 2,  39.99),
            (3,  2, 3, 1, 129.99),
            (4,  3, 4, 2,  89.99),
            (5,  3, 5, 1,  24.99),
            (6,  4, 6, 3,  19.99)",
    )
    .await;
}

/// L1a — products enriched with category name.
async fn ec_create_enriched_products(db: &E2eDb) {
    db.create_st(
        "ec_enriched_products",
        "SELECT
            p.id          AS product_id,
            p.name        AS product_name,
            p.price,
            c.id          AS category_id,
            c.name        AS category_name
        FROM ec_products p
        JOIN ec_categories c ON c.id = p.category_id",
        "1m",
        "DIFFERENTIAL",
    )
    .await;
}

/// L1b — orders enriched with customer info.
async fn ec_create_order_headers(db: &E2eDb) {
    db.create_st(
        "ec_order_headers",
        "SELECT
            o.id          AS order_id,
            o.status,
            c.id          AS customer_id,
            c.name        AS customer_name,
            c.region
        FROM ec_orders o
        JOIN ec_customers c ON c.id = o.customer_id",
        "1m",
        "DIFFERENTIAL",
    )
    .await;
}

/// L2 — order items joined with enriched products.
async fn ec_create_line_details(db: &E2eDb) {
    db.create_st(
        "ec_line_details",
        "SELECT
            oi.id          AS line_id,
            oi.order_id,
            oi.quantity,
            oi.unit_price,
            oi.quantity * oi.unit_price AS line_total,
            ep.product_id,
            ep.product_name,
            ep.category_id,
            ep.category_name
        FROM ec_order_items oi
        JOIN ec_enriched_products ep ON ep.product_id = oi.product_id",
        "1m",
        "DIFFERENTIAL",
    )
    .await;
}

/// L3 — order totals: join line_details with order_headers, sum per order.
async fn ec_create_order_totals(db: &E2eDb) {
    db.create_st(
        "ec_order_totals",
        "SELECT
            oh.order_id,
            oh.customer_id,
            oh.customer_name,
            oh.region,
            oh.status,
            SUM(ld.line_total)   AS order_total,
            SUM(ld.quantity)     AS total_items
        FROM ec_order_headers oh
        JOIN ec_line_details ld ON ld.order_id = oh.order_id
        GROUP BY oh.order_id, oh.customer_id, oh.customer_name, oh.region, oh.status",
        "1m",
        "DIFFERENTIAL",
    )
    .await;
}

/// L4 — category revenue: aggregate ec_line_details by category.
async fn ec_create_category_revenue(db: &E2eDb) {
    db.create_st(
        "ec_category_revenue",
        "SELECT
            category_id,
            category_name,
            COUNT(DISTINCT order_id) AS order_count,
            SUM(quantity)            AS units_sold,
            SUM(line_total)          AS total_revenue
        FROM ec_line_details
        GROUP BY category_id, category_name",
        "1m",
        "DIFFERENTIAL",
    )
    .await;
}

// ── E-commerce tests ──────────────────────────────────────────────────────

/// Initial population: all 4 stream table layers are correctly populated.
#[tokio::test]
async fn test_ec_pipeline_initial_population() {
    let db = E2eDb::new().await.with_extension().await;
    ec_setup_base_tables(&db).await;
    ec_create_enriched_products(&db).await;
    ec_create_order_headers(&db).await;
    ec_create_line_details(&db).await;
    ec_create_order_totals(&db).await;
    ec_create_category_revenue(&db).await;

    // L1a: 6 products
    assert_eq!(db.count("ec_enriched_products").await, 6);

    // L1b: 4 orders each has a customer
    assert_eq!(db.count("ec_order_headers").await, 4);

    // L2: 6 order items → 6 line detail rows
    assert_eq!(db.count("ec_line_details").await, 6);

    // L3: 4 orders — but order 4 has items too, so 4 rows
    assert_eq!(db.count("ec_order_totals").await, 4);

    // Order 1: headphones (79.99) + 2x USB-C hub (2*39.99=79.98) = 159.97
    let (order1_total, order1_items): (String, i64) = sqlx::query_as(
        "SELECT order_total::text, total_items::bigint
         FROM ec_order_totals WHERE order_id = 1",
    )
    .fetch_one(&db.pool)
    .await
    .unwrap();
    assert_eq!(order1_total, "159.97");
    assert_eq!(order1_items, 3);

    // Order 3 (Bob): running shoes 2x89.99=179.98 + hose 24.99 = 204.97
    let (order3_total, order3_region): (String, String) = sqlx::query_as(
        "SELECT order_total::text, region
         FROM ec_order_totals WHERE order_id = 3",
    )
    .fetch_one(&db.pool)
    .await
    .unwrap();
    assert_eq!(order3_total, "204.97");
    assert_eq!(order3_region, "APAC");

    // L4: 3 categories (Electronics, Apparel, Home & Garden)
    assert_eq!(db.count("ec_category_revenue").await, 3);

    let electronics_rev: String = db
        .query_scalar("SELECT total_revenue::text FROM ec_category_revenue WHERE category_name = 'Electronics'")
        .await;
    // Order 1: headphones(79.99) + 2*USB-C(79.98) = 159.97
    assert_eq!(electronics_rev, "159.97");

    // All stream tables ACTIVE
    for st in [
        "ec_enriched_products",
        "ec_order_headers",
        "ec_line_details",
        "ec_order_totals",
        "ec_category_revenue",
    ] {
        let (status, _, populated, errors) = db.pgt_status(st).await;
        assert_eq!(status, "ACTIVE", "{st}");
        assert!(populated, "{st} populated");
        assert_eq!(errors, 0, "{st} errors");
    }
}

/// New completed order with items cascades through all 4 layers.
#[tokio::test]
async fn test_ec_pipeline_insert_order_cascades_all_layers() {
    let db = E2eDb::new().await.with_extension().await;
    ec_setup_base_tables(&db).await;
    ec_create_enriched_products(&db).await;
    ec_create_order_headers(&db).await;
    ec_create_line_details(&db).await;
    ec_create_order_totals(&db).await;
    ec_create_category_revenue(&db).await;

    // Pre-state: Apparel revenue = jacket(129.99) + 2*shoes(179.98) = 309.97
    let pre_apparel: String = db
        .query_scalar(
            "SELECT total_revenue::text FROM ec_category_revenue WHERE category_name = 'Apparel'",
        )
        .await;
    assert_eq!(pre_apparel, "309.97");

    // Carol places a new order for headphones + jacket
    db.execute("INSERT INTO ec_orders (id, customer_id, status) VALUES (5, 3, 'completed')")
        .await;
    db.execute(
        "INSERT INTO ec_order_items (id, order_id, product_id, quantity, unit_price) VALUES
            (7, 5, 1, 1, 79.99),
            (8, 5, 3, 2, 129.99)",
    )
    .await;

    // Refresh in dependency order
    db.refresh_st("ec_enriched_products").await;
    db.refresh_st("ec_order_headers").await;
    db.refresh_st("ec_line_details").await;
    db.refresh_st("ec_order_totals").await;
    db.refresh_st("ec_category_revenue").await;

    // L1b: 4 → 5 orders
    assert_eq!(db.count("ec_order_headers").await, 5);

    // L2: 6 → 8 line items
    assert_eq!(db.count("ec_line_details").await, 8);

    // L3: Carol's new order total = 79.99 + 2*129.99 = 339.97
    let carol_order: String = db
        .query_scalar("SELECT order_total::text FROM ec_order_totals WHERE order_id = 5")
        .await;
    assert_eq!(carol_order, "339.97");

    // L4: Electronics gained 79.99; Apparel gained 2*129.99=259.98
    let post_electronics: String = db
        .query_scalar("SELECT total_revenue::text FROM ec_category_revenue WHERE category_name = 'Electronics'")
        .await;
    assert_eq!(post_electronics, "239.96"); // 159.97 + 79.99

    let post_apparel: String = db
        .query_scalar(
            "SELECT total_revenue::text FROM ec_category_revenue WHERE category_name = 'Apparel'",
        )
        .await;
    assert_eq!(post_apparel, "569.95"); // 309.97 + 259.98

    // Home & Garden unchanged (no new items in that category)
    let post_hg: String = db
        .query_scalar("SELECT total_revenue::text FROM ec_category_revenue WHERE category_name = 'Home & Garden'")
        .await;
    assert_eq!(post_hg, "84.96", "Home & Garden unchanged"); // 3*19.99 + 24.99

    // DBSP invariant on all layers
    db.assert_st_matches_query(
        "ec_line_details",
        "SELECT oi.id AS line_id, oi.order_id, oi.quantity,
                oi.unit_price::NUMERIC AS unit_price,
                oi.quantity * oi.unit_price AS line_total,
                ep.product_id, ep.product_name, ep.category_id, ep.category_name
         FROM ec_order_items oi
         JOIN ec_enriched_products ep ON ep.product_id = oi.product_id",
    )
    .await;
}

/// Renaming a category cascades through L1a → L2 → L4.
/// `ec_order_totals` (L3) is unaffected because it does not project category_name.
#[tokio::test]
async fn test_ec_pipeline_rename_category_cascades() {
    let db = E2eDb::new().await.with_extension().await;
    ec_setup_base_tables(&db).await;
    ec_create_enriched_products(&db).await;
    ec_create_order_headers(&db).await;
    ec_create_line_details(&db).await;
    ec_create_order_totals(&db).await;
    ec_create_category_revenue(&db).await;

    // Rename 'Apparel' → 'Fashion'
    db.execute("UPDATE ec_categories SET name = 'Fashion' WHERE name = 'Apparel'")
        .await;

    db.refresh_st("ec_enriched_products").await;
    db.refresh_st("ec_order_headers").await;
    db.refresh_st("ec_line_details").await;
    db.refresh_st("ec_order_totals").await;
    db.refresh_st("ec_category_revenue").await;

    // L1a: Apparel gone, Fashion present
    let apparel_count: i64 = db
        .query_scalar("SELECT count(*) FROM ec_enriched_products WHERE category_name = 'Apparel'")
        .await;
    assert_eq!(apparel_count, 0);
    let fashion_count: i64 = db
        .query_scalar("SELECT count(*) FROM ec_enriched_products WHERE category_name = 'Fashion'")
        .await;
    assert_eq!(
        fashion_count, 2,
        "Jacket and Running Shoes now under Fashion"
    );

    // L4: category_revenue reflects the rename
    let old_cat: i64 = db
        .query_scalar("SELECT count(*) FROM ec_category_revenue WHERE category_name = 'Apparel'")
        .await;
    assert_eq!(old_cat, 0, "'Apparel' row removed from category_revenue");
    let new_rev: String = db
        .query_scalar(
            "SELECT total_revenue::text FROM ec_category_revenue WHERE category_name = 'Fashion'",
        )
        .await;
    assert_eq!(
        new_rev, "309.97",
        "'Fashion' has the same revenue as 'Apparel' had"
    );

    // DBSP invariant
    db.assert_st_matches_query(
        "ec_category_revenue",
        "SELECT category_id, category_name,
                COUNT(DISTINCT order_id) AS order_count,
                SUM(quantity)            AS units_sold,
                SUM(line_total)          AS total_revenue
         FROM ec_line_details
         GROUP BY category_id, category_name",
    )
    .await;
}

/// Adding a new product to an existing category cascades only when that product
/// is ordered. Adding a product with no orders does not affect revenue aggregates.
#[tokio::test]
async fn test_ec_pipeline_new_product_without_orders_no_revenue_change() {
    let db = E2eDb::new().await.with_extension().await;
    ec_setup_base_tables(&db).await;
    ec_create_enriched_products(&db).await;
    ec_create_order_headers(&db).await;
    ec_create_line_details(&db).await;
    ec_create_order_totals(&db).await;
    ec_create_category_revenue(&db).await;

    let pre_electronics_rev: String = db
        .query_scalar("SELECT total_revenue::text FROM ec_category_revenue WHERE category_name = 'Electronics'")
        .await;

    // Add a new Electronics product
    db.execute(
        "INSERT INTO ec_products (id, category_id, name, price) VALUES (7, 1, 'Smart Watch', 299.99)",
    )
    .await;

    db.refresh_st("ec_enriched_products").await;
    db.refresh_st("ec_line_details").await;
    db.refresh_st("ec_order_totals").await;
    db.refresh_st("ec_category_revenue").await;

    // L1a gains a row
    assert_eq!(db.count("ec_enriched_products").await, 7);

    // L4: revenue unchanged (no orders for Smart Watch yet)
    let post_electronics_rev: String = db
        .query_scalar("SELECT total_revenue::text FROM ec_category_revenue WHERE category_name = 'Electronics'")
        .await;
    assert_eq!(
        pre_electronics_rev, post_electronics_rev,
        "Revenue unchanged when new product has no orders"
    );

    // Now someone orders the Smart Watch
    db.execute("INSERT INTO ec_orders (id, customer_id, status) VALUES (5, 2, 'completed')")
        .await;
    db.execute(
        "INSERT INTO ec_order_items (id, order_id, product_id, quantity, unit_price) VALUES (7, 5, 7, 1, 299.99)",
    )
    .await;

    db.refresh_st("ec_order_headers").await;
    db.refresh_st("ec_line_details").await;
    db.refresh_st("ec_order_totals").await;
    db.refresh_st("ec_category_revenue").await;

    let final_electronics_rev: String = db
        .query_scalar("SELECT total_revenue::text FROM ec_category_revenue WHERE category_name = 'Electronics'")
        .await;
    assert_eq!(final_electronics_rev, "459.96"); // 159.97 + 299.99
}

// ═══════════════════════════════════════════════════════════════════════════
// Scenario 3 — IoT Telemetry Pipeline
// ═══════════════════════════════════════════════════════════════════════════

async fn tl_setup_base_tables(db: &E2eDb) {
    db.execute(
        "CREATE TABLE tl_devices (
            id          SERIAL PRIMARY KEY,
            name        TEXT NOT NULL,
            location    TEXT NOT NULL,
            device_type TEXT NOT NULL
        )",
    )
    .await;

    db.execute(
        "CREATE TABLE tl_sensors (
            id          SERIAL PRIMARY KEY,
            device_id   INT NOT NULL REFERENCES tl_devices(id),
            sensor_type TEXT NOT NULL,
            unit        TEXT NOT NULL
        )",
    )
    .await;

    db.execute(
        "CREATE TABLE tl_readings (
            id        BIGSERIAL PRIMARY KEY,
            sensor_id INT NOT NULL REFERENCES tl_sensors(id),
            value     NUMERIC(12,4) NOT NULL
        )",
    )
    .await;

    // Seed: 3 devices, 6 sensors, 18 readings
    db.execute(
        "INSERT INTO tl_devices (id, name, location, device_type) VALUES
            (1, 'WeatherStation-A', 'Rooftop',      'Weather'),
            (2, 'WeatherStation-B', 'Ground Floor', 'Weather'),
            (3, 'PowerMeter-01',    'Basement',     'Power')",
    )
    .await;

    db.execute(
        "INSERT INTO tl_sensors (id, device_id, sensor_type, unit) VALUES
            (1, 1, 'temperature', 'Celsius'),
            (2, 1, 'humidity',    'Percent'),
            (3, 2, 'temperature', 'Celsius'),
            (4, 2, 'humidity',    'Percent'),
            (5, 3, 'voltage',     'Volt'),
            (6, 3, 'current',     'Ampere')",
    )
    .await;

    db.execute(
        "INSERT INTO tl_readings (id, sensor_id, value) VALUES
            (1,  1, 22.5),
            (2,  1, 23.1),
            (3,  1, 21.8),
            (4,  2, 60.0),
            (5,  2, 62.5),
            (6,  2, 61.0),
            (7,  3, 18.2),
            (8,  3, 19.0),
            (9,  3, 17.5),
            (10, 4, 55.0),
            (11, 4, 57.0),
            (12, 4, 56.5),
            (13, 5, 230.0),
            (14, 5, 231.5),
            (15, 5, 229.8),
            (16, 6, 15.2),
            (17, 6, 15.5),
            (18, 6, 14.9)",
    )
    .await;
}

/// L1 — readings enriched with sensor metadata.
async fn tl_create_sensor_readings(db: &E2eDb) {
    db.create_st(
        "tl_sensor_readings",
        "SELECT
            r.id             AS reading_id,
            r.sensor_id,
            r.value,
            s.device_id,
            s.sensor_type,
            s.unit
        FROM tl_readings r
        JOIN tl_sensors s ON s.id = r.sensor_id",
        "1m",
        "DIFFERENTIAL",
    )
    .await;
}

/// L2 — readings enriched with device metadata (sensor_readings JOIN devices).
async fn tl_create_device_readings(db: &E2eDb) {
    db.create_st(
        "tl_device_readings",
        "SELECT
            sr.reading_id,
            sr.sensor_id,
            sr.sensor_type,
            sr.unit,
            sr.value,
            d.id        AS device_id,
            d.name      AS device_name,
            d.location,
            d.device_type
        FROM tl_sensor_readings sr
        JOIN tl_devices d ON d.id = sr.device_id",
        "1m",
        "DIFFERENTIAL",
    )
    .await;
}

/// L3 — per-device aggregated statistics.
async fn tl_create_device_stats(db: &E2eDb) {
    db.create_st(
        "tl_device_stats",
        "SELECT
            device_id,
            device_name,
            location,
            device_type,
            COUNT(reading_id)   AS reading_count,
            MIN(value)          AS min_value,
            MAX(value)          AS max_value,
            AVG(value)          AS avg_value
        FROM tl_device_readings
        GROUP BY device_id, device_name, location, device_type",
        "1m",
        "DIFFERENTIAL",
    )
    .await;
}

// ── IoT tests ─────────────────────────────────────────────────────────────

/// Initial population of the 3-level telemetry pipeline.
#[tokio::test]
async fn test_tl_pipeline_initial_population() {
    let db = E2eDb::new().await.with_extension().await;
    tl_setup_base_tables(&db).await;
    tl_create_sensor_readings(&db).await;
    tl_create_device_readings(&db).await;
    tl_create_device_stats(&db).await;

    // L1: 18 readings
    assert_eq!(db.count("tl_sensor_readings").await, 18);

    // L2: 18 readings (all enriched with device info)
    assert_eq!(db.count("tl_device_readings").await, 18);

    // L3: 3 devices
    assert_eq!(db.count("tl_device_stats").await, 3);

    // WeatherStation-A: sensors 1 (temp) + 2 (humidity) → 6 readings
    let ws_a_count: i64 = db
        .query_scalar(
            "SELECT reading_count::bigint FROM tl_device_stats WHERE device_name = 'WeatherStation-A'",
        )
        .await;
    assert_eq!(ws_a_count, 6);

    // PowerMeter-01: sensors 5 (voltage) + 6 (current) → 6 readings
    let pm_count: i64 = db
        .query_scalar(
            "SELECT reading_count::bigint FROM tl_device_stats WHERE device_name = 'PowerMeter-01'",
        )
        .await;
    assert_eq!(pm_count, 6);

    for st in [
        "tl_sensor_readings",
        "tl_device_readings",
        "tl_device_stats",
    ] {
        let (status, mode, populated, errors) = db.pgt_status(st).await;
        assert_eq!(status, "ACTIVE", "{st}");
        assert_eq!(mode, "DIFFERENTIAL", "{st}");
        assert!(populated, "{st}");
        assert_eq!(errors, 0, "{st}");
    }
}

/// Batch of new readings cascades through all 3 levels.
#[tokio::test]
async fn test_tl_pipeline_insert_readings_cascades() {
    let db = E2eDb::new().await.with_extension().await;
    tl_setup_base_tables(&db).await;
    tl_create_sensor_readings(&db).await;
    tl_create_device_readings(&db).await;
    tl_create_device_stats(&db).await;

    // Pre-conditions for WeatherStation-A temperature sensor (id=1)
    let pre_ws_a_count: i64 = db
        .query_scalar(
            "SELECT reading_count::bigint FROM tl_device_stats WHERE device_name = 'WeatherStation-A'",
        )
        .await;
    assert_eq!(pre_ws_a_count, 6);

    // Add 3 new temperature readings for sensor 1 (WeatherStation-A)
    db.execute(
        "INSERT INTO tl_readings (id, sensor_id, value) VALUES
            (19, 1, 24.0),
            (20, 1, 24.5),
            (21, 1, 23.8)",
    )
    .await;

    db.refresh_st("tl_sensor_readings").await;
    db.refresh_st("tl_device_readings").await;
    db.refresh_st("tl_device_stats").await;

    // L1: 18 → 21
    assert_eq!(db.count("tl_sensor_readings").await, 21);

    // L3: WeatherStation-A 6 → 9 readings; max_value updated
    let (ws_a_count, ws_a_max): (i64, String) = sqlx::query_as(
        "SELECT reading_count::bigint, max_value::text
         FROM tl_device_stats WHERE device_name = 'WeatherStation-A'",
    )
    .fetch_one(&db.pool)
    .await
    .unwrap();
    assert_eq!(ws_a_count, 9);
    // Max is from humidity sensor 2 (reading id 5, value 62.5), not new temp readings
    assert_eq!(ws_a_max, "62.5000");

    // Other devices unaffected
    let ws_b_count: i64 = db
        .query_scalar(
            "SELECT reading_count::bigint FROM tl_device_stats WHERE device_name = 'WeatherStation-B'",
        )
        .await;
    assert_eq!(ws_b_count, 6, "WeatherStation-B unaffected");

    // DBSP invariant
    db.assert_st_matches_query(
        "tl_device_stats",
        "SELECT device_id, device_name, location, device_type,
                COUNT(reading_id) AS reading_count,
                MIN(value) AS min_value, MAX(value) AS max_value, AVG(value) AS avg_value
         FROM tl_device_readings
         GROUP BY device_id, device_name, location, device_type",
    )
    .await;
}

/// Adding a new sensor (not yet with readings) only extends L1 skeleton;
/// once readings arrive, all 3 levels update.
#[tokio::test]
async fn test_tl_pipeline_new_sensor_lifecycle() {
    let db = E2eDb::new().await.with_extension().await;
    tl_setup_base_tables(&db).await;
    tl_create_sensor_readings(&db).await;
    tl_create_device_readings(&db).await;
    tl_create_device_stats(&db).await;

    let pre_pm_readings: i64 = db
        .query_scalar(
            "SELECT reading_count::bigint FROM tl_device_stats WHERE device_name = 'PowerMeter-01'",
        )
        .await;
    assert_eq!(pre_pm_readings, 6);

    // Add a new 'energy' sensor to PowerMeter-01 (no readings yet)
    db.execute(
        "INSERT INTO tl_sensors (id, device_id, sensor_type, unit) VALUES (7, 3, 'energy', 'kWh')",
    )
    .await;

    db.refresh_st("tl_sensor_readings").await;
    db.refresh_st("tl_device_readings").await;
    db.refresh_st("tl_device_stats").await;

    // New sensor has no readings → counts unchanged
    let mid_pm_readings: i64 = db
        .query_scalar(
            "SELECT reading_count::bigint FROM tl_device_stats WHERE device_name = 'PowerMeter-01'",
        )
        .await;
    assert_eq!(mid_pm_readings, 6, "No new readings, count unchanged");

    // Now deliver a reading for the new sensor
    db.execute("INSERT INTO tl_readings (id, sensor_id, value) VALUES (19, 7, 3.55)")
        .await;

    db.refresh_st("tl_sensor_readings").await;
    db.refresh_st("tl_device_readings").await;
    db.refresh_st("tl_device_stats").await;

    let post_pm_readings: i64 = db
        .query_scalar(
            "SELECT reading_count::bigint FROM tl_device_stats WHERE device_name = 'PowerMeter-01'",
        )
        .await;
    assert_eq!(post_pm_readings, 7, "One new reading for PowerMeter-01");
}

/// Moving a device to a new location cascades through L2 and L3.
#[tokio::test]
async fn test_tl_pipeline_update_device_location_cascades() {
    let db = E2eDb::new().await.with_extension().await;
    tl_setup_base_tables(&db).await;
    tl_create_sensor_readings(&db).await;
    tl_create_device_readings(&db).await;
    tl_create_device_stats(&db).await;

    // Pre-state
    let pre_location: String = db
        .query_scalar("SELECT location FROM tl_device_stats WHERE device_name = 'WeatherStation-B'")
        .await;
    assert_eq!(pre_location, "Ground Floor");

    // Move WeatherStation-B to Rooftop
    db.execute("UPDATE tl_devices SET location = 'Rooftop Level 2' WHERE id = 2")
        .await;

    db.refresh_st("tl_sensor_readings").await;
    db.refresh_st("tl_device_readings").await;
    db.refresh_st("tl_device_stats").await;

    // L2: all 6 readings for device_id=2 reflect new location
    let old_loc_count: i64 = db
        .query_scalar(
            "SELECT count(*) FROM tl_device_readings
             WHERE device_id = 2 AND location = 'Ground Floor'",
        )
        .await;
    assert_eq!(old_loc_count, 0, "Old location gone from device_readings");

    let new_loc_count: i64 = db
        .query_scalar(
            "SELECT count(*) FROM tl_device_readings
             WHERE device_id = 2 AND location = 'Rooftop Level 2'",
        )
        .await;
    assert_eq!(new_loc_count, 6, "All readings on new location");

    // L3: stats row reflects updated location
    let post_location: String = db
        .query_scalar("SELECT location FROM tl_device_stats WHERE device_name = 'WeatherStation-B'")
        .await;
    assert_eq!(post_location, "Rooftop Level 2");

    // Other device untouched
    let ws_a_location: String = db
        .query_scalar("SELECT location FROM tl_device_stats WHERE device_name = 'WeatherStation-A'")
        .await;
    assert_eq!(ws_a_location, "Rooftop", "WeatherStation-A unchanged");

    // DBSP invariants on all layers
    db.assert_st_matches_query(
        "tl_device_readings",
        "SELECT sr.reading_id, sr.sensor_id, sr.sensor_type, sr.unit, sr.value,
                d.id AS device_id, d.name AS device_name, d.location, d.device_type
         FROM tl_sensor_readings sr
         JOIN tl_devices d ON d.id = sr.device_id",
    )
    .await;
    db.assert_st_matches_query(
        "tl_device_stats",
        "SELECT device_id, device_name, location, device_type,
                COUNT(reading_id) AS reading_count,
                MIN(value) AS min_value, MAX(value) AS max_value, AVG(value) AS avg_value
         FROM tl_device_readings
         GROUP BY device_id, device_name, location, device_type",
    )
    .await;
}

/// Deleting all readings for one device leaves its L3 stats row with zero count.
/// (Tests that the LEFT JOIN materialisation handles the empty aggregate correctly.)
#[tokio::test]
async fn test_tl_pipeline_delete_all_device_readings() {
    let db = E2eDb::new().await.with_extension().await;
    tl_setup_base_tables(&db).await;
    tl_create_sensor_readings(&db).await;
    tl_create_device_readings(&db).await;
    // Note: tl_device_stats uses INNER JOIN semantics via GROUP BY,
    // so when all readings are deleted the row disappears entirely.
    tl_create_device_stats(&db).await;

    assert_eq!(db.count("tl_device_stats").await, 3);

    // Delete all readings for WeatherStation-B (sensor_ids 3 and 4)
    db.execute("DELETE FROM tl_readings WHERE sensor_id IN (3, 4)")
        .await;

    db.refresh_st("tl_sensor_readings").await;
    db.refresh_st("tl_device_readings").await;
    db.refresh_st("tl_device_stats").await;

    // L1: 18 → 12 (lost 6 readings for WeatherStation-B)
    assert_eq!(db.count("tl_sensor_readings").await, 12);

    // L3: WeatherStation-B row disappears (GROUP BY with no rows = no output)
    assert_eq!(
        db.count("tl_device_stats").await,
        2,
        "WeatherStation-B stats row removed when it has no readings"
    );

    let ws_b_count: i64 = db
        .query_scalar("SELECT count(*) FROM tl_device_stats WHERE device_name = 'WeatherStation-B'")
        .await;
    assert_eq!(ws_b_count, 0);

    // DBSP invariant
    db.assert_st_matches_query(
        "tl_device_stats",
        "SELECT device_id, device_name, location, device_type,
                COUNT(reading_id) AS reading_count,
                MIN(value) AS min_value, MAX(value) AS max_value, AVG(value) AS avg_value
         FROM tl_device_readings
         GROUP BY device_id, device_name, location, device_type",
    )
    .await;
}
