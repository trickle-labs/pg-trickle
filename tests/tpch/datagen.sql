-- TPC-H-derived data generator — pure SQL, no external tools.
--
-- This data generator is inspired by the TPC-H Benchmark specification but
-- uses custom SQL-based generation (not the TPC reference `dbgen` tool).
-- Results do not constitute a TPC-H Benchmark result. "TPC-H" is a
-- trademark of the Transaction Processing Performance Council (tpc.org).
--
-- Generates TPC-H-like data using generate_series with deterministic
-- pseudo-random distributions. Parameterized by a scale factor (SF)
-- substituted by the Rust test harness before execution:
--
--   __SF_ORDERS__   = number of orders   (SF * 150000, min 1500)
--   __SF_CUSTOMERS__ = number of customers (SF * 15000, min 150)
--   __SF_SUPPLIERS__ = number of suppliers (SF * 1000,  min 10)
--   __SF_PARTS__     = number of parts    (SF * 20000, min 200)
--
-- The harness replaces these tokens before sending to PostgreSQL.

-- ── Reference tables (static, same at all scale factors) ───────────

INSERT INTO region (r_regionkey, r_name, r_comment) VALUES
    (0, 'AFRICA',     'special Tiresias about the furiously even dolphins'),
    (1, 'AMERICA',    'hs use ironic, even requests. s'),
    (2, 'ASIA',       'ges. thinly even pinto beans ca'),
    (3, 'EUROPE',     'ly final courts cajole furiously final excuse'),
    (4, 'MIDDLE EAST','uickly special accounts cajole carefully blithely close');

INSERT INTO nation (n_nationkey, n_name, n_regionkey, n_comment)
VALUES
    ( 0, 'ALGERIA',        0, 'haggle. carefully final deposits detect slyly agai'),
    ( 1, 'ARGENTINA',      1, 'al foxes promise slyly according to the regular accounts'),
    ( 2, 'BRAZIL',         1, 'y alongside of the pending deposits. carefully special'),
    ( 3, 'CANADA',         1, 'eas hang ironic, silent packages. slyly regular packages'),
    ( 4, 'EGYPT',          4, 'y above the carefully unusual theodolites. final dugouts'),
    ( 5, 'ETHIOPIA',       0, 'ven packages wake quickly. regu'),
    ( 6, 'FRANCE',         3, 'refully final requests. regular, ironi'),
    ( 7, 'GERMANY',        3, 'l platelets. regular accounts x-ray: unusual, regular acco'),
    ( 8, 'INDIA',          2, 'ss excuses cajole slyly across the packages. quietly even'),
    ( 9, 'INDONESIA',      2, 'slyly express asymptotes. regular deposits haggle slyly'),
    (10, 'IRAN',           4, 'efully alongside of the slyly final dependencies'),
    (11, 'IRAQ',           4, 'nic deposits boost atop the quickly final requests?'),
    (12, 'JAPAN',          2, 'ously. final, express gifts cajole a'),
    (13, 'JORDAN',         4, 'ic deposits are blithely about the carefully regular pa'),
    (14, 'KENYA',          0, 'pending excuses haggle furiously deposits. pending, express'),
    (15, 'MOROCCO',        0, 'rns. blithely bold courts among the closely regular'),
    (16, 'MOZAMBIQUE',     0, 's. ironic, unusual asymptotes wake blithely r'),
    (17, 'PERU',           1, 'platelets. blithely pending dependencies use fluffily'),
    (18, 'CHINA',          2, 'c dependencies. furiously express notornis sleep slyly'),
    (19, 'ROMANIA',        3, 'ular asymptotes are about the furious multipliers. express'),
    (20, 'SAUDI ARABIA',   4, 'ts. silent requests haggle. closely express packages sleep'),
    (21, 'VIETNAM',        2, 'hely enticingly express accounts. even, final'),
    (22, 'RUSSIA',         3, 'requests against the platelets use never according to the'),
    (23, 'UNITED KINGDOM', 3, 'eans boost carefully special requests. accounts are'),
    (24, 'UNITED STATES',  1, 'y final packages. slow foxes cajole quickly. quickly silent');

-- ── Suppliers ──────────────────────────────────────────────────────

INSERT INTO supplier (s_suppkey, s_name, s_address, s_nationkey, s_phone, s_acctbal, s_comment)
SELECT
    i AS s_suppkey,
    'Supplier#' || lpad(i::text, 9, '0') AS s_name,
    'Addr' || i AS s_address,
    (i * 7 + 3) % 25 AS s_nationkey,
    lpad(((i * 7 + 3) % 25 + 10)::text, 2, '0') || '-' ||
        lpad((100 + i % 900)::text, 3, '0') || '-' ||
        lpad((1000 + i % 9000)::text, 4, '0') || '-' ||
        lpad((1000 + (i * 3) % 9000)::text, 4, '0') AS s_phone,
    round((-999.99 + (i * 1327) % 20000)::numeric / 10, 2) AS s_acctbal,
    CASE WHEN i % 100 = 50
         THEN 'Customer Complaints' || repeat(' ', i % 20)
         ELSE 'regular accounts ' || (i % 1000)::text
    END AS s_comment
FROM generate_series(1, __SF_SUPPLIERS__) AS s(i);

-- ── Parts ──────────────────────────────────────────────────────────

INSERT INTO part (p_partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice, p_comment)
SELECT
    i AS p_partkey,
    -- Name: combination of color/material words
    (ARRAY['almond','antique','aquamarine','azure','beige',
           'bisque','black','blanched','blue','blush',
           'brown','burlywood','burnished','chartreuse','chocolate',
           'coral','cornflower','cornsilk','cream','cyan'])[1 + i % 20]
    || ' ' ||
    (ARRAY['steel','copper','tin','brass','nickel',
           'iron','frosted','polished','burnished','plated'])[1 + (i * 3) % 10]
    AS p_name,
    'Manufacturer#' || (1 + i % 5) AS p_mfgr,
    'Brand#' || (1 + (i - 1) % 5) || (1 + ((i - 1) / 5) % 5) AS p_brand,
    (ARRAY['ECONOMY ANODIZED STEEL','ECONOMY BRUSHED COPPER','ECONOMY BURNISHED BRASS',
           'ECONOMY PLATED TIN','ECONOMY POLISHED NICKEL',
           'STANDARD ANODIZED STEEL','STANDARD BRUSHED COPPER','STANDARD BURNISHED BRASS',
           'STANDARD PLATED TIN','STANDARD POLISHED NICKEL',
           'MEDIUM ANODIZED STEEL','MEDIUM BRUSHED COPPER','MEDIUM BURNISHED BRASS',
           'MEDIUM PLATED TIN','MEDIUM POLISHED NICKEL',
           'LARGE ANODIZED STEEL','LARGE BRUSHED COPPER','LARGE BURNISHED BRASS',
           'LARGE PLATED TIN','LARGE POLISHED NICKEL',
           'PROMO ANODIZED STEEL','PROMO BRUSHED COPPER','PROMO BURNISHED BRASS',
           'PROMO PLATED TIN','PROMO POLISHED NICKEL'])[1 + i % 25] AS p_type,
    1 + i % 50 AS p_size,
    (ARRAY['SM CASE','SM BOX','SM PACK','SM PKG','SM BAG','SM CAN','SM DRUM','SM JAR',
           'MED CASE','MED BOX','MED PACK','MED PKG','MED BAG','MED CAN','MED DRUM','MED JAR',
           'LG CASE','LG BOX','LG PACK','LG PKG','LG BAG','LG CAN','LG DRUM','LG JAR',
           'WRAP CASE','WRAP BOX','WRAP PACK','WRAP PKG','WRAP BAG','WRAP CAN','WRAP DRUM','WRAP JAR',
           'JUMBO CASE','JUMBO BOX','JUMBO PACK','JUMBO PKG','JUMBO BAG','JUMBO CAN','JUMBO DRUM','JUMBO JAR'
          ])[1 + ((i - 1) / 25) % 40] AS p_container,
    round((90000 + (i % 2001) * 100 + (i / 10))::numeric / 100, 2) AS p_retailprice,
    'comment ' || i AS p_comment
FROM generate_series(1, __SF_PARTS__) AS s(i);

-- ── PartSupp ───────────────────────────────────────────────────────
-- 4 suppliers per part (standard TPC-H ratio)

INSERT INTO partsupp (ps_partkey, ps_suppkey, ps_availqty, ps_supplycost, ps_comment)
SELECT
    p AS ps_partkey,
    1 + ((p + s * 251 - 1) % __SF_SUPPLIERS__) AS ps_suppkey,
    (p * 17 + s * 1327) % 10000 AS ps_availqty,
    round(((p * 31 + s * 97) % 100000)::numeric / 100, 2) AS ps_supplycost,
    'partsupp comment ' || p || '-' || s AS ps_comment
FROM generate_series(1, __SF_PARTS__) AS parts(p),
     generate_series(0, 3) AS idx(s);

-- ── Customers ──────────────────────────────────────────────────────

INSERT INTO customer (c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_mktsegment, c_comment)
SELECT
    i AS c_custkey,
    'Customer#' || lpad(i::text, 9, '0') AS c_name,
    'CAddr' || i AS c_address,
    (i * 11 + 7) % 25 AS c_nationkey,
    lpad(((i * 11 + 7) % 25 + 10)::text, 2, '0') || '-' ||
        lpad((100 + i % 900)::text, 3, '0') || '-' ||
        lpad((1000 + i % 9000)::text, 4, '0') || '-' ||
        lpad((1000 + (i * 7) % 9000)::text, 4, '0') AS c_phone,
    round((-999.99 + (i * 1019) % 20000)::numeric / 10, 2) AS c_acctbal,
    (ARRAY['AUTOMOBILE','BUILDING','FURNITURE','HOUSEHOLD','MACHINERY'])[1 + i % 5] AS c_mktsegment,
    'customer comment ' || i AS c_comment
FROM generate_series(1, __SF_CUSTOMERS__) AS s(i);

-- ── Orders ─────────────────────────────────────────────────────────

INSERT INTO orders (o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate,
                    o_orderpriority, o_clerk, o_shippriority, o_comment)
SELECT
    i AS o_orderkey,
    -- Customers: use sparse key space (every 3rd customer has orders — matches TPC-H distribution)
    1 + ((i * 1013) % __SF_CUSTOMERS__) AS o_custkey,
    (ARRAY['O','F','P'])[1 + i % 3] AS o_orderstatus,
    round((10000 + (i::bigint * 7919) % 500000)::numeric / 100, 2) AS o_totalprice,
    DATE '1992-01-01' + ((i::bigint * 2609) % 2557)::int AS o_orderdate,  -- 7 years: 1992-01-01 to 1998-12-31
    (ARRAY['1-URGENT','2-HIGH','3-MEDIUM','4-NOT SPECIFIED','5-LOW'])[1 + i % 5] AS o_orderpriority,
    'Clerk#' || lpad((1 + i % 1000)::text, 9, '0') AS o_clerk,
    0 AS o_shippriority,
    'order comment ' || i AS o_comment
FROM generate_series(1, __SF_ORDERS__) AS s(i);

-- ── Lineitem ───────────────────────────────────────────────────────
-- ~4 lineitems per order on average (TPC-H uses 1–7)

INSERT INTO lineitem (l_orderkey, l_linenumber, l_partkey, l_suppkey,
                      l_quantity, l_extendedprice, l_discount, l_tax,
                      l_returnflag, l_linestatus, l_shipdate, l_commitdate,
                      l_receiptdate, l_shipinstruct, l_shipmode, l_comment)
SELECT
    o AS l_orderkey,
    ln AS l_linenumber,
    1 + ((o * 7 + ln * 1013) % __SF_PARTS__) AS l_partkey,
    1 + ((o * 7 + ln * 1013 + ln * 251) % __SF_SUPPLIERS__) AS l_suppkey,
    1 + ((o * 3 + ln * 17) % 50) AS l_quantity,
    round((100 + ((o * 31 + ln * 97) % 100000))::numeric / 100 *
          (1 + ((o * 3 + ln * 17) % 50)), 2) AS l_extendedprice,
    round(((o * 13 + ln * 7) % 11)::numeric / 100, 2) AS l_discount,  -- 0.00..0.10
    round(((o * 17 + ln * 3) % 9)::numeric / 100, 2) AS l_tax,        -- 0.00..0.08
    (ARRAY['A','N','R'])[1 + (o + ln) % 3] AS l_returnflag,
    (ARRAY['O','F'])[1 + (o + ln * 3) % 2] AS l_linestatus,
    -- shipdate: order date + 1..121 days
    DATE '1992-01-01' + ((o::bigint * 2609) % 2557)::int + 1 + ((o * 3 + ln * 11) % 121) AS l_shipdate,
    -- commitdate: order date + 30..90 days
    DATE '1992-01-01' + ((o::bigint * 2609) % 2557)::int + 30 + ((o * 7 + ln * 5) % 61) AS l_commitdate,
    -- receiptdate: shipdate + 1..30 days
    DATE '1992-01-01' + ((o::bigint * 2609) % 2557)::int + 1 + ((o * 3 + ln * 11) % 121) + 1 + ((o + ln * 13) % 30) AS l_receiptdate,
    (ARRAY['DELIVER IN PERSON','COLLECT COD','NONE','TAKE BACK RETURN'])[1 + (o + ln) % 4] AS l_shipinstruct,
    (ARRAY['REG AIR','AIR','RAIL','SHIP','TRUCK','MAIL','FOB'])[1 + (o * 3 + ln) % 7] AS l_shipmode,
    'lineitem comment ' || o || '-' || ln AS l_comment
FROM generate_series(1, __SF_ORDERS__) AS orders(o),
     -- Variable number of lineitems per order: 1–7 based on order key
     generate_series(1, 1 + (o % 7)) AS lines(ln);

-- ── ANALYZE all tables for stable query plans ──────────────────────

-- Q17 has a correlated lookup by part key; make that access path available.
CREATE INDEX IF NOT EXISTS lineitem_l_partkey_idx ON lineitem (l_partkey);

ANALYZE region;
ANALYZE nation;
ANALYZE supplier;
ANALYZE part;
ANALYZE partsupp;
ANALYZE customer;
ANALYZE orders;
ANALYZE lineitem;
