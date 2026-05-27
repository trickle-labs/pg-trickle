#![allow(dead_code)]

use super::E2eDb;

const DEFAULT_CASES: usize = 3;
const DEFAULT_CYCLES: usize = 4;
const DEFAULT_INITIAL_ROWS: usize = 12;
const SEED_STRIDE: u64 = 0x9e37_79b9_7f4a_7c15;

#[derive(Clone, Copy, Debug)]
pub struct TraceConfig {
    pub cases: usize,
    pub cycles: usize,
    pub initial_rows: usize,
    pub seed_override: Option<u64>,
}

impl TraceConfig {
    pub fn from_env() -> Self {
        Self {
            cases: env_usize("PGS_PROP_CASES", DEFAULT_CASES),
            cycles: env_usize("PGS_PROP_CYCLES", DEFAULT_CYCLES),
            initial_rows: env_usize("PGS_PROP_INITIAL_ROWS", DEFAULT_INITIAL_ROWS),
            seed_override: env_u64("PGS_PROP_SEED"),
        }
    }

    pub fn seeds(&self, base_seed: u64) -> Vec<u64> {
        if let Some(seed) = self.seed_override {
            return vec![seed];
        }

        (0..self.cases)
            .map(|index| base_seed.wrapping_add((index as u64).wrapping_mul(SEED_STRIDE)))
            .collect()
    }
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

fn env_u64(name: &str) -> Option<u64> {
    let raw = std::env::var(name).ok()?;
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }

    if let Some(hex) = trimmed
        .strip_prefix("0x")
        .or_else(|| trimmed.strip_prefix("0X"))
    {
        u64::from_str_radix(hex, 16).ok()
    } else {
        trimmed.parse::<u64>().ok()
    }
}

#[derive(Debug, Clone)]
pub struct SeededRng {
    state: u64,
}

impl SeededRng {
    pub fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    pub fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(SEED_STRIDE);
        let mut value = self.state;
        value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        value ^ (value >> 31)
    }

    pub fn usize_range(&mut self, min: usize, max: usize) -> usize {
        if min >= max {
            return min;
        }

        min + (self.next_u64() as usize) % (max - min + 1)
    }

    pub fn i32_range(&mut self, min: i32, max: i32) -> i32 {
        if min >= max {
            return min;
        }

        let span = (max as i64 - min as i64 + 1) as u64;
        min + (self.next_u64() % span) as i32
    }

    pub fn choose<'a, T>(&mut self, items: &'a [T]) -> &'a T {
        assert!(!items.is_empty(), "cannot choose from an empty slice");
        &items[self.usize_range(0, items.len() - 1)]
    }

    pub fn gen_alpha(&mut self, len: usize) -> String {
        (0..len)
            .map(|_| (b'a' + (self.next_u64() % 26) as u8) as char)
            .collect()
    }

    pub fn gen_bool(&mut self) -> bool {
        self.next_u64().is_multiple_of(2)
    }
}

#[derive(Debug, Clone)]
pub struct TrackedIds {
    next_id: i32,
    live: Vec<i32>,
}

impl TrackedIds {
    pub fn new() -> Self {
        Self {
            next_id: 1,
            live: Vec::new(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.live.is_empty()
    }

    pub fn alloc(&mut self) -> i32 {
        let id = self.next_id;
        self.next_id += 1;
        self.live.push(id);
        id
    }

    pub fn next_unused_id(&self) -> i32 {
        self.next_id
    }

    pub fn pick(&self, rng: &mut SeededRng) -> Option<i32> {
        if self.live.is_empty() {
            None
        } else {
            Some(*rng.choose(&self.live))
        }
    }

    pub fn remove_random(&mut self, rng: &mut SeededRng) -> Option<i32> {
        if self.live.is_empty() {
            return None;
        }

        let index = rng.usize_range(0, self.live.len() - 1);
        Some(self.live.swap_remove(index))
    }
}

pub async fn assert_st_query_invariant(
    db: &E2eDb,
    st_table: &str,
    defining_query: &str,
    seed: u64,
    cycle: usize,
    step: &str,
) {
    let cols_sql = format!(
        "SELECT string_agg(column_name, ', ' ORDER BY ordinal_position), \
                string_agg(\
                    CASE WHEN data_type = 'json' \
                         THEN column_name || '::text' \
                         ELSE column_name END, \
                    ', ' ORDER BY ordinal_position) \
         FROM information_schema.columns \
         WHERE (table_schema || '.' || table_name = '{st_table}' \
            OR table_name = '{st_table}') \
         AND column_name NOT LIKE '__pgt_%'"
    );
    let (raw_cols, cast_cols): (Option<String>, Option<String>) =
        sqlx::query_as(sqlx::AssertSqlSafe(cols_sql))
            .fetch_one(&db.pool)
            .await
            .unwrap_or_else(|error| panic!("cols query failed for '{st_table}': {error}"));
    let raw_cols = raw_cols.unwrap_or_else(|| "*".to_string());
    let cast_cols = cast_cols.unwrap_or_else(|| "*".to_string());
    let selected_cols = if raw_cols == cast_cols {
        raw_cols
    } else {
        cast_cols
    };

    let has_dual_counts: bool = db
        .query_scalar(&format!(
            "SELECT EXISTS( \
                SELECT 1 FROM information_schema.columns \
                WHERE (table_schema || '.' || table_name = '{st_table}' \
                   OR table_name = '{st_table}') \
                AND column_name = '__pgt_count_l')"
        ))
        .await;

    let query_upper = defining_query.to_uppercase();
    let set_op_filter = if has_dual_counts {
        if query_upper.contains("INTERSECT ALL") {
            " WHERE LEAST(__pgt_count_l, __pgt_count_r) > 0"
        } else if query_upper.contains("INTERSECT") {
            " WHERE __pgt_count_l > 0 AND __pgt_count_r > 0"
        } else if query_upper.contains("EXCEPT ALL") {
            " WHERE __pgt_count_l > __pgt_count_r"
        } else if query_upper.contains("EXCEPT") {
            " WHERE __pgt_count_l > 0 AND __pgt_count_r = 0"
        } else {
            ""
        }
    } else {
        ""
    };

    let st_select = format!("SELECT {selected_cols} FROM {st_table}{set_op_filter}");
    let query_select = format!("SELECT {selected_cols} FROM ({defining_query}) __pgt_dq");

    let matches: bool = db
        .query_scalar(&format!(
            "SELECT NOT EXISTS ( \
                (({st_select}) EXCEPT ALL ({query_select})) \
                UNION ALL \
                (({query_select}) EXCEPT ALL ({st_select})) \
            )"
        ))
        .await;

    if matches {
        return;
    }

    let st_count: i64 = db
        .query_scalar(&format!(
            "SELECT count(*) FROM ({st_select}) __pgt_st_count"
        ))
        .await;
    let query_count: i64 = db
        .query_scalar(&format!(
            "SELECT count(*) FROM ({query_select}) __pgt_query_count"
        ))
        .await;
    let extra_rows: i64 = db
        .query_scalar(&format!(
            "SELECT count(*) FROM ((({st_select}) EXCEPT ALL ({query_select}))) __pgt_extra"
        ))
        .await;
    let missing_rows: i64 = db
        .query_scalar(&format!(
            "SELECT count(*) FROM ((({query_select}) EXCEPT ALL ({st_select}))) __pgt_missing"
        ))
        .await;

    panic!(
        "INVARIANT VIOLATED at cycle {cycle} (seed={seed:#x})\n\
         Step: {step}\n\
         ST: {st_table}\n\
         ST rows: {st_count}, Query rows: {query_count}\n\
         Extra in ST: {extra_rows}, Missing from ST: {missing_rows}\n\
         Query: {defining_query}"
    );
}

pub async fn assert_st_query_invariants(
    db: &E2eDb,
    invariants: &[(&str, &str)],
    seed: u64,
    cycle: usize,
    step: &str,
) {
    for (st_table, defining_query) in invariants {
        assert_st_query_invariant(db, st_table, defining_query, seed, cycle, step).await;
    }
}

pub async fn snapshot_data_timestamps(db: &E2eDb, names: &[&str]) -> Vec<String> {
    let mut timestamps = Vec::with_capacity(names.len());
    for name in names {
        timestamps.push(data_timestamp_text(db, name).await);
    }
    timestamps
}

pub async fn assert_data_timestamps_stable(
    db: &E2eDb,
    names: &[&str],
    expected: &[String],
    seed: u64,
    cycle: usize,
    step: &str,
) {
    assert_eq!(
        names.len(),
        expected.len(),
        "names/expected length mismatch for timestamp stability check"
    );

    for (index, name) in names.iter().enumerate() {
        let current = data_timestamp_text(db, name).await;
        assert_eq!(
            current, expected[index],
            "data_timestamp drift for '{name}' at cycle {cycle} (seed={seed:#x}, step={step})"
        );
    }
}

async fn data_timestamp_text(db: &E2eDb, name: &str) -> String {
    db.query_scalar(&format!(
        "SELECT COALESCE(data_timestamp::text, 'null') \
         FROM pgtrickle.pgt_stream_tables \
         WHERE pgt_name = '{name}'"
    ))
    .await
}
