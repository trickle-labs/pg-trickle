//! Small, exhaustive semantic domains rendered into the existing replay format.

use super::{
    ExecutionSettings, ExpectedCapability, FeatureVector, Mutation, MutationCycle, QuerySpec,
    Scenario, SchemaSpec,
};

fn case(id: &str, query: &str, columns: &[&str], rows: &str, changes: &[(&str, u64)]) -> Scenario {
    let schema = format!("sem_{id}");
    let qualify = |sql: &str| sql.replace("$s", &schema);
    Scenario {
        scenario_id: id.to_string(),
        format_version: super::SCENARIO_FORMAT_VERSION,
        generator_version: "semantic-boundaries-v1".to_string(),
        seed: 1217,
        preserve_sql_boundaries: true,
        schema: SchemaSpec {
            name: schema.clone(),
            setup_sql: vec![
                "ALTER SYSTEM SET pg_trickle.enabled = false".to_string(),
                "ALTER SYSTEM SET pg_trickle.cdc_mode = 'trigger'".to_string(),
                "SELECT pg_reload_conf()".to_string(),
                qualify(
                    "CREATE TABLE $s.input (id INT PRIMARY KEY DEFERRABLE INITIALLY DEFERRED, k INT, v NUMERIC, w INT)",
                ),
            ],
        },
        initial_data: vec![qualify(rows)],
        query: QuerySpec {
            stream_table: format!("{schema}.result"),
            defining_query: qualify(query),
            columns: columns.iter().map(|column| (*column).to_string()).collect(),
        },
        cycles: changes
            .iter()
            .enumerate()
            .map(|(i, (sql, affected))| MutationCycle {
                name: format!("{id}_{i}"),
                mutations: vec![Mutation {
                    sql: qualify(sql),
                    expected_affected_rows: *affected,
                }],
                changed_leaves: vec![format!("{schema}.input")],
                mutation_intents: vec![id.to_string()],
            })
            .collect(),
        execution: ExecutionSettings {
            schedule: "24h".to_string(),
            requested_refresh_mode: "DIFFERENTIAL".to_string(),
            require_differential_history: true,
            observe_numeric_scale: false,
        },
        expected_capability: ExpectedCapability {
            differential: true,
            expected_mode: "DIFFERENTIAL".to_string(),
        },
        features: FeatureVector {
            aggregates: vec![],
            joins: vec![],
            simultaneous_source_changes: false,
            nullable_groups: true,
            duplicate_rows: true,
            changed_leaf_buckets: vec![1],
            mutation_intents: vec![id.to_string()],
        },
    }
}

/// Every unordered bag of size <= 2 over NULL, -1, 0, 1, including duplicates.
pub fn small_bags() -> Vec<Vec<&'static str>> {
    bags_through(2)
}

pub fn bags_through(max_size: usize) -> Vec<Vec<&'static str>> {
    let values = ["NULL", "-1", "0", "1"];
    let mut bags = vec![vec![]];
    let mut level = vec![(vec![], 0)];
    for _ in 0..max_size {
        let mut next = Vec::new();
        for (prefix, start) in level {
            for (i, value) in values.iter().enumerate().skip(start) {
                let mut bag = prefix.clone();
                bag.push(*value);
                bags.push(bag.clone());
                next.push((bag, i));
            }
        }
        level = next;
    }
    bags
}

pub fn bounded_cases() -> Vec<Scenario> {
    cases_for_bags(2)
}

pub fn cases_for_bags(max_size: usize) -> Vec<Scenario> {
    assert!(
        (2..=3).contains(&max_size),
        "bounded PR/scheduled domains only"
    );
    let mut cases = Vec::new();
    for (i, bag) in bags_through(max_size).iter().enumerate() {
        let rows = if bag.is_empty() {
            "INSERT INTO $s.input SELECT 1, 1, NULL, NULL WHERE false".to_string()
        } else {
            format!(
                "INSERT INTO $s.input VALUES {}",
                bag.iter()
                    .enumerate()
                    .map(|(j, v)| format!("({}, 1, {v}, NULL)", j + 1))
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        };
        cases.push(case(&format!("count_bag_{i}"),
            "SELECT k, COUNT(*) AS rows, COUNT(v) AS arguments, COUNT(ROW(v,w)) AS records FROM $s.input GROUP BY k", &["k", "rows", "arguments", "records"], &rows,
            &[("INSERT INTO $s.input VALUES (100, 1, NULL, NULL)", 1),
              ("UPDATE $s.input SET v = -1 WHERE id = 100", 1),
              ("DELETE FROM $s.input WHERE id = 100", 1)]));
    }
    for (old, old_value) in ["1", "-1", "NULL"].iter().enumerate() {
        for (new, new_value) in ["2", "-2", "NULL"].iter().enumerate() {
            let observed = |truth: usize| {
                format!(
                    "DO $observe$ BEGIN IF (SELECT coalesce((SUM(v) > 0)::text, 'unknown') FROM $s.input) <> '{}' THEN RAISE EXCEPTION 'truth boundary not reached'; END IF; END $observe$",
                    ["true", "false", "unknown"][truth]
                )
            };
            cases.push(case(&format!("truth_{old}_{new}"),
                "SELECT k, SUM(v) AS total, COUNT(v) AS arguments, COUNT(w) AS payload FROM $s.input GROUP BY k HAVING SUM(v) > 0",
                &["k", "total", "arguments", "payload"],
                &format!("INSERT INTO $s.input VALUES (1,1,{old_value},NULL)"),
                &[(&format!("{}; UPDATE $s.input SET v = {new_value}, w = 1 WHERE id = 1; {}", observed(old), observed(new)), 1)]));
        }
    }
    let mut numeric = case(
        "numeric_observations",
        "SELECT id, v FROM $s.input",
        &["id", "v"],
        "INSERT INTO $s.input VALUES (1,1,1.0,NULL)",
        &[
            ("UPDATE $s.input SET v = 1.00 WHERE id = 1", 1),
            ("UPDATE $s.input SET v = 'Infinity' WHERE id = 1", 1),
            ("UPDATE $s.input SET v = '-Infinity' WHERE id = 1", 1),
            ("UPDATE $s.input SET v = 'NaN' WHERE id = 1", 1),
            ("UPDATE $s.input SET v = -0.00 WHERE id = 1", 1),
        ],
    );
    numeric.execution.observe_numeric_scale = true;
    cases.push(numeric);
    for (name, expression) in [
        ("filtered_argument", "SUM(100 / v) FILTER (WHERE v <> 0)"),
        (
            "case_argument",
            "SUM(CASE WHEN v <> 0 THEN 100 / v ELSE NULL END)",
        ),
    ] {
        cases.push(case(
            name,
            &format!("SELECT k, {expression} AS total FROM $s.input GROUP BY k"),
            &["k", "total"],
            "INSERT INTO $s.input VALUES (1,1,0,NULL),(2,1,-1,NULL),(3,1,NULL,NULL)",
            &[
                ("UPDATE $s.input SET v = 1 WHERE id = 2", 1),
                ("UPDATE $s.input SET v = 0 WHERE id = 2", 1),
                ("UPDATE $s.input SET v = -1 WHERE id = 1", 1),
            ],
        ));
    }
    cases.push(case("deferred_payload", "SELECT id, k, v, w FROM $s.input",
        &["id", "k", "v", "w"],
        "INSERT INTO $s.input VALUES (1,1,1,NULL)",
        &[("BEGIN; INSERT INTO $s.input VALUES (1,2,-1,4); DELETE FROM $s.input WHERE k = 1; COMMIT", 2),
          ("BEGIN; UPDATE $s.input SET v = 10; SAVEPOINT discarded; UPDATE $s.input SET v = 99; ROLLBACK TO discarded; UPDATE $s.input SET w = 3; COMMIT", 3),
          ("BEGIN; UPDATE $s.input SET v = 99; ROLLBACK; UPDATE $s.input SET w = 4", 2)]));
    cases.push(case(
        "special_sum",
        "SELECT k, SUM(v) AS total, AVG(v) AS average FROM $s.input GROUP BY k",
        &["k", "total", "average"],
        "INSERT INTO $s.input VALUES (1,1,'Infinity',NULL),(2,1,1,NULL)",
        &[
            ("DELETE FROM $s.input WHERE id = 1", 1),
            ("UPDATE $s.input SET v = '-Infinity' WHERE id = 2", 1),
            ("UPDATE $s.input SET v = 'NaN' WHERE id = 2", 1),
            ("UPDATE $s.input SET v = NULL WHERE id = 2", 1),
            ("UPDATE $s.input SET v = -1 WHERE id = 2", 1),
        ],
    ));
    cases.push(case("ternary_partition",
        "SELECT id,k,v FROM $s.input WHERE (v>0) IS TRUE UNION ALL SELECT id,k,v FROM $s.input WHERE (v>0) IS FALSE UNION ALL SELECT id,k,v FROM $s.input WHERE (v>0) IS UNKNOWN",
        &["id", "k", "v"],
        "INSERT INTO $s.input VALUES (1,1,NULL,NULL),(2,1,-1,NULL),(3,1,0,NULL),(4,1,1,NULL)",
        &[("UPDATE $s.input SET v = -v", 4), ("UPDATE $s.input SET v = NULL WHERE id = 4", 1)]));
    cases.push(case("nested_count",
        "WITH projected AS (SELECT k,v FROM $s.input), grouped AS (SELECT k,COUNT(v) AS arguments FROM projected GROUP BY k) SELECT g.k,g.arguments FROM grouped g JOIN (SELECT k FROM $s.input GROUP BY k) keys ON g.k=keys.k",
        &["k", "arguments"],
        "INSERT INTO $s.input VALUES (1,1,NULL,NULL),(2,1,-1,NULL)",
        &[("UPDATE $s.input SET v = 0 WHERE id = 1", 1), ("UPDATE $s.input SET k = 2 WHERE id = 2", 1)]));
    let mut mixed = case(
        "mixed_join",
        "SELECT l.id,100/(l.v-r.v) AS quotient FROM $s.input l JOIN $s.rhs r ON l.id=r.id",
        &["id", "quotient"],
        "INSERT INTO $s.input VALUES (1,1,1,NULL)",
        &[
            (
                "BEGIN; UPDATE $s.input SET v=2; UPDATE $s.rhs SET v=1; COMMIT",
                2,
            ),
            (
                "BEGIN; UPDATE $s.input SET v=1; UPDATE $s.rhs SET v=0; COMMIT",
                2,
            ),
        ],
    );
    mixed
        .schema
        .setup_sql
        .push("CREATE TABLE sem_mixed_join.rhs (id INT PRIMARY KEY, v NUMERIC)".to_string());
    mixed
        .initial_data
        .push("INSERT INTO sem_mixed_join.rhs VALUES (1,0)".to_string());
    mixed.features.simultaneous_source_changes = true;
    mixed.features.joins.push("inner".to_string());
    cases.push(mixed);
    for all_first in [true, false] {
        let id = if all_first {
            "all_first"
        } else {
            "narrow_first"
        };
        let mut consumers = case(
            id,
            "SELECT id,v FROM $s.input",
            &["id", "v"],
            "INSERT INTO $s.input VALUES (1,1,1.0,NULL)",
            &[
                ("UPDATE $s.input SET v=1.00 WHERE id=1", 1),
                ("UPDATE $s.input SET v=1.0 WHERE id=1", 1),
                ("UPDATE $s.input SET v=NULL WHERE id=1", 1),
                ("UPDATE $s.input SET v=1.00 WHERE id=1", 1),
            ],
        );
        consumers.execution.observe_numeric_scale = true;
        let create = format!(
            "SELECT pgtrickle.create_stream_table('sem_{id}.all_columns', 'SELECT id,k,v,w FROM sem_{id}.input', '24h', 'DIFFERENTIAL')"
        );
        if all_first {
            consumers.initial_data.push(create);
        } else {
            consumers.cycles[0].mutations.insert(
                0,
                Mutation {
                    sql: create,
                    expected_affected_rows: 1,
                },
            );
        }
        // Advancing the other consumer first must not discard a narrow window.
        for cycle in &mut consumers.cycles {
            cycle.mutations.push(Mutation {
                sql: format!("SELECT pgtrickle.refresh_stream_table('sem_{id}.all_columns')"),
                expected_affected_rows: 1,
            });
        }
        cases.push(consumers);
    }
    cases
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn semantic_domains_are_exhaustive_and_replayable() {
        let bags = small_bags();
        assert_eq!(bags.len(), 15);
        let cases = bounded_cases();
        assert_eq!(cases.len(), 34);
        assert_eq!(bags_through(3).len(), 35);
        assert_eq!(cases_for_bags(3).len(), 54);
        let observed: Vec<_> = cases
            .iter()
            .filter(|scenario| scenario.execution.observe_numeric_scale)
            .map(|scenario| scenario.scenario_id.as_str())
            .collect();
        assert_eq!(
            observed,
            vec!["numeric_observations", "all_first", "narrow_first"]
        );
        let mut ids = std::collections::BTreeSet::new();
        for scenario in cases {
            assert!(ids.insert(scenario.scenario_id.clone()));
            super::super::validate_scenario(&scenario).expect("valid generated replay");
            let json = serde_json::to_string(&scenario).expect("serialize replay");
            let decoded: Scenario = serde_json::from_str(&json).expect("deserialize replay");
            assert_eq!(decoded.query.defining_query, scenario.query.defining_query);
            assert_eq!(
                decoded.execution.observe_numeric_scale,
                scenario.execution.observe_numeric_scale
            );
        }
    }
}
