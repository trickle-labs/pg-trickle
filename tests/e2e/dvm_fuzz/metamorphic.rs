//! Pure metamorphic specifications for DVM refresh scenarios.

use std::collections::BTreeMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetamorphicFamily {
    RefreshBatching,
    UpdateVsDeleteInsert,
    CteVsInline,
    AliasRenaming,
    IrrelevantColumnWidening,
    InnerJoinReorder,
    ProjectionPlacement,
    Idempotence,
    NoOpMutation,
    MutationOrder,
}

impl MetamorphicFamily {
    pub const fn all() -> &'static [Self] {
        &[
            Self::RefreshBatching,
            Self::UpdateVsDeleteInsert,
            Self::CteVsInline,
            Self::AliasRenaming,
            Self::IrrelevantColumnWidening,
            Self::InnerJoinReorder,
            Self::ProjectionPlacement,
            Self::Idempotence,
            Self::NoOpMutation,
            Self::MutationOrder,
        ]
    }

    pub const fn name(self) -> &'static str {
        match self {
            Self::RefreshBatching => "refresh_batching",
            Self::UpdateVsDeleteInsert => "update_vs_delete_insert",
            Self::CteVsInline => "cte_vs_inline",
            Self::AliasRenaming => "alias_renaming",
            Self::IrrelevantColumnWidening => "irrelevant_column_widening",
            Self::InnerJoinReorder => "inner_join_reorder",
            Self::ProjectionPlacement => "projection_placement",
            Self::Idempotence => "idempotence",
            Self::NoOpMutation => "no_op_mutation",
            Self::MutationOrder => "mutation_order",
        }
    }

    pub fn transform(self, base: &Scenario) -> Scenario {
        let mut transformed = base.clone();
        match self {
            Self::RefreshBatching => transformed.batches = vec![transformed.mutations.clone()],
            Self::UpdateVsDeleteInsert => {
                transformed.mutations = transformed
                    .mutations
                    .iter()
                    .flat_map(|mutation| match *mutation {
                        Mutation::Update { key, value } => {
                            vec![Mutation::Delete { key }, Mutation::Insert { key, value }]
                        }
                        mutation => vec![mutation],
                    })
                    .collect();
            }
            Self::CteVsInline => {
                transformed.query.sql = transformed.query.sql.replace(
                    "WITH source AS (SELECT * FROM input) SELECT * FROM source",
                    "SELECT * FROM input",
                )
            }
            Self::AliasRenaming => {
                transformed.query.sql = transformed.query.sql.replace("input", "renamed_input")
            }
            Self::IrrelevantColumnWidening => {
                transformed.query.sql = transformed
                    .query
                    .sql
                    .replace("SELECT key, value", "SELECT key, value, ignored")
            }
            Self::InnerJoinReorder => {
                transformed.query.sql = transformed
                    .query
                    .sql
                    .replace("left JOIN right", "right JOIN left")
            }
            Self::ProjectionPlacement => {
                transformed.query.sql = transformed.query.sql.replace(
                    "SELECT key, value FROM input",
                    "SELECT key, value FROM (SELECT * FROM input) projected",
                )
            }
            Self::Idempotence => {
                transformed.mutations = transformed
                    .mutations
                    .iter()
                    .flat_map(|mutation| [*mutation, *mutation])
                    .collect()
            }
            Self::NoOpMutation => transformed.mutations.push(Mutation::NoOp),
            Self::MutationOrder => transformed.mutations.reverse(),
        }
        transformed
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuerySpec {
    pub sql: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mutation {
    Insert { key: i32, value: i32 },
    Update { key: i32, value: i32 },
    Delete { key: i32 },
    NoOp,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Scenario {
    pub query: QuerySpec,
    pub initial: Vec<(i32, i32)>,
    pub mutations: Vec<Mutation>,
    pub batches: Vec<Vec<Mutation>>,
}

impl Scenario {
    pub fn new(
        sql: impl Into<String>,
        initial: impl Into<Vec<(i32, i32)>>,
        mutations: impl Into<Vec<Mutation>>,
    ) -> Self {
        let mutations = mutations.into();
        Self {
            query: QuerySpec { sql: sql.into() },
            initial: initial.into(),
            batches: vec![mutations.clone()],
            mutations,
        }
    }

    pub fn final_state(&self) -> BTreeMap<i32, i32> {
        let mut state = self.initial.iter().copied().collect();
        for mutation in &self.mutations {
            apply(&mut state, *mutation);
        }
        state
    }

    pub fn batched_final_state(&self) -> BTreeMap<i32, i32> {
        let mut state = self.initial.iter().copied().collect();
        for batch in &self.batches {
            for mutation in batch {
                apply(&mut state, *mutation);
            }
        }
        state
    }
}

fn apply(state: &mut BTreeMap<i32, i32>, mutation: Mutation) {
    match mutation {
        Mutation::Insert { key, value } | Mutation::Update { key, value } => {
            state.insert(key, value);
        }
        Mutation::Delete { key } => {
            state.remove(&key);
        }
        Mutation::NoOp => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base() -> Scenario {
        Scenario::new(
            "WITH source AS (SELECT * FROM input) SELECT key, value FROM input",
            [(1, 10), (2, 20)],
            [
                Mutation::Update { key: 1, value: 99 },
                Mutation::Delete { key: 2 },
            ],
        )
    }

    #[test]
    fn inventory_has_at_least_six_families() {
        assert!(MetamorphicFamily::all().len() >= 6);
    }

    #[test]
    fn batching_preserves_model_final_state() {
        let mut scenario = base();
        scenario.batches = vec![vec![scenario.mutations[0]], vec![scenario.mutations[1]]];
        assert_eq!(scenario.final_state(), scenario.batched_final_state());
    }

    #[test]
    fn update_decomposition_preserves_model_final_state() {
        let scenario = base();
        let decomposed = MetamorphicFamily::UpdateVsDeleteInsert.transform(&scenario);
        let mut expected = scenario.clone();
        expected.mutations[0] = Mutation::Update { key: 1, value: 99 };
        assert_eq!(expected.final_state(), decomposed.final_state());
    }
}
