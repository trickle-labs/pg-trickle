//! Pure, deterministic source-state mutations for the v0.87.4 DVM fuzzer.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceState {
    pub leaves: Vec<LeafState>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LeafState {
    pub name: String,
    pub rows: Vec<Row>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Row {
    pub id: u32,
    pub group: Option<i32>,
    pub key: Option<i32>,
    pub value: i32,
    pub unused: i32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChangedLeaves {
    One,
    Two,
    All,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Intent {
    CreateGroup,
    DeleteGroup,
    MinWinnerReplacement,
    MaxWinnerReplacement,
    MoveGroup,
    JoinMatchTransition,
    NullableKeyTransition,
    ValueChange,
    KeyChange,
    UnusedColumnChange,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Mutation {
    pub intent: Intent,
    pub leaves: Vec<usize>,
    pub row_ids: Vec<u32>,
    pub before: SourceState,
    pub after: SourceState,
}

impl SourceState {
    pub fn seed() -> Self {
        Self {
            leaves: vec![
                LeafState {
                    name: "left".into(),
                    rows: vec![
                        row(1, Some(1), Some(7), 10),
                        row(2, Some(1), Some(7), 20),
                        row(3, Some(2), Some(9), 30),
                    ],
                },
                LeafState {
                    name: "right".into(),
                    rows: vec![
                        row(11, Some(1), Some(7), 100),
                        row(12, Some(3), Some(8), 200),
                        row(13, Some(4), None, 300),
                        row(14, Some(4), Some(6), 350),
                    ],
                },
                LeafState {
                    name: "third".into(),
                    rows: vec![row(21, Some(5), Some(9), 400)],
                },
            ],
        }
    }

    fn row_mut(&mut self, leaf: usize, id: u32) -> Option<&mut Row> {
        self.leaves
            .get_mut(leaf)?
            .rows
            .iter_mut()
            .find(|r| r.id == id)
    }

    fn row(&self, leaf: usize, id: u32) -> Option<&Row> {
        self.leaves.get(leaf)?.rows.iter().find(|r| r.id == id)
    }

    pub fn apply(&mut self, mutation: &Mutation) -> bool {
        if *self != mutation.before {
            return false;
        }
        *self = mutation.after.clone();
        mutation.before != mutation.after
    }

    pub fn changed_leaves(&self, other: &Self) -> Vec<usize> {
        self.leaves
            .iter()
            .zip(&other.leaves)
            .enumerate()
            .filter_map(|(index, (before, after))| (before != after).then_some(index))
            .collect()
    }
}

fn row(id: u32, group: Option<i32>, key: Option<i32>, value: i32) -> Row {
    Row {
        id,
        group,
        key,
        value,
        unused: 0,
    }
}

pub fn plan(seed: u64, bucket: ChangedLeaves) -> Vec<Mutation> {
    let mut state = SourceState::seed();
    let intents = [
        Intent::CreateGroup,
        Intent::DeleteGroup,
        Intent::MinWinnerReplacement,
        Intent::MaxWinnerReplacement,
        Intent::MoveGroup,
        Intent::JoinMatchTransition,
        Intent::NullableKeyTransition,
        Intent::ValueChange,
        Intent::KeyChange,
        Intent::UnusedColumnChange,
    ];
    let mut planned = Vec::with_capacity(intents.len());
    for (index, intent) in intents.into_iter().enumerate() {
        let leaves = selected_leaves(seed, bucket, state.leaves.len(), index);
        let mutation = make_mutation(&state, intent, &leaves, index);
        assert!(
            state.apply(&mutation),
            "planner emitted an ineffective mutation"
        );
        let mut changed = mutation.before.changed_leaves(&mutation.after);
        changed.sort_unstable();
        let mut selected = leaves.clone();
        selected.sort_unstable();
        assert_eq!(changed, selected, "planner must change every selected leaf");
        planned.push(mutation);
    }
    planned
}

fn selected_leaves(seed: u64, bucket: ChangedLeaves, count: usize, index: usize) -> Vec<usize> {
    let start = (seed as usize + index) % count;
    match bucket {
        ChangedLeaves::One => vec![start],
        ChangedLeaves::Two => {
            let mut selected = vec![start, (start + 1) % count];
            selected.sort_unstable();
            selected
        }
        ChangedLeaves::All => (0..count).collect(),
    }
}

fn make_mutation(state: &SourceState, intent: Intent, leaves: &[usize], index: usize) -> Mutation {
    let before = state.clone();
    let mut after = before.clone();
    let leaf = leaves[0];
    let id = after.leaves[leaf].rows[0].id;
    let mut row_ids = vec![id];
    match intent {
        Intent::CreateGroup => {
            for (offset, &leaf) in leaves.iter().enumerate() {
                let id = after.leaves[leaf].rows[0].id;
                if let Some(row) = after.row_mut(leaf, id) {
                    row.group = Some(100 + index as i32 + offset as i32);
                }
            }
        }
        Intent::DeleteGroup => {
            for &leaf in leaves {
                let id = after.leaves[leaf].rows[0].id;
                if let Some(row) = after.row_mut(leaf, id) {
                    row.group = None;
                }
            }
        }
        Intent::MinWinnerReplacement => {
            for &leaf in leaves {
                let id = winner_id(&after.leaves[leaf], true)
                    .unwrap_or_else(|| after.leaves[leaf].rows[0].id);
                if let Some(row) = after.row_mut(leaf, id) {
                    row.value -= 1;
                }
            }
        }
        Intent::MaxWinnerReplacement => {
            for &leaf in leaves {
                let id = winner_id(&after.leaves[leaf], false)
                    .unwrap_or_else(|| after.leaves[leaf].rows[0].id);
                if let Some(row) = after.row_mut(leaf, id) {
                    row.value += 1000;
                }
            }
        }
        Intent::MoveGroup => {
            for (offset, &leaf) in leaves.iter().enumerate() {
                let id = after.leaves[leaf].rows[0].id;
                if let Some(row) = after.row_mut(leaf, id) {
                    row.group = Some(200 + index as i32 + offset as i32);
                }
            }
        }
        Intent::JoinMatchTransition => {
            let key = Some(700 + index as i32);
            for &leaf in leaves {
                let id = after.leaves[leaf].rows[0].id;
                if let Some(row) = after.row_mut(leaf, id) {
                    row.key = key;
                }
            }
            row_ids.extend(
                leaves
                    .iter()
                    .skip(1)
                    .map(|&leaf| after.leaves[leaf].rows[0].id),
            );
        }
        Intent::NullableKeyTransition => {
            for &leaf in leaves {
                let id = after.leaves[leaf].rows[0].id;
                if let Some(row) = after.row_mut(leaf, id) {
                    row.key = None;
                }
            }
        }
        Intent::ValueChange => {
            for &leaf in leaves {
                let id = after.leaves[leaf].rows[0].id;
                if let Some(row) = after.row_mut(leaf, id) {
                    row.value += 7;
                }
            }
        }
        Intent::KeyChange => {
            for (offset, &leaf) in leaves.iter().enumerate() {
                let id = after.leaves[leaf].rows[0].id;
                if let Some(row) = after.row_mut(leaf, id) {
                    row.key = Some(700 + index as i32 + offset as i32);
                }
            }
        }
        Intent::UnusedColumnChange => {
            for &leaf in leaves {
                let id = after.leaves[leaf].rows[0].id;
                if let Some(row) = after.row_mut(leaf, id) {
                    row.unused += 1;
                }
            }
        }
    }
    Mutation {
        intent,
        leaves: leaves.to_vec(),
        row_ids,
        before,
        after,
    }
}

fn winner_id(leaf: &LeafState, minimum: bool) -> Option<u32> {
    leaf.rows.iter().find_map(|candidate| {
        let group = candidate.group?;
        let mut members = leaf.rows.iter().filter(|row| row.group == Some(group));
        let first = members.next()?;
        let second = members.next()?;
        [first, second]
            .into_iter()
            .min_by_key(|row| if minimum { row.value } else { -row.value })
            .map(|row| row.id)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_intent_is_effective_and_leaf_buckets_are_selectable() {
        for bucket in [ChangedLeaves::One, ChangedLeaves::Two, ChangedLeaves::All] {
            let mutations = plan(17, bucket);
            assert_eq!(mutations.len(), 10);
            assert!(mutations.iter().all(|m| m.before != m.after));
            assert!(mutations.iter().all(|m| m.leaves.len()
                == match bucket {
                    ChangedLeaves::One => 1,
                    ChangedLeaves::Two => 2,
                    ChangedLeaves::All => 3,
                }));
            let mut state = SourceState::seed();
            for mutation in &mutations {
                assert!(state.apply(mutation));
            }
        }
    }
}
