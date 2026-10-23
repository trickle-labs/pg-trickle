//! Production visibility and aggregate-lifecycle decisions.
//! SQL evaluation, count arithmetic and lowering are qualified separately.

use vstd::prelude::*;

verus! {
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RowActions {
    pub retract: bool,
    pub insert: bool,
}

pub open spec fn visible(exists: bool, predicate: Option<bool>) -> bool {
    exists && predicate == Some(true)
}

/// The old/new visible bags differ only when presence or the payload changes.
pub fn visibility_actions(
    old_exists: bool, old_predicate: Option<bool>,
    new_exists: bool, new_predicate: Option<bool>, payload_changed: bool,
) -> (actions: RowActions)
    ensures
        actions.retract == (visible(old_exists, old_predicate)
            && (!visible(new_exists, new_predicate) || payload_changed)),
        actions.insert == (visible(new_exists, new_predicate)
            && (!visible(old_exists, old_predicate) || payload_changed)),
{
    let old_visible = old_exists && old_predicate == Some(true);
    let new_visible = new_exists && new_predicate == Some(true);
    RowActions {
        retract: old_visible && (!new_visible || payload_changed),
        insert: new_visible && (!old_visible || payload_changed),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GroupAction { Insert, Update, Delete, Skip }

pub fn group_present(global: bool, has_rows: bool) -> (present: bool)
    ensures present == (global || has_rows),
{
    global || has_rows
}

pub fn group_action(global: bool, old_present: bool, new_has_rows: bool) -> (action: GroupAction)
    ensures
        (action == GroupAction::Insert) == ((global || new_has_rows) && !old_present),
        (action == GroupAction::Update) == ((global || new_has_rows) && old_present),
        (action == GroupAction::Delete) == (!(global || new_has_rows) && old_present),
        (action == GroupAction::Skip) == (!(global || new_has_rows) && !old_present),
{
    if group_present(global, new_has_rows) {
        if old_present { GroupAction::Update } else { GroupAction::Insert }
    } else {
        if old_present { GroupAction::Delete } else { GroupAction::Skip }
    }
}

/// Argument presence controls NULL output, independently of group row presence.
pub fn aggregate_is_null(nullable: bool, has_arguments: bool) -> (is_null: bool)
    ensures is_null == (nullable && !has_arguments),
{
    nullable && !has_arguments
}
}

// These adapters lower finite checked decisions; PostgreSQL conformance tests cover the SQL.
pub fn having_admission_sql(predicate: &str) -> String {
    let mut terms = Vec::new();
    for (truth, test) in [
        (Some(true), "IS TRUE"),
        (Some(false), "IS FALSE"),
        (None, "IS UNKNOWN"),
    ] {
        if visibility_actions(false, None, true, truth, false).insert {
            terms.push(format!("({predicate}) {test}"));
        }
    }
    if terms.is_empty() {
        "FALSE".to_string()
    } else {
        terms.join(" OR ")
    }
}

pub fn having_retraction_sql(predicate: &str) -> String {
    let on_false = visibility_actions(true, Some(true), true, Some(false), false).retract;
    let on_unknown = visibility_actions(true, Some(true), true, None, false).retract;
    match (on_false, on_unknown) {
        (true, true) => format!("({predicate}) IS NOT TRUE"),
        (true, false) => format!("({predicate}) IS FALSE"),
        (false, true) => format!("({predicate}) IS UNKNOWN"),
        (false, false) => "FALSE".to_string(),
    }
}

pub fn group_presence_sql(global: bool, count: &str) -> String {
    match (group_present(global, false), group_present(global, true)) {
        (true, true) => "TRUE".to_string(),
        (false, true) => format!("({count}) > 0"),
        (true, false) => format!("({count}) <= 0"),
        (false, false) => "FALSE".to_string(),
    }
}

fn action_code(action: GroupAction) -> &'static str {
    match action {
        GroupAction::Insert => "I",
        GroupAction::Update => "U",
        GroupAction::Delete => "D",
        GroupAction::Skip => "N",
    }
}

pub fn group_action_sql(global: bool, old_present: &str, new_count: &str) -> String {
    let branch = |old| {
        let nonempty = action_code(group_action(global, old, true));
        let empty = action_code(group_action(global, old, false));
        if nonempty == empty {
            format!("'{nonempty}'")
        } else {
            format!("CASE WHEN ({new_count}) > 0 THEN '{nonempty}' ELSE '{empty}' END")
        }
    };
    format!(
        "CASE WHEN {old_present} THEN {} ELSE {} END",
        branch(true),
        branch(false)
    )
}

pub fn aggregate_value_sql(nullable: bool, argument_count: &str, value: &str) -> String {
    match (
        aggregate_is_null(nullable, false),
        aggregate_is_null(nullable, true),
    ) {
        (false, false) => value.to_string(),
        (true, false) => format!("CASE WHEN ({argument_count}) > 0 THEN {value} ELSE NULL END"),
        (false, true) => format!("CASE WHEN ({argument_count}) > 0 THEN NULL ELSE {value} END"),
        (true, true) => "NULL".to_string(),
    }
}

#[allow(dead_code)]
fn main() {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_visibility_actions_truth_table_payload_and_absent_groups() {
        let states = [Some(true), Some(false), None];
        let expected = [
            [(true, true), (true, false), (true, false)],
            [(false, true), (false, false), (false, false)],
            [(false, true), (false, false), (false, false)],
        ];
        for (old, old_state) in states.into_iter().enumerate() {
            for (new, new_state) in states.into_iter().enumerate() {
                let actions = visibility_actions(true, old_state, true, new_state, true);
                assert_eq!((actions.retract, actions.insert), expected[old][new]);
            }
        }
        assert_eq!(
            visibility_actions(true, Some(true), true, Some(true), false),
            RowActions {
                retract: false,
                insert: false
            }
        );
        assert_eq!(
            visibility_actions(false, Some(true), true, Some(true), true),
            RowActions {
                retract: false,
                insert: true
            }
        );
        assert_eq!(
            visibility_actions(true, Some(true), false, Some(true), true),
            RowActions {
                retract: true,
                insert: false
            }
        );
        assert_eq!(having_retraction_sql("p"), "(p) IS NOT TRUE");
        assert_eq!(having_admission_sql("p"), "(p) IS TRUE");
    }

    #[test]
    fn test_group_plan_separates_rows_arguments_and_global_empty_output() {
        assert_eq!(group_action(false, true, false), GroupAction::Delete);
        assert_eq!(group_action(false, false, false), GroupAction::Skip);
        assert_eq!(group_action(true, true, false), GroupAction::Update);
        assert_eq!(group_action(true, false, false), GroupAction::Insert);
        assert_eq!(group_presence_sql(true, "n"), "TRUE");
        assert_eq!(group_presence_sql(false, "n"), "(n) > 0");
        assert_eq!(aggregate_value_sql(false, "n", "0"), "0");
        assert_eq!(
            aggregate_value_sql(true, "n", "v"),
            "CASE WHEN (n) > 0 THEN v ELSE NULL END"
        );
    }
}
