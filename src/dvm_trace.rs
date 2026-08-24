//! Structured, opt-in DVM decisions for correctness tests and diagnostics.

use serde::Serialize;

use crate::dvm::schema::{ColumnProvenance, RelationSchema};

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct TraceColumn {
    pub name: String,
    pub type_oid: u32,
    pub typmod: i32,
    pub nullable: bool,
    pub provenance: String,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct DecisionEvent {
    pub operator_path: String,
    pub operator: String,
    pub output_schema: Vec<TraceColumn>,
    pub snapshot_plan: Option<String>,
    pub aggregate_strategy: Option<String>,
    pub decisions: Vec<String>,
    pub delta_cte: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, PartialEq, Eq)]
pub struct DecisionTrace {
    pub events: Vec<DecisionEvent>,
}

impl DecisionTrace {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push(&mut self, event: DecisionEvent) {
        self.events.push(event);
    }

    pub fn record(
        &mut self,
        operator_path: impl Into<String>,
        operator: impl Into<String>,
        output_schema: Vec<TraceColumn>,
        snapshot_plan: Option<String>,
        decisions: impl IntoIterator<Item = String>,
        delta_cte: Option<String>,
    ) {
        self.push(DecisionEvent {
            operator_path: operator_path.into(),
            operator: operator.into(),
            output_schema,
            snapshot_plan,
            aggregate_strategy: None,
            decisions: decisions.into_iter().collect(),
            delta_cte,
        });
    }

    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }

    pub fn render_markdown(&self) -> String {
        let mut output =
            String::from("| Path | Operator | Snapshot plan | Delta CTE |\n|---|---|---|---|\n");
        for event in &self.events {
            output.push_str("| ");
            output.push_str(&event.operator_path);
            output.push_str(" | ");
            output.push_str(&event.operator);
            output.push_str(" | ");
            output.push_str(event.snapshot_plan.as_deref().unwrap_or("—"));
            output.push_str(" | ");
            output.push_str(event.delta_cte.as_deref().unwrap_or("—"));
            output.push_str(" |\n");
        }
        output
    }
}

pub fn trace_schema(schema: &RelationSchema) -> Vec<TraceColumn> {
    schema
        .0
        .iter()
        .map(|column| TraceColumn {
            name: column.name.clone(),
            type_oid: column.type_oid,
            typmod: column.typmod,
            nullable: column.nullable,
            provenance: match column.provenance {
                ColumnProvenance::User => "User",
                ColumnProvenance::Internal => "Internal",
            }
            .to_string(),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trace_serializes_and_renders_decisions() {
        let mut trace = DecisionTrace::new();
        trace.record(
            "root.left",
            "LeftJoin",
            vec![TraceColumn {
                name: "id".to_string(),
                type_oid: 23,
                typmod: -1,
                nullable: false,
                provenance: "User".to_string(),
            }],
            Some("ExactCombined".to_string()),
            ["cache_hit".to_string(), "materialized".to_string()],
            Some("__pgt_cte_join_1".to_string()),
        );

        let json = trace.to_json().expect("trace JSON");
        assert!(json.contains("root.left"));
        assert!(trace.render_markdown().contains("ExactCombined"));
    }
}
