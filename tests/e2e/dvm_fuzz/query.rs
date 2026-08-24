//! Typed Wave A relation trees used by the composition correctness tests.
//!
//! The generator works with relations first and renders SQL last.  That keeps
//! arity, names, and join keys checkable before PostgreSQL sees a query.

use std::collections::HashSet;
use std::fmt::Write as _;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScalarType {
    Bool,
    Int,
    BigInt,
    Numeric,
    Text,
}

impl ScalarType {
    pub fn sql(self) -> &'static str {
        match self {
            Self::Bool => "boolean",
            Self::Int => "integer",
            Self::BigInt => "bigint",
            Self::Numeric => "numeric",
            Self::Text => "text",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Column {
    pub name: String,
    pub data_type: ScalarType,
    pub nullable: bool,
}

impl Column {
    pub fn new(name: impl Into<String>, data_type: ScalarType, nullable: bool) -> Self {
        Self {
            name: name.into(),
            data_type,
            nullable,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationSchema {
    pub columns: Vec<Column>,
}

impl RelationSchema {
    pub fn new(columns: impl Into<Vec<Column>>) -> Self {
        Self {
            columns: columns.into(),
        }
    }

    pub fn column(&self, index: usize) -> Option<&Column> {
        self.columns.get(index)
    }

    fn require_column(&self, index: usize, context: &str) -> Result<&Column, String> {
        self.column(index)
            .ok_or_else(|| format!("{context}: column index {index} is out of range"))
    }

    fn unique_names(&self) -> Result<(), String> {
        let mut names = HashSet::new();
        for column in &self.columns {
            validate_identifier(&column.name, "column name")?;
            if !names.insert(&column.name) {
                return Err(format!("duplicate output column {}", column.name));
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinKind {
    Inner,
    Left,
    Full,
}

impl JoinKind {
    pub fn sql(self) -> &'static str {
        match self {
            Self::Inner => "JOIN",
            Self::Left => "LEFT JOIN",
            Self::Full => "FULL JOIN",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Predicate {
    True,
    IsNotNull { input_column: usize },
    EqualsLiteral { input_column: usize, value: String },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregateKind {
    Count,
    Sum,
    Avg,
    Min,
    Max,
    StringAgg,
}

impl AggregateKind {
    fn output_type(self, input: Option<&Column>) -> Result<(ScalarType, bool), String> {
        match self {
            Self::Count => Ok((ScalarType::BigInt, false)),
            Self::Sum => match input.map(|column| column.data_type) {
                Some(ScalarType::Int) => Ok((ScalarType::BigInt, true)),
                Some(ScalarType::BigInt) => Ok((ScalarType::Numeric, true)),
                Some(other) => Err(format!("SUM does not accept {}", other.sql())),
                None => Err("SUM requires an input column".to_string()),
            },
            Self::Avg => match input.map(|column| column.data_type) {
                Some(ScalarType::Int | ScalarType::BigInt | ScalarType::Numeric) => {
                    Ok((ScalarType::Numeric, true))
                }
                Some(other) => Err(format!("AVG does not accept {}", other.sql())),
                None => Err("AVG requires an input column".to_string()),
            },
            Self::Min | Self::Max => input
                .map(|column| (column.data_type, true))
                .ok_or_else(|| "MIN/MAX require an input column".to_string()),
            Self::StringAgg => match input.map(|column| column.data_type) {
                Some(ScalarType::Text) => Ok((ScalarType::Text, true)),
                Some(other) => Err(format!("STRING_AGG requires text, got {}", other.sql())),
                None => Err("STRING_AGG requires an input column".to_string()),
            },
        }
    }

    fn sql(self, input: Option<&str>) -> String {
        match self {
            Self::Count => "COUNT(*)".to_string(),
            Self::StringAgg => format!(
                "STRING_AGG({}, ',' ORDER BY {})",
                input.unwrap_or("''"),
                input.unwrap_or("''")
            ),
            Self::Sum => format!("SUM({})", input.unwrap_or("NULL")),
            Self::Avg => format!("AVG({})", input.unwrap_or("NULL")),
            Self::Min => format!("MIN({})", input.unwrap_or("NULL")),
            Self::Max => format!("MAX({})", input.unwrap_or("NULL")),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AggregateExpr {
    pub kind: AggregateKind,
    pub input_column: Option<usize>,
    pub alias: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectExpr {
    pub input_column: usize,
    pub alias: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RelNode {
    Scan {
        table: String,
        schema: RelationSchema,
    },
    Filter {
        input: Box<Self>,
        predicate: Predicate,
    },
    Project {
        input: Box<Self>,
        expressions: Vec<ProjectExpr>,
    },
    Aggregate {
        input: Box<Self>,
        group_by: Vec<usize>,
        aggregates: Vec<AggregateExpr>,
    },
    Join {
        kind: JoinKind,
        left: Box<Self>,
        right: Box<Self>,
        left_column: usize,
        right_column: usize,
    },
    CteRef {
        name: String,
        schema: RelationSchema,
    },
    Subquery {
        input: Box<Self>,
        alias: String,
    },
}

impl RelNode {
    pub fn schema(&self) -> Result<RelationSchema, String> {
        let schema = match self {
            Self::Scan { schema, .. } | Self::CteRef { schema, .. } => schema.clone(),
            Self::Filter { input, predicate } => {
                validate_predicate(input, predicate)?;
                input.schema()?
            }
            Self::Project { input, expressions } => {
                let input_schema = input.schema()?;
                let mut columns = Vec::with_capacity(expressions.len());
                for expression in expressions {
                    let source = input_schema.require_column(expression.input_column, "project")?;
                    validate_identifier(&expression.alias, "project alias")?;
                    columns.push(Column::new(
                        &expression.alias,
                        source.data_type,
                        source.nullable,
                    ));
                }
                RelationSchema::new(columns)
            }
            Self::Aggregate {
                input,
                group_by,
                aggregates,
            } => {
                let input_schema = input.schema()?;
                let mut columns = Vec::with_capacity(group_by.len() + aggregates.len());
                for &index in group_by {
                    columns.push(
                        input_schema
                            .require_column(index, "aggregate group")?
                            .clone(),
                    );
                }
                for aggregate in aggregates {
                    let input_column = aggregate
                        .input_column
                        .map(|index| input_schema.require_column(index, "aggregate input"))
                        .transpose()?;
                    let (data_type, nullable) = aggregate.kind.output_type(input_column)?;
                    validate_identifier(&aggregate.alias, "aggregate alias")?;
                    columns.push(Column::new(&aggregate.alias, data_type, nullable));
                }
                RelationSchema::new(columns)
            }
            Self::Join {
                kind,
                left,
                right,
                left_column,
                right_column,
            } => {
                let left_schema = left.schema()?;
                let right_schema = right.schema()?;
                let left_key = left_schema.require_column(*left_column, "join left key")?;
                let right_key = right_schema.require_column(*right_column, "join right key")?;
                if left_key.data_type != right_key.data_type {
                    return Err(format!(
                        "join key type mismatch: {} and {}",
                        left_key.data_type.sql(),
                        right_key.data_type.sql()
                    ));
                }
                let mut names = HashSet::new();
                let mut columns =
                    Vec::with_capacity(left_schema.columns.len() + right_schema.columns.len());
                for (side, side_columns) in [
                    ("left", &left_schema.columns),
                    ("right", &right_schema.columns),
                ] {
                    for column in side_columns {
                        let mut name = column.name.clone();
                        if !names.insert(name.clone()) {
                            name = format!("{side}_{}", name);
                            let mut suffix = 2;
                            while !names.insert(name.clone()) {
                                name = format!("{side}_{suffix}_{}", column.name);
                                suffix += 1;
                            }
                        }
                        let nullable = column.nullable
                            || matches!(kind, JoinKind::Full)
                            || matches!((kind, side), (JoinKind::Left, "right"));
                        columns.push(Column::new(name, column.data_type, nullable));
                    }
                }
                RelationSchema::new(columns)
            }
            Self::Subquery { input, alias } => {
                validate_identifier(alias, "subquery alias")?;
                input.schema()?
            }
        };
        schema.unique_names()?;
        Ok(schema)
    }

    pub fn render_sql(&self) -> Result<String, String> {
        self.schema()?;
        self.render_sql_inner()
    }

    fn render_sql_inner(&self) -> Result<String, String> {
        match self {
            Self::Scan { table, schema } => {
                validate_identifier(table, "scan table")?;
                let columns = schema
                    .columns
                    .iter()
                    .map(|column| quote_ident(&column.name))
                    .collect::<Vec<_>>()
                    .join(", ");
                Ok(format!("SELECT {columns} FROM {}", quote_ident(table)))
            }
            Self::Filter { input, predicate } => {
                let source = input.render_sql_inner()?;
                Ok(format!(
                    "SELECT * FROM ({source}) AS filtered WHERE {}",
                    render_predicate(input, predicate, "filtered")?
                ))
            }
            Self::Project { input, expressions } => {
                let source = input.render_sql_inner()?;
                let input_schema = input.schema()?;
                let select = expressions
                    .iter()
                    .map(|expression| {
                        input_schema
                            .require_column(expression.input_column, "project")
                            .map(|column| {
                                format!(
                                    "{}.{} AS {}",
                                    "projected",
                                    quote_ident(&column.name),
                                    quote_ident(&expression.alias)
                                )
                            })
                    })
                    .collect::<Result<Vec<_>, _>>()?
                    .join(", ");
                Ok(format!("SELECT {select} FROM ({source}) AS projected"))
            }
            Self::Aggregate {
                input,
                group_by,
                aggregates,
            } => {
                let source = input.render_sql_inner()?;
                let input_schema = input.schema()?;
                let mut select = Vec::with_capacity(group_by.len() + aggregates.len());
                let mut groups = Vec::with_capacity(group_by.len());
                for &index in group_by {
                    let column = input_schema.require_column(index, "aggregate group")?;
                    let reference = format!("grouped.{}", quote_ident(&column.name));
                    groups.push(reference.clone());
                    select.push(reference);
                }
                for aggregate in aggregates {
                    let input = aggregate.input_column.map(|index| {
                        input_schema
                            .require_column(index, "aggregate input")
                            .map(|column| format!("grouped.{}", quote_ident(&column.name)))
                    });
                    let input = input.transpose()?;
                    select.push(format!(
                        "{} AS {}",
                        aggregate.kind.sql(input.as_deref()),
                        quote_ident(&aggregate.alias)
                    ));
                }
                let group_by = if groups.is_empty() {
                    String::new()
                } else {
                    format!(" GROUP BY {}", groups.join(", "))
                };
                Ok(format!(
                    "SELECT {} FROM ({source}) AS grouped{group_by}",
                    select.join(", ")
                ))
            }
            Self::Join {
                kind,
                left,
                right,
                left_column,
                right_column,
            } => {
                let left_sql = left.render_sql_inner()?;
                let right_sql = right.render_sql_inner()?;
                let left_schema = left.schema()?;
                let right_schema = right.schema()?;
                let left_key = left_schema.require_column(*left_column, "join left key")?;
                let right_key = right_schema.require_column(*right_column, "join right key")?;
                let output_schema = self.schema()?;
                let columns = left_schema
                    .columns
                    .iter()
                    .enumerate()
                    .map(|(index, column)| {
                        format!(
                            "l.{} AS {}",
                            quote_ident(&column.name),
                            quote_ident(&output_schema.columns[index].name)
                        )
                    })
                    .chain(
                        right_schema
                            .columns
                            .iter()
                            .enumerate()
                            .map(|(index, column)| {
                                let output_index = left_schema.columns.len() + index;
                                format!(
                                    "r.{} AS {}",
                                    quote_ident(&column.name),
                                    quote_ident(&output_schema.columns[output_index].name)
                                )
                            }),
                    )
                    .collect::<Vec<_>>()
                    .join(", ");
                let operator = if left_key.nullable || right_key.nullable {
                    "IS NOT DISTINCT FROM"
                } else {
                    "="
                };
                Ok(format!(
                    "SELECT {columns} FROM ({left_sql}) AS l {} ({right_sql}) AS r ON l.{} {operator} r.{}",
                    kind.sql(),
                    quote_ident(&left_key.name),
                    quote_ident(&right_key.name)
                ))
            }
            Self::CteRef { name, schema } => {
                validate_identifier(name, "CTE name")?;
                let columns = schema
                    .columns
                    .iter()
                    .map(|column| quote_ident(&column.name))
                    .collect::<Vec<_>>()
                    .join(", ");
                Ok(format!("SELECT {columns} FROM {}", quote_ident(name)))
            }
            Self::Subquery { input, alias } => {
                let source = input.render_sql_inner()?;
                Ok(format!(
                    "SELECT * FROM ({source}) AS {}",
                    quote_ident(alias)
                ))
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CteDefinition {
    pub name: String,
    pub query: RelNode,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelQuery {
    pub ctes: Vec<CteDefinition>,
    pub body: RelNode,
}

impl RelQuery {
    pub fn schema(&self) -> Result<RelationSchema, String> {
        validate_ctes(&self.ctes)?;
        self.body.schema()
    }

    pub fn render_sql(&self) -> Result<String, String> {
        let body = self.body.render_sql()?;
        if self.ctes.is_empty() {
            return Ok(body);
        }
        let mut rendered = String::from("WITH ");
        for (index, cte) in self.ctes.iter().enumerate() {
            if index > 0 {
                rendered.push_str(", ");
            }
            write!(
                rendered,
                "{} AS ({})",
                quote_ident(&cte.name),
                cte.query.render_sql()?
            )
            .map_err(|error| error.to_string())?;
        }
        write!(rendered, " {body}").map_err(|error| error.to_string())?;
        Ok(rendered)
    }
}

fn validate_ctes(ctes: &[CteDefinition]) -> Result<(), String> {
    let mut names = HashSet::new();
    for cte in ctes {
        validate_identifier(&cte.name, "CTE name")?;
        if !names.insert(&cte.name) {
            return Err(format!("duplicate CTE {}", cte.name));
        }
        cte.query.schema()?;
    }
    Ok(())
}

fn validate_predicate(input: &RelNode, predicate: &Predicate) -> Result<(), String> {
    let schema = input.schema()?;
    match predicate {
        Predicate::True => Ok(()),
        Predicate::IsNotNull { input_column } | Predicate::EqualsLiteral { input_column, .. } => {
            schema
                .require_column(*input_column, "predicate")
                .map(|_| ())
        }
    }
}

fn render_predicate(input: &RelNode, predicate: &Predicate, alias: &str) -> Result<String, String> {
    let schema = input.schema()?;
    match predicate {
        Predicate::True => Ok("TRUE".to_string()),
        Predicate::IsNotNull { input_column } => {
            let column = schema.require_column(*input_column, "predicate")?;
            Ok(format!("{alias}.{} IS NOT NULL", quote_ident(&column.name)))
        }
        Predicate::EqualsLiteral {
            input_column,
            value,
        } => {
            let column = schema.require_column(*input_column, "predicate")?;
            if column.data_type != ScalarType::Text {
                return Err("literal predicates currently require text columns".to_string());
            }
            Ok(format!(
                "{alias}.{} = {}",
                quote_ident(&column.name),
                quote_literal(value)
            ))
        }
    }
}

fn validate_identifier(value: &str, label: &str) -> Result<(), String> {
    if value.is_empty()
        || !value
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || character == '_')
        || !value.as_bytes()[0].is_ascii_alphabetic()
    {
        return Err(format!("{label} is not a safe SQL identifier: {value}"));
    }
    Ok(())
}

fn quote_ident(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

fn quote_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn source() -> RelNode {
        RelNode::Scan {
            table: "source".to_string(),
            schema: RelationSchema::new(vec![
                Column::new("id", ScalarType::Int, false),
                Column::new("grp", ScalarType::Text, true),
                Column::new("value", ScalarType::Int, true),
            ]),
        }
    }

    #[test]
    fn wave_a_composition_computes_schema_before_rendering() {
        let query = RelQuery {
            ctes: vec![CteDefinition {
                name: "grouped".to_string(),
                query: RelNode::Aggregate {
                    input: Box::new(source()),
                    group_by: vec![1],
                    aggregates: vec![AggregateExpr {
                        kind: AggregateKind::Max,
                        input_column: Some(2),
                        alias: "max_value".to_string(),
                    }],
                },
            }],
            body: RelNode::Project {
                input: Box::new(RelNode::CteRef {
                    name: "grouped".to_string(),
                    schema: RelationSchema::new(vec![
                        Column::new("grp", ScalarType::Text, true),
                        Column::new("max_value", ScalarType::Int, true),
                    ]),
                }),
                expressions: vec![ProjectExpr {
                    input_column: 1,
                    alias: "max_value".to_string(),
                }],
            },
        };

        let schema = query.schema().expect("schema must be valid");
        assert_eq!(schema.columns.len(), 1);
        assert!(query.render_sql().is_ok());
    }

    #[test]
    fn invalid_join_arity_is_rejected_before_rendering() {
        let join = RelNode::Join {
            kind: JoinKind::Inner,
            left: Box::new(source()),
            right: Box::new(source()),
            left_column: 99,
            right_column: 0,
        };
        assert!(join.schema().is_err());
        assert!(join.render_sql().is_err());
    }
}
