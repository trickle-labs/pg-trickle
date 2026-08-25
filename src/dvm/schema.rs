//! Typed relation schemas carried through DVM delta results.

use crate::error::PgTrickleError;
use serde::{Deserialize, Serialize};

/// Origin of a relation column.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ColumnProvenance {
    User,
    Internal,
}

/// The metadata needed to safely compose two relation results.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RelationColumn {
    pub name: String,
    pub type_oid: u32,
    pub typmod: i32,
    pub nullable: bool,
    pub provenance: ColumnProvenance,
}

/// Ordered output schema for a DVM relation.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RelationSchema(pub Vec<RelationColumn>);

impl RelationSchema {
    pub fn from_names(names: &[String]) -> Self {
        Self(
            names
                .iter()
                .map(|name| RelationColumn {
                    name: name.clone(),
                    type_oid: 0,
                    typmod: -1,
                    nullable: true,
                    provenance: ColumnProvenance::User,
                })
                .collect(),
        )
    }

    pub fn from_parser_columns(columns: &[crate::dvm::parser::Column]) -> Self {
        Self(
            columns
                .iter()
                .map(|column| RelationColumn {
                    name: column.name.clone(),
                    type_oid: column.type_oid,
                    typmod: -1,
                    nullable: column.is_nullable,
                    provenance: ColumnProvenance::User,
                })
                .collect(),
        )
    }

    pub fn with_internal(mut self, name: impl Into<String>, type_oid: u32) -> Self {
        self.0.push(RelationColumn {
            name: name.into(),
            type_oid,
            typmod: -1,
            nullable: false,
            provenance: ColumnProvenance::Internal,
        });
        self
    }

    pub fn names(&self) -> Vec<String> {
        self.0.iter().map(|column| column.name.clone()).collect()
    }

    pub fn concat(&self, other: &Self) -> Self {
        let mut columns = self.0.clone();
        columns.extend(other.0.iter().cloned());
        Self(columns)
    }

    pub fn nullable(mut self) -> Self {
        for column in &mut self.0 {
            column.nullable = true;
        }
        self
    }

    pub fn mark_internal(mut self) -> Self {
        for column in &mut self.0 {
            if column.name.starts_with("__pgt_") {
                column.provenance = ColumnProvenance::Internal;
            }
        }
        self
    }

    pub fn renamed(&self, names: &[String]) -> Self {
        let mut columns = names
            .iter()
            .enumerate()
            .map(|(index, name)| {
                self.0.get(index).cloned().unwrap_or(RelationColumn {
                    name: name.clone(),
                    type_oid: 0,
                    typmod: -1,
                    nullable: true,
                    provenance: ColumnProvenance::User,
                })
            })
            .collect::<Vec<_>>();
        for (column, name) in columns.iter_mut().zip(names) {
            column.name = name.clone();
        }
        Self(columns)
    }

    pub fn describe(&self) -> String {
        self.0
            .iter()
            .map(|column| {
                format!(
                    "{}:{}:{}:{}:{:?}",
                    column.name, column.type_oid, column.typmod, column.nullable, column.provenance
                )
            })
            .collect::<Vec<_>>()
            .join(", ")
    }
}

/// Validate the positional schema contract required by SQL set operations.
pub fn validate_set_operation(
    operator: &str,
    left: &RelationSchema,
    right: &RelationSchema,
) -> Result<(), PgTrickleError> {
    if left.0.len() != right.0.len() {
        return Err(PgTrickleError::TypeMismatch(format!(
            "{operator}: arity mismatch (left {} [{}], right {} [{}])",
            left.0.len(),
            left.describe(),
            right.0.len(),
            right.describe()
        )));
    }

    for (position, (left_column, right_column)) in left.0.iter().zip(&right.0).enumerate() {
        if left_column.name != right_column.name {
            return Err(PgTrickleError::TypeMismatch(format!(
                "{operator}: column {} alias mismatch (left {}, right {}; schemas: [{}] vs [{}])",
                position + 1,
                left_column.name,
                right_column.name,
                left.describe(),
                right.describe()
            )));
        }
        if left_column.provenance != right_column.provenance {
            return Err(PgTrickleError::TypeMismatch(format!(
                "{operator}: column {} internal-column provenance mismatch (schemas: [{}] vs [{}])",
                position + 1,
                left.describe(),
                right.describe()
            )));
        }
        if left_column.type_oid != 0
            && right_column.type_oid != 0
            && left_column.type_oid != right_column.type_oid
        {
            return Err(PgTrickleError::TypeMismatch(format!(
                "{operator}: column {} type mismatch (left {}, right {}; schemas: [{}] vs [{}])",
                position + 1,
                left_column.type_oid,
                right_column.type_oid,
                left.describe(),
                right.describe()
            )));
        }
        if left_column.typmod != -1
            && right_column.typmod != -1
            && left_column.typmod != right_column.typmod
        {
            return Err(PgTrickleError::TypeMismatch(format!(
                "{operator}: column {} typmod mismatch (left {}, right {}; schemas: [{}] vs [{}])",
                position + 1,
                left_column.typmod,
                right_column.typmod,
                left.describe(),
                right.describe()
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn schema(name: &str, oid: u32) -> RelationSchema {
        RelationSchema(vec![RelationColumn {
            name: name.into(),
            type_oid: oid,
            typmod: -1,
            nullable: true,
            provenance: ColumnProvenance::User,
        }])
    }

    #[test]
    fn validates_matching_schema() {
        assert!(validate_set_operation("UNION ALL", &schema("id", 23), &schema("id", 23)).is_ok());
    }

    #[test]
    fn rejects_arity_alias_type_and_internal_mismatches() {
        assert!(
            validate_set_operation("UNION ALL", &schema("id", 23), &RelationSchema::default())
                .is_err()
        );
        assert!(
            validate_set_operation("UNION ALL", &schema("id", 23), &schema("other", 23)).is_err()
        );
        assert!(validate_set_operation("UNION ALL", &schema("id", 23), &schema("id", 25)).is_err());
        let internal = schema("id", 23).with_internal("__pgt_count", 20);
        let user = schema("id", 23).with_internal("__pgt_count", 20);
        assert!(validate_set_operation("UNION ALL", &internal, &user).is_ok());
        let mut user_column = schema("id", 23);
        user_column.0.push(RelationColumn {
            name: "__pgt_count".into(),
            type_oid: 20,
            typmod: -1,
            nullable: true,
            provenance: ColumnProvenance::User,
        });
        assert!(validate_set_operation("UNION ALL", &internal, &user_column).is_err());
    }

    #[test]
    fn composes_and_marks_internal_columns() {
        let schema = schema("id", 23)
            .concat(&schema("value", 25))
            .with_internal("__pgt_count", 20)
            .nullable()
            .mark_internal();
        assert_eq!(schema.names(), ["id", "value", "__pgt_count"]);
        assert!(schema.0.iter().all(|column| column.nullable));
        assert_eq!(schema.0[2].provenance, ColumnProvenance::Internal);
    }
}
