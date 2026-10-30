//! Rows that refer to other rows.
//!
//! A column of one mapped table may hold the id of a row of another. In a
//! SQL database that is a foreign key, and the source reads the foreign
//! keys of the mapped tables from its catalogue when the controller
//! starts. A mapping may also declare such a column by hand, for a table
//! without the constraint, or for a source that has no constraints. Either
//! way the column is a [`Reference`]: when the row is
//! reconciled, the value in the column is replaced by an attribute of the
//! resource that the referenced row describes, as the engine recorded it.
//! A subscription whose `topic` column holds the id of a row of `topics`
//! is handed the topic's real identifier, not the row id.
//!
//! What follows from that is in three places:
//!
//! * [`resolve`] replaces the values, and says when the referenced
//!   resource has nothing to give yet, so that the row waits for it
//!   instead of failing.
//! * [`store`] remembers which resource refers to which, so that a
//!   resource is not removed while another still refers to it, and so
//!   that a change to what one resource offers reaches those that took
//!   it.
//! * The scheduler holds a resource that waits, and gives it its turn as
//!   soon as what it waits for has been reconciled.
//!
//! # Rules
//!
//! * Only a foreign key of one column counts, and only when the column it
//!   points at is the referenced mapping's id column. Others are passed
//!   over with a note in the log.
//! * The id, type, stack and attributes columns of a mapping are never
//!   references, whatever the constraints say.
//! * A reference declared in the mapping wins over a foreign key on the
//!   same column.
//! * The attribute handed over is the referenced mapping's `exports`, or
//!   the `attribute` of a declared reference.
//! * The value goes to the attribute of the column's own name, or to the
//!   one a declared reference `sets`, in which case the column itself is
//!   not an attribute.
//! * The import column of a mapping is never a reference.
//! * The foreign keys are read once, at start. A constraint added while
//!   the controller runs is seen at the next start.

pub mod resolve;
pub mod store;

use std::collections::BTreeSet;
use std::fmt;

pub use resolve::{Dependency, Resolved, Resolver};
pub use store::ReferenceStore;

use crate::config::Mapping;
use crate::error::{Error, Result};

/// A column of a mapped table that holds the id of a row of another
/// mapped table.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Reference {
    /// The schema of the table that holds the column.
    pub schema: String,
    /// The table that holds the column.
    pub table: String,
    /// The column.
    pub column: String,
    /// The schema of the table the column points at.
    pub target_schema: String,
    /// The table the column points at.
    pub target_table: String,
    /// The attribute of the referenced resource that replaces the column's
    /// value when the row is reconciled.
    pub attribute: String,
    /// The attribute of the row's own resource that receives the value:
    /// the column itself, unless the mapping says another.
    pub sets: String,
    /// Where the reference was found.
    pub source: Provenance,
}

/// Where a reference came from.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Provenance {
    /// A foreign key constraint, by its name.
    Constraint(String),
    /// A `[[mappings.references]]` block of the configuration.
    Mapping,
}

impl Reference {
    /// The table that holds the column, as `schema.table`.
    pub fn qualified_table(&self) -> String {
        format!("{}.{}", self.schema, self.table)
    }

    /// The table the column points at, as `schema.table`.
    pub fn qualified_target(&self) -> String {
        format!("{}.{}", self.target_schema, self.target_table)
    }
}

impl fmt::Display for Reference {
    /// `public.subscriptions.topic -> public.topics (id), from the
    /// constraint subscriptions_topic_fkey`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}.{} -> {} ({})",
            self.qualified_table(),
            self.column,
            self.qualified_target(),
            self.attribute
        )?;
        if self.sets != self.column {
            write!(f, " into {}", self.sets)?;
        }
        f.write_str(", from ")?;
        match &self.source {
            Provenance::Constraint(name) => write!(f, "the constraint {name}"),
            Provenance::Mapping => f.write_str("the mapping"),
        }
    }
}

/// A foreign key as a source's catalogue describes it, reduced to its
/// first column.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ForeignKey {
    /// The constraint's name.
    pub constraint: String,
    /// The schema of the table that holds the key.
    pub schema: String,
    /// The table that holds the key.
    pub table: String,
    /// The key's first column.
    pub column: String,
    /// The schema of the table the key points at.
    pub target_schema: String,
    /// The table the key points at.
    pub target_table: String,
    /// The column it points at, for the key's first column.
    pub target_column: String,
    /// How many columns the key spans. Only a key of one is a reference.
    pub columns: usize,
}

/// Every reference between the mapped tables, in one place.
///
/// Made once at start by [`assemble`](Self::assemble), from the foreign
/// keys the source found and the mappings' own declarations, and shared
/// read-only from then on.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct References {
    /// Sorted, with one entry per column.
    references: Vec<Reference>,
}

impl References {
    /// References from a list, sorted, with the first entry for a column
    /// kept where there are several.
    pub fn new(mut references: Vec<Reference>) -> Self {
        let mut seen = BTreeSet::new();
        references.retain(|reference| {
            seen.insert((
                reference.schema.clone(),
                reference.table.clone(),
                reference.column.clone(),
            ))
        });
        references.sort();
        Self { references }
    }

    /// No references at all.
    pub fn none() -> Self {
        Self::default()
    }

    /// The references of the mapped tables: those declared in the mappings
    /// and those among `keys` that the rules at the top of this module
    /// admit.
    ///
    /// A declared reference comes first for its column, so a foreign key
    /// on the same column is passed over. A key that is passed over for
    /// another reason is noted in the log at `info`, so that a table whose
    /// constraint was expected to count can be understood.
    ///
    /// # Errors
    ///
    /// [`Error::Config`] if a declared reference points at a table that is
    /// not mapped, which [`validate_mappings`](crate::config::validate_mappings)
    /// refuses before this is reached.
    pub fn assemble(mappings: &[Mapping], keys: &[ForeignKey]) -> Result<Self> {
        let mapping_of = |schema: &str, table: &str| {
            mappings
                .iter()
                .find(|mapping| mapping.schema == schema && mapping.table == table)
        };
        let mut references = Vec::new();
        for mapping in mappings {
            for declared in &mapping.references {
                let target = mapping_of(&declared.schema, &declared.table).ok_or_else(|| {
                    Error::Config(format!(
                        "mapping {}: reference column {} points at {}.{}, which is not mapped",
                        mapping.qualified_table(),
                        declared.column,
                        declared.schema,
                        declared.table
                    ))
                })?;
                references.push(Reference {
                    schema: mapping.schema.clone(),
                    table: mapping.table.clone(),
                    column: declared.column.clone(),
                    target_schema: target.schema.clone(),
                    target_table: target.table.clone(),
                    attribute: declared
                        .attribute
                        .clone()
                        .unwrap_or_else(|| target.exports.clone()),
                    sets: declared
                        .sets
                        .clone()
                        .unwrap_or_else(|| declared.column.clone()),
                    source: Provenance::Mapping,
                });
            }
        }
        for key in keys {
            let Some(mapping) = mapping_of(&key.schema, &key.table) else {
                continue;
            };
            if !mapping.foreign_keys {
                continue;
            }
            let table = mapping.qualified_table();
            let Some(target) = mapping_of(&key.target_schema, &key.target_table) else {
                tracing::debug!(constraint = %key.constraint, %table, "a foreign key points at a table that is not mapped, and is not a reference");
                continue;
            };
            if key.columns != 1 {
                tracing::info!(constraint = %key.constraint, %table, columns = key.columns, "a foreign key of several columns is not a reference");
                continue;
            }
            if key.target_column != target.id_column {
                tracing::info!(constraint = %key.constraint, %table, column = %key.target_column, id_column = %target.id_column, "a foreign key that does not point at the id column is not a reference");
                continue;
            }
            if mapping.names_resource_with(&key.column) {
                tracing::info!(constraint = %key.constraint, %table, column = %key.column, "a foreign key on a column that names the resource is not a reference");
                continue;
            }
            if mapping.import_column.as_deref() == Some(key.column.as_str()) {
                tracing::info!(constraint = %key.constraint, %table, column = %key.column, "a foreign key on the import column is not a reference");
                continue;
            }
            references.push(Reference {
                schema: mapping.schema.clone(),
                table: mapping.table.clone(),
                column: key.column.clone(),
                target_schema: target.schema.clone(),
                target_table: target.table.clone(),
                attribute: target.exports.clone(),
                sets: key.column.clone(),
                source: Provenance::Constraint(key.constraint.clone()),
            });
        }
        Ok(Self::new(references))
    }

    /// The references held by the columns of `mapping`'s table.
    pub fn of<'a>(&'a self, mapping: &'a Mapping) -> impl Iterator<Item = &'a Reference> + 'a {
        self.references.iter().filter(move |reference| {
            reference.schema == mapping.schema && reference.table == mapping.table
        })
    }

    /// The attributes that other tables' columns take from the resources
    /// of `schema.table`: what those resources have to export.
    pub fn exported_by(&self, schema: &str, table: &str) -> BTreeSet<String> {
        self.references
            .iter()
            .filter(|reference| {
                reference.target_schema == schema && reference.target_table == table
            })
            .map(|reference| reference.attribute.clone())
            .collect()
    }

    /// Every reference, sorted by table and column.
    pub fn iter(&self) -> impl Iterator<Item = &Reference> {
        self.references.iter()
    }

    /// How many references there are.
    pub fn len(&self) -> usize {
        self.references.len()
    }

    /// Whether there are none.
    pub fn is_empty(&self) -> bool {
        self.references.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mapping(text: &str) -> Mapping {
        toml::from_str(text).unwrap()
    }

    fn key(
        constraint: &str,
        table: &str,
        column: &str,
        target: &str,
        target_column: &str,
    ) -> ForeignKey {
        ForeignKey {
            constraint: constraint.into(),
            schema: "public".into(),
            table: table.into(),
            column: column.into(),
            target_schema: "public".into(),
            target_table: target.into(),
            target_column: target_column.into(),
            columns: 1,
        }
    }

    #[test]
    fn foreign_keys_into_mapped_tables_are_references_and_the_rest_are_not() {
        let mappings = vec![
            mapping("table = \"topics\"\nresource_type = \"topic\"\nexports = \"arn\""),
            mapping(
                "table = \"subscriptions\"\nresource_type = \"subscription\"\nstack_column = \"env\"",
            ),
            mapping("table = \"folders\"\nresource_type = \"folder\"\nid_column = \"code\""),
        ];
        let keys = vec![
            key("sub_topic_fkey", "subscriptions", "topic", "topics", "id"),
            // Points at a table that is not mapped.
            key("sub_owner_fkey", "subscriptions", "owner", "users", "id"),
            // Points at a column that is not the id column.
            key(
                "sub_name_fkey",
                "subscriptions",
                "topic_name",
                "topics",
                "name",
            ),
            // Spans two columns.
            ForeignKey {
                columns: 2,
                ..key("sub_pair_fkey", "subscriptions", "a", "topics", "id")
            },
            // On the column that names the stack.
            key("sub_env_fkey", "subscriptions", "env", "topics", "id"),
            // A table that refers to itself, by the mapping's id column.
            key(
                "folders_parent_fkey",
                "folders",
                "parent",
                "folders",
                "code",
            ),
            // A table that is not mapped at all.
            key("users_team_fkey", "users", "team", "topics", "id"),
        ];
        let references = References::assemble(&mappings, &keys).unwrap();
        let listed: Vec<String> = references.iter().map(ToString::to_string).collect();
        assert_eq!(
            listed,
            [
                "public.folders.parent -> public.folders (id), from the constraint folders_parent_fkey",
                "public.subscriptions.topic -> public.topics (arn), from the constraint sub_topic_fkey",
            ]
        );
        assert_eq!(references.len(), 2);
        assert!(!references.is_empty());
        assert_eq!(
            references.exported_by("public", "topics"),
            BTreeSet::from(["arn".to_owned()])
        );
        assert_eq!(
            references.exported_by("public", "folders"),
            BTreeSet::from(["id".to_owned()])
        );
        assert!(references.exported_by("public", "subscriptions").is_empty());
        assert_eq!(references.of(&mappings[1]).count(), 1);
        assert_eq!(references.of(&mappings[0]).count(), 0);
    }

    #[test]
    fn a_declared_reference_wins_over_a_constraint_and_may_name_its_attribute() {
        let mappings = vec![
            mapping("table = \"topics\"\nresource_type = \"topic\""),
            mapping(
                "table = \"subscriptions\"\nresource_type = \"subscription\"\n[[references]]\ncolumn = \"topic\"\ntable = \"topics\"\nattribute = \"selfLink\"\n[[references]]\ncolumn = \"dead_letter\"\ntable = \"topics\"",
            ),
        ];
        let keys = vec![key(
            "sub_topic_fkey",
            "subscriptions",
            "topic",
            "topics",
            "id",
        )];
        let references = References::assemble(&mappings, &keys).unwrap();
        let listed: Vec<String> = references.iter().map(ToString::to_string).collect();
        assert_eq!(
            listed,
            [
                "public.subscriptions.dead_letter -> public.topics (id), from the mapping",
                "public.subscriptions.topic -> public.topics (selfLink), from the mapping",
            ]
        );
        assert_eq!(
            references.exported_by("public", "topics"),
            BTreeSet::from(["id".to_owned(), "selfLink".to_owned()])
        );
    }

    #[test]
    fn a_mapping_that_turns_foreign_keys_off_keeps_only_what_it_declares() {
        let mappings = vec![
            mapping("table = \"topics\"\nresource_type = \"topic\""),
            mapping(
                "table = \"subscriptions\"\nresource_type = \"subscription\"\nforeign_keys = false",
            ),
        ];
        let keys = vec![key(
            "sub_topic_fkey",
            "subscriptions",
            "topic",
            "topics",
            "id",
        )];
        let references = References::assemble(&mappings, &keys).unwrap();
        assert!(references.is_empty());
        assert_eq!(References::none(), References::default());
    }

    #[test]
    fn a_reference_may_set_another_attribute_and_the_import_column_is_never_one() {
        let mappings = vec![
            mapping("table = \"networks\"\nresource_type = \"network\""),
            mapping(
                "table = \"servers\"\nresource_type = \"server\"\nimport_column = \"object\"\n[[references]]\ncolumn = \"network_ref\"\ntable = \"networks\"\nsets = \"network_id\"",
            ),
        ];
        let keys = vec![
            key("servers_object_fkey", "servers", "object", "networks", "id"),
            key("servers_other_fkey", "servers", "other", "networks", "id"),
        ];
        let references = References::assemble(&mappings, &keys).unwrap();
        let found: Vec<String> = references.iter().map(ToString::to_string).collect();
        assert_eq!(
            found,
            [
                "public.servers.network_ref -> public.networks (id) into network_id, from the mapping",
                "public.servers.other -> public.networks (id), from the constraint servers_other_fkey",
            ],
            "the key on the import column is passed over"
        );
        assert_eq!(references.exported_by("public", "networks").len(), 1);
    }

    #[test]
    fn a_declared_reference_into_an_unmapped_table_is_refused() {
        let mappings = vec![mapping(
            "table = \"subscriptions\"\nresource_type = \"subscription\"\n[[references]]\ncolumn = \"topic\"\ntable = \"topics\"",
        )];
        let error = References::assemble(&mappings, &[]).unwrap_err();
        assert!(
            matches!(error, Error::Config(ref m) if m.contains("not mapped")),
            "{error}"
        );
    }

    #[test]
    fn the_first_entry_for_a_column_is_kept() {
        let one = Reference {
            schema: "public".into(),
            table: "t".into(),
            column: "c".into(),
            target_schema: "public".into(),
            target_table: "u".into(),
            attribute: "id".into(),
            sets: "c".into(),
            source: Provenance::Mapping,
        };
        let two = Reference {
            attribute: "arn".into(),
            source: Provenance::Constraint("k".into()),
            ..one.clone()
        };
        let references = References::new(vec![one.clone(), two]);
        assert_eq!(references.iter().collect::<Vec<_>>(), [&one]);
        assert_eq!(one.qualified_table(), "public.t");
        assert_eq!(one.qualified_target(), "public.u");
    }
}
