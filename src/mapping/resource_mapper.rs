//! Which resource a row change is about.
//!
//! The reader asks a [`ResourceMapper`] about every row change, once the
//! change has been related to its table and before it is gathered into its
//! transaction. [`TableMapper`] is the mapper the configuration describes.

use morph::ResourceId;

use crate::config::{Filters, Mapping, STACK_KIND};
use crate::error::{Error, Result};
use crate::postgres::pgoutput::Relation;
use crate::postgres::relation::RowChange;

/// Decides which resource, if any, a change to a row is about.
///
/// Nothing about the protocol lives here, and nothing about what Morph
/// should do: only the naming. A mapper answers with a resource id and
/// nothing else: not what happened to the row, and not what the row held.
pub trait ResourceMapper: Send + Sync {
    /// The resource the change is about.
    ///
    /// `None` for a table that is of no interest. `relation` is the table
    /// the change belongs to. The reader passes the change's own.
    ///
    /// # Errors
    ///
    /// [`Error::Mapping`] for a row that should name a resource and does
    /// not.
    fn map_change(&self, relation: &Relation, change: &RowChange) -> Result<Option<ResourceId>>;
}

/// The mapper the configuration describes.
///
/// Each mapped table has a column that names the resource. The kind is
/// fixed for the whole table, or comes from a column of its own.
///
/// # Examples
///
/// A delete from a table whose kind is fixed. The change carries the key
/// alone, and that is enough to name the resource:
///
/// ```
/// use std::sync::Arc;
///
/// use morph_postgres_controller::config::{Filters, Mapping};
/// use morph_postgres_controller::mapping::{ResourceMapper, TableMapper};
/// use morph_postgres_controller::postgres::pgoutput::{
///     Column, Datum, Relation, ReplicaIdentity, Tuple,
/// };
/// use morph_postgres_controller::postgres::relation::{ChangeKind, RowChange};
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let mapping: Mapping = toml::from_str(
///     r#"
///     table = "queues"
///     resource_type = "aws_sqs_queue"
///     "#,
/// )?;
/// let mapper = TableMapper::new(vec![mapping], Filters::default());
///
/// let column = |name: &str, key| Column {
///     name: name.into(),
///     key,
///     type_oid: 25,
///     modifier: -1,
/// };
/// let queues = Arc::new(Relation {
///     id: 16385,
///     schema: "public".into(),
///     name: "queues".into(),
///     replica_identity: ReplicaIdentity::Default,
///     columns: vec![column("id", true), column("name", false)],
/// });
/// let deleted = RowChange {
///     kind: ChangeKind::Delete,
///     relation: Arc::clone(&queues),
///     new: None,
///     old: Some(Tuple(vec![Datum::Text("orders".into()), Datum::Null])),
/// };
///
/// let resource = mapper.map_change(&queues, &deleted)?;
/// assert_eq!(resource, Some("aws_sqs_queue.orders".parse()?));
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug)]
pub struct TableMapper {
    mappings: Vec<Mapping>,
    filters: Filters,
}

impl TableMapper {
    /// A mapper for these mappings, listening only where the filters allow.
    ///
    /// The mappings are taken as they are. Checking them is the
    /// configuration's job.
    pub fn new(mappings: Vec<Mapping>, filters: Filters) -> Self {
        Self { mappings, filters }
    }

    /// The mapping for a table, if there is one and the filters allow it.
    ///
    /// The first mapping that names the table is used. The configuration's
    /// checks refuse a table that is mapped twice.
    pub fn mapping_for(&self, schema: &str, table: &str) -> Option<&Mapping> {
        if !self.filters.admits(schema, table) {
            return None;
        }
        self.mappings
            .iter()
            .find(|mapping| mapping.schema == schema && mapping.table == table)
    }

    /// Every mapping, whether or not the filters admit its table.
    pub fn mappings(&self) -> &[Mapping] {
        &self.mappings
    }
}

impl ResourceMapper for TableMapper {
    /// Names the resource from the mapping of the change's table.
    ///
    /// The name is the text of the id column. The kind is the mapping's
    /// fixed kind, or else the text of its type column. Both columns are
    /// read as [`RowChange::text`] reads them: from the new row, and from
    /// the old where the new has nothing.
    ///
    /// An update that changes the id column names the resource of the new
    /// id only. The resource of the old id is left to the periodic full
    /// reconciliation, which finds its record without a row.
    ///
    /// A table whose rows are grouped into stacks names the stack
    /// instead, as `stack.<name>`, from the mapping's `stack` or the
    /// text of its `stack_column`.
    ///
    /// `None` if the table is not mapped, or the filters do not admit it.
    ///
    /// # Errors
    ///
    /// [`Error::Mapping`] if the change has no text in the id column, or in
    /// the stack column of a grouped table; if
    /// the kind comes from a column and the change has no text there, as
    /// for a delete from a table whose replica identity leaves that column
    /// out; if the mapping gives neither a kind nor a type column, which
    /// the configuration's checks refuse; or if the kind and the name do
    /// not make a valid resource id.
    fn map_change(&self, relation: &Relation, change: &RowChange) -> Result<Option<ResourceId>> {
        let Some(mapping) = self.mapping_for(&relation.schema, &relation.name) else {
            return Ok(None);
        };
        let table = relation.qualified_name();
        // A grouped table's rows are not resources of their own to the
        // scheduler. The stack is, and every change to a row is a change
        // to its stack.
        if let Some(stack) = &mapping.stack {
            return ResourceId::new(STACK_KIND, stack.as_str())
                .map(Some)
                .map_err(|error| Error::Mapping(format!("a row of {table}: {error}")));
        }
        if let Some(column) = &mapping.stack_column {
            let stack = change.text(column).ok_or_else(|| {
                Error::Mapping(format!(
                    "a change to {table} came without its {column} column; for deletes to name \
                     their stack, the table needs REPLICA IDENTITY FULL"
                ))
            })?;
            return ResourceId::new(STACK_KIND, stack)
                .map(Some)
                .map_err(|error| Error::Mapping(format!("a row of {table}: {error}")));
        }
        let name = change.text(&mapping.id_column).ok_or_else(|| {
            Error::Mapping(format!(
                "a change to {table} came without its {} column; is it in the replica identity?",
                mapping.id_column
            ))
        })?;
        let kind = match (&mapping.resource_type, &mapping.type_column) {
            (Some(kind), _) => kind.clone(),
            (None, Some(column)) => change.text(column).map(str::to_owned).ok_or_else(|| {
                Error::Mapping(format!(
                    "a change to {table} came without its {column} column; for deletes to name \
                         their kind, the table needs REPLICA IDENTITY FULL"
                ))
            })?,
            (None, None) => {
                return Err(Error::Mapping(format!(
                    "the mapping for {table} names no kind"
                )));
            }
        };
        ResourceId::new(kind.as_str(), name)
            .map(Some)
            .map_err(|error| Error::Mapping(format!("a row of {table}: {error}")))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::postgres::pgoutput::{Column, Datum, ReplicaIdentity, Tuple};
    use crate::postgres::relation::ChangeKind;

    fn relation(schema: &str, table: &str, columns: &[&str]) -> Arc<Relation> {
        Arc::new(Relation {
            id: 1,
            schema: schema.into(),
            name: table.into(),
            replica_identity: ReplicaIdentity::Default,
            columns: columns
                .iter()
                .enumerate()
                .map(|(index, name)| Column {
                    name: (*name).to_owned(),
                    key: index == 0,
                    type_oid: 25,
                    modifier: -1,
                })
                .collect(),
        })
    }

    fn change(relation: &Arc<Relation>, kind: ChangeKind, values: &[Option<&str>]) -> RowChange {
        let tuple = Tuple(
            values
                .iter()
                .map(|value| value.map_or(Datum::Null, |text| Datum::Text(text.to_owned())))
                .collect(),
        );
        match kind {
            ChangeKind::Delete => RowChange {
                kind,
                relation: relation.clone(),
                new: None,
                old: Some(tuple),
            },
            _ => RowChange {
                kind,
                relation: relation.clone(),
                new: Some(tuple),
                old: None,
            },
        }
    }

    fn mapping(table: &str) -> Mapping {
        toml::from_str(&format!(
            "table = \"{table}\"\ntype_column = \"resource_type\""
        ))
        .unwrap()
    }

    fn static_mapping(schema: &str, table: &str, kind: &str) -> Mapping {
        toml::from_str(&format!(
            "schema = \"{schema}\"\ntable = \"{table}\"\nid_column = \"uuid\"\nresource_type = \"{kind}\""
        ))
        .unwrap()
    }

    #[test]
    fn a_grouped_table_names_its_stack_instead_of_its_row() {
        let fixed: Mapping =
            toml::from_str("table = \"vms\"\nresource_type = \"vm\"\nstack = \"fleet\"").unwrap();
        let by_column: Mapping = toml::from_str(
            "table = \"things\"\ntype_column = \"resource_type\"\nstack_column = \"env\"",
        )
        .unwrap();
        let mapper = TableMapper::new(vec![fixed, by_column], Filters::default());

        let vms = relation("public", "vms", &["id", "cpu"]);
        let found = mapper
            .map_change(
                &vms,
                &change(&vms, ChangeKind::Insert, &[Some("web-1"), Some("2")]),
            )
            .unwrap();
        assert_eq!(found, Some("stack.fleet".parse().unwrap()));

        let things = relation("public", "things", &["id", "resource_type", "env"]);
        let found = mapper
            .map_change(
                &things,
                &change(
                    &things,
                    ChangeKind::Update,
                    &[Some("7"), Some("server"), Some("prod")],
                ),
            )
            .unwrap();
        assert_eq!(found, Some("stack.prod".parse().unwrap()));

        // A delete without the stack column cannot name its stack.
        let error = mapper
            .map_change(
                &things,
                &change(&things, ChangeKind::Delete, &[Some("7"), None, None]),
            )
            .unwrap_err();
        assert!(
            error.to_string().contains("REPLICA IDENTITY FULL"),
            "{error}"
        );
    }

    #[test]
    fn unmapped_filtered_and_unnameable_rows() {
        let filters: Filters = toml::from_str("exclude_tables = [\"public.resources\"]").unwrap();
        let mapper = TableMapper::new(vec![mapping("resources"), mapping("things")], filters);
        assert!(mapper.mapping_for("public", "things").is_some());
        assert!(mapper.mapping_for("other", "things").is_none());
        assert_eq!(mapper.mappings().len(), 2);

        let excluded = relation("public", "resources", &["id", "resource_type"]);
        let change_to_excluded =
            change(&excluded, ChangeKind::Insert, &[Some("1"), Some("server")]);
        assert_eq!(
            mapper.map_change(&excluded, &change_to_excluded).unwrap(),
            None
        );

        let unmapped = relation("public", "nothing", &["id"]);
        assert_eq!(
            mapper
                .map_change(
                    &unmapped,
                    &change(&unmapped, ChangeKind::Insert, &[Some("1")])
                )
                .unwrap(),
            None
        );

        let things = relation("public", "things", &["id", "resource_type"]);
        let no_id = mapper
            .map_change(
                &things,
                &change(&things, ChangeKind::Insert, &[None, Some("server")]),
            )
            .unwrap_err();
        assert!(no_id.to_string().contains("replica identity"), "{no_id}");
        let no_kind = mapper
            .map_change(
                &things,
                &change(&things, ChangeKind::Delete, &[Some("1"), None]),
            )
            .unwrap_err();
        assert!(
            no_kind.to_string().contains("REPLICA IDENTITY FULL"),
            "{no_kind}"
        );
        let bad_name = mapper
            .map_change(
                &things,
                &change(&things, ChangeKind::Insert, &[Some("a b"), Some("server")]),
            )
            .unwrap_err();
        assert!(
            matches!(bad_name, Error::Mapping(ref m) if m.contains("a row of public.things")),
            "{bad_name}"
        );
    }

    #[test]
    fn a_row_names_its_resource_by_a_type_column_or_a_fixed_kind() {
        let mapper = TableMapper::new(
            vec![
                mapping("resources"),
                static_mapping("infra", "virtual_machines", "vm"),
            ],
            Filters::default(),
        );
        let resources = relation("public", "resources", &["id", "resource_type", "cpu"]);
        let found = mapper
            .map_change(
                &resources,
                &change(
                    &resources,
                    ChangeKind::Insert,
                    &[Some("7"), Some("server"), Some("2")],
                ),
            )
            .unwrap();
        assert_eq!(found, Some("server.7".parse().unwrap()));

        let machines = relation("infra", "virtual_machines", &["uuid", "cpu"]);
        let found = mapper
            .map_change(
                &machines,
                &change(&machines, ChangeKind::Update, &[Some("a-b-c"), Some("4")]),
            )
            .unwrap();
        assert_eq!(found, Some("vm.a-b-c".parse().unwrap()));
    }

    #[test]
    fn tables_that_are_not_mapped_or_not_admitted_are_of_no_interest() {
        let filters = Filters {
            exclude_tables: vec!["public.resources".into()],
            ..Filters::default()
        };
        let mapper = TableMapper::new(vec![mapping("resources"), mapping("other")], filters);
        let resources = relation("public", "resources", &["id", "resource_type"]);
        let row = change(&resources, ChangeKind::Insert, &[Some("7"), Some("server")]);
        assert_eq!(
            mapper.map_change(&resources, &row).unwrap(),
            None,
            "excluded"
        );
        let unmapped = relation("public", "audit", &["id"]);
        let row = change(&unmapped, ChangeKind::Insert, &[Some("7")]);
        assert_eq!(
            mapper.map_change(&unmapped, &row).unwrap(),
            None,
            "unmapped"
        );
    }

    #[test]
    fn a_row_that_cannot_name_its_resource_is_an_error_that_says_why() {
        let mapper = TableMapper::new(vec![mapping("resources")], Filters::default());
        let resources = relation("public", "resources", &["id", "resource_type"]);

        // A delete with only the key: the kind is not known.
        let deleted = change(&resources, ChangeKind::Delete, &[Some("7"), None]);
        let error = mapper
            .map_change(&resources, &deleted)
            .unwrap_err()
            .to_string();
        assert!(error.contains("REPLICA IDENTITY FULL"), "{error}");

        // A delete with the whole row names its kind.
        let deleted = change(&resources, ChangeKind::Delete, &[Some("7"), Some("server")]);
        assert_eq!(
            mapper.map_change(&resources, &deleted).unwrap(),
            Some("server.7".parse().unwrap())
        );

        // Text that cannot be a name.
        let odd = change(
            &resources,
            ChangeKind::Insert,
            &[Some("a b"), Some("server")],
        );
        assert!(mapper.map_change(&resources, &odd).is_err());

        // No id at all.
        let nameless = change(&resources, ChangeKind::Insert, &[None, Some("server")]);
        assert!(
            mapper
                .map_change(&resources, &nameless)
                .unwrap_err()
                .to_string()
                .contains("id column")
        );
    }
}
