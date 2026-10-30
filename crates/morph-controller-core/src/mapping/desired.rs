//! A row, as the desired state of a resource.
//!
//! When a worker reconciles a resource, the source reads the row that
//! describes it, as a [`Row`], and [`desired_from_rows`] makes of it the
//! desired state that Morph is given. Nothing here looks at the stream of
//! changes. The stream said which resource to look at, and that is all it
//! is trusted with.

use std::collections::{BTreeMap, BTreeSet};

use morph::{
    DesiredState, Expr, Lifecycle, RemovalPolicy, ReplaceOrder, ResourceId, ResourceSpec, Value,
};

use crate::config::{Mapping, STACK_KIND};
use crate::error::{Error, Result};

/// A row that describes a resource, with the mapping of its table.
///
/// This is what an engine is handed: the row as a JSON object, one entry
/// per column, with its reference columns resolved, and the mapping that
/// says which of its columns matter and how the resource is to be treated.
#[derive(Clone, Debug)]
pub struct Row {
    /// The resource the row describes.
    pub resource: ResourceId,
    /// The mapping of the table the row came from.
    pub mapping: Mapping,
    /// The row, as a JSON object. A reference column holds what the
    /// referenced resource exports once the row has been resolved, and the
    /// row id before.
    pub json: serde_json::Value,
    /// The attributes of the resource that other rows refer to. The engine
    /// exports them, and reports when they change. Empty until the row has
    /// been resolved, and for a resource nothing refers to.
    pub exports: BTreeSet<String>,
    /// Reference columns whose target is reconciled as part of the same
    /// stack as this row. The engine resolves each itself: Morph as a
    /// reference between the two resources of its plan, Pulumi as an
    /// expression in the program. Empty until the row has been resolved,
    /// and for every row that is a record of its own.
    pub links: Vec<Link>,
    /// The identifier by which the engine is to take charge of an object
    /// that already exists, from the mapping's `import_column`. None when
    /// the mapping has no such column or the row has nothing there.
    pub import_id: Option<String>,
}

/// A reference column that points within the row's own stack.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Link {
    /// The column.
    pub column: String,
    /// The resource the column refers to, a member of the same stack.
    pub target: ResourceId,
    /// The attribute of that resource that the column takes.
    pub attribute: String,
}

impl Row {
    /// A row as it was read: nothing resolved yet.
    pub fn new(resource: ResourceId, mapping: Mapping, json: serde_json::Value) -> Self {
        let import_id =
            mapping
                .import_column
                .as_deref()
                .and_then(|column| match json.get(column)? {
                    serde_json::Value::Null => None,
                    serde_json::Value::String(text) => Some(text.clone()),
                    other => Some(other.to_string()),
                });
        Self {
            resource,
            mapping,
            json,
            exports: BTreeSet::new(),
            links: Vec::new(),
            import_id,
        }
    }

    /// A row as its table's mapping reads it, naming its own resource.
    ///
    /// The name is the text of the id column, and the kind is the
    /// mapping's `resource_type` or the text of its `type_column`. A
    /// number in either column names by its text. The import id, if the
    /// mapping has an `import_column`, is the text there. This is how a
    /// source turns what it read into a row, when the resource is not
    /// known beforehand.
    ///
    /// # Errors
    ///
    /// [`Error::Mapping`] if the row is not a JSON object, has nothing in
    /// the id column or the type column, if the mapping names no kind, or
    /// if the kind and the name do not make a valid resource id.
    pub fn from_json(mapping: &Mapping, json: serde_json::Value) -> Result<Self> {
        let table = mapping.qualified_table();
        if !json.is_object() {
            return Err(Error::Mapping(format!("a row of {table} is not an object")));
        }
        let text = |column: &str| -> Option<String> {
            match json.get(column)? {
                serde_json::Value::Null => None,
                serde_json::Value::String(text) => Some(text.clone()),
                other => Some(other.to_string()),
            }
        };
        let name = text(&mapping.id_column).ok_or_else(|| {
            Error::Mapping(format!("a row of {table} has no {}", mapping.id_column))
        })?;
        let kind = match (&mapping.resource_type, &mapping.type_column) {
            (Some(kind), _) => kind.clone(),
            (None, Some(column)) => text(column).ok_or_else(|| {
                Error::Mapping(format!("the row {name:?} of {table} has no {column}"))
            })?,
            (None, None) => {
                return Err(Error::Mapping(format!(
                    "the mapping for {table} names no kind"
                )));
            }
        };
        let resource = ResourceId::new(kind.as_str(), name.as_str())
            .map_err(|error| Error::Mapping(format!("the row {name:?} of {table}: {error}")))?;
        Ok(Self::new(resource, mapping.clone(), json))
    }

    /// The resource that is reconciled to reconcile this row: the row's
    /// own, or the stack that groups it, `stack.<name>`.
    ///
    /// # Errors
    ///
    /// [`Error::Mapping`] if the table is grouped by a `stack_column` and
    /// the row has nothing there, or if the stack's name is not valid.
    pub fn unit(&self) -> Result<ResourceId> {
        let stack = match (&self.mapping.stack, &self.mapping.stack_column) {
            (Some(stack), _) => stack.clone(),
            (None, Some(column)) => match self.json.get(column) {
                Some(serde_json::Value::String(text)) => text.clone(),
                Some(other) if !other.is_null() => other.to_string(),
                _ => {
                    return Err(Error::Mapping(format!(
                        "the row of {} has no {column}, which names its stack",
                        self.resource
                    )));
                }
            },
            (None, None) => return Ok(self.resource.clone()),
        };
        ResourceId::new(STACK_KIND, stack.as_str())
            .map_err(|error| Error::Mapping(format!("the row of {}: {error}", self.resource)))
    }
}

/// The specification of `resource`, from a row of the mapped table.
///
/// The row is given as the JSON object that `to_jsonb(row)` makes of it.
///
/// Every column is an attribute, except the id, type and stack columns,
/// the excluded ones, and those outside `attribute_columns` if that is
/// given.
/// The column named as `attributes_column` is left out too: merging its
/// contents over the rest is the work of [`desired_from_row`]. A null is not
/// an attribute at all.
///
/// The lifecycle settings of the mapping are carried over as they are.
///
/// # Errors
///
/// [`Error::Mapping`] if `row` is not a JSON object, or if a column holds a
/// value that Morph cannot represent.
pub fn spec_from_row(
    mapping: &Mapping,
    resource: ResourceId,
    row: serde_json::Value,
) -> Result<ResourceSpec> {
    let (attributes, _) = split_row(mapping, row)?;
    let mut spec = ResourceSpec::new(resource);
    for (name, value) in attributes {
        let value: Value = serde_json::from_value(value).map_err(|error| {
            Error::Mapping(format!(
                "column {name} of {}: {error}",
                mapping.qualified_table()
            ))
        })?;
        spec = spec.attr(name, value);
    }
    let lifecycle = Lifecycle {
        prevent_destroy: mapping.lifecycle.prevent_destroy,
        removal_policy: match mapping.lifecycle.retain {
            true => RemovalPolicy::Retain,
            false => RemovalPolicy::Delete,
        },
        replace_order: match mapping.lifecycle.create_before_destroy {
            true => ReplaceOrder::CreateBeforeDestroy,
            false => ReplaceOrder::DestroyBeforeCreate,
        },
        ..Lifecycle::default()
    };
    Ok(spec.lifecycle(lifecycle))
}

/// The desired state of `resource` from a row, with the attributes of a
/// JSON column merged in if the mapping names one.
///
/// This is [`desired_from_rows`] for one row that has no links.
///
/// # Errors
///
/// As [`spec_of_row`] fails.
pub fn desired_from_row(
    mapping: &Mapping,
    resource: ResourceId,
    row: serde_json::Value,
) -> Result<DesiredState> {
    desired_from_rows(&[Row::new(resource, mapping.clone(), row)])
}

/// The desired state of a stack, or of a single resource: one
/// specification for each of `rows`, as [`spec_of_row`] makes them.
///
/// Morph plans the specifications together. Where one row links to
/// another of the same stack, the plan orders the two and carries the
/// value across, and a replacement of the one spreads to the other, all
/// within one plan.
///
/// # Errors
///
/// As [`spec_of_row`] fails for any of the rows.
pub fn desired_from_rows(rows: &[Row]) -> Result<DesiredState> {
    let mut desired = DesiredState::new();
    for row in rows {
        desired = desired.with(spec_of_row(row)?);
    }
    Ok(desired)
}

/// The specification of a row's resource: its columns, the entries of its
/// attributes column, and its links.
///
/// The columns of the row become attributes as [`spec_from_row`] says. The
/// entries of the object in the `attributes_column` are then added, each in
/// place of a column of the same name. An entry that is null is left out.
/// The column settings of the mapping do not apply to the entries. An
/// attributes column that is null or missing adds nothing. Last, each link
/// becomes a reference to the linked resource's attribute, in place of
/// whatever the column held, so that Morph resolves it within the plan.
///
/// # Errors
///
/// [`Error::Mapping`] if the row is not a JSON object, if the attributes
/// column holds anything but an object or null, or if a value is one that
/// Morph cannot represent.
pub fn spec_of_row(row: &Row) -> Result<ResourceSpec> {
    let mapping = &row.mapping;
    let mut json = row.json.clone();
    let nested = mapping
        .attributes_column
        .as_ref()
        .and_then(|column| json.get_mut(column).map(serde_json::Value::take));
    let mut spec = spec_from_row(mapping, row.resource.clone(), json)?;
    for (name, value) in nested_entries(mapping, nested)? {
        let value: Value = serde_json::from_value(value).map_err(|error| {
            Error::Mapping(format!(
                "attribute {name} of {}: {error}",
                mapping.qualified_table()
            ))
        })?;
        spec = spec.attr(name, value);
    }
    for link in &row.links {
        spec = spec.attr(
            link.column.clone(),
            Expr::reference(link.target.clone(), link.attribute.as_str()),
        );
    }
    Ok(spec)
}

/// The properties of a resource from a row, for an engine that takes them
/// as JSON.
///
/// The columns become properties by the same rule as [`spec_from_row`]
/// makes attributes of them, and the entries of the `attributes_column`
/// are laid over them as [`desired_from_row`] lays them. With
/// `camel_case`, the name of each column is turned into `camelCase` by
/// [`camel_case`], which is how Pulumi's providers name their properties.
/// The entries of the attributes column are passed on as written, in
/// either case.
///
/// # Errors
///
/// [`Error::Mapping`] if `row` is not a JSON object, or if the attributes
/// column holds anything but an object or null.
pub fn properties_from_row(
    mapping: &Mapping,
    row: serde_json::Value,
    camel_case: bool,
) -> Result<serde_json::Map<String, serde_json::Value>> {
    let (columns, nested) = split_row(mapping, row)?;
    let mut properties = serde_json::Map::new();
    for (name, value) in columns {
        let name = if camel_case {
            self::camel_case(&name)
        } else {
            name
        };
        properties.insert(name, value);
    }
    for (name, value) in nested_entries(mapping, nested)? {
        properties.insert(name, value);
    }
    Ok(properties)
}

/// The columns of a row that are attributes, and the attributes column if
/// the mapping names one and the row has it.
///
/// Every column is kept except the id, type, stack and import columns,
/// the excluded ones, those outside `attribute_columns` if that is given,
/// the attributes column, and any that is null.
///
/// # Errors
///
/// [`Error::Mapping`] if `row` is not a JSON object.
fn split_row(
    mapping: &Mapping,
    row: serde_json::Value,
) -> Result<(
    BTreeMap<String, serde_json::Value>,
    Option<serde_json::Value>,
)> {
    let serde_json::Value::Object(columns) = row else {
        return Err(Error::Mapping(format!(
            "a row of {} is not an object",
            mapping.qualified_table()
        )));
    };
    let mut attributes = BTreeMap::new();
    let mut nested = None;
    for (name, value) in columns {
        if mapping.attributes_column.as_deref() == Some(&name) {
            nested = Some(value);
            continue;
        }
        let skipped = name == mapping.id_column
            || mapping.type_column.as_deref() == Some(&name)
            || mapping.stack_column.as_deref() == Some(&name)
            || mapping.import_column.as_deref() == Some(&name)
            || mapping.exclude_columns.contains(&name)
            || mapping
                .attribute_columns
                .as_ref()
                .is_some_and(|only| !only.contains(&name));
        if skipped || value.is_null() {
            continue;
        }
        attributes.insert(name, value);
    }
    Ok((attributes, nested))
}

/// The entries of an attributes column that add something: those that are
/// not null, in the order of their names.
///
/// # Errors
///
/// [`Error::Mapping`] if the column holds anything but an object or null.
fn nested_entries(
    mapping: &Mapping,
    nested: Option<serde_json::Value>,
) -> Result<Vec<(String, serde_json::Value)>> {
    match nested {
        Some(serde_json::Value::Object(entries)) => Ok(entries
            .into_iter()
            .filter(|(_, value)| !value.is_null())
            .collect()),
        Some(serde_json::Value::Null) | None => Ok(Vec::new()),
        Some(other) => Err(Error::Mapping(format!(
            "the attributes column of {} holds {other}, not an object",
            mapping.qualified_table()
        ))),
    }
}

/// A `snake_case` name as `camelCase`: `machine_type` becomes
/// `machineType`.
///
/// Each `_` is dropped and the letter after it is made upper case. A name
/// without `_` is returned as it is, so a name that is `camelCase` already
/// is unchanged. Underscores at the ends are dropped too.
///
/// # Examples
///
/// ```
/// use morph_controller_core::mapping::desired::camel_case;
///
/// assert_eq!(camel_case("machine_type"), "machineType");
/// assert_eq!(camel_case("allow_stopping_for_update"), "allowStoppingForUpdate");
/// assert_eq!(camel_case("machineType"), "machineType");
/// assert_eq!(camel_case("zone"), "zone");
/// ```
pub fn camel_case(name: &str) -> String {
    let mut out = String::with_capacity(name.len());
    let mut upper_next = false;
    for c in name.chars() {
        if c == '_' {
            upper_next = !out.is_empty();
            continue;
        }
        if upper_next {
            out.extend(c.to_uppercase());
            upper_next = false;
        } else {
            out.push(c);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use morph::Expr;
    use serde_json::json;

    fn mapping(extra: &str) -> Mapping {
        toml::from_str(&format!(
            "table = \"resources\"\ntype_column = \"resource_type\"\n{extra}"
        ))
        .unwrap()
    }

    fn id() -> ResourceId {
        "server.7".parse().unwrap()
    }

    #[test]
    fn every_column_but_the_naming_ones_is_an_attribute() {
        let row = json!({"id": 7, "resource_type": "server", "name": "web", "cpu": 2, "tags": {"team": "web"}, "ports": [80, 443], "note": null});
        let desired = desired_from_row(&mapping(""), id(), row).unwrap();
        let spec = desired.get(&id()).unwrap();
        let names: Vec<&String> = spec.attributes().keys().collect();
        assert_eq!(names, ["cpu", "name", "ports", "tags"]);
        assert_eq!(spec.attributes()["cpu"], Expr::from(Value::from(2)));
        assert_eq!(
            spec.attributes()["ports"],
            Expr::from(Value::list([Value::from(80), Value::from(443)]))
        );
    }

    #[test]
    fn the_import_column_names_the_object_and_is_not_an_attribute() {
        let mapping = mapping("import_column = \"object\"");
        let row = Row::from_json(
            &mapping,
            json!({"id": 7, "resource_type": "server", "name": "web", "object": "i-123"}),
        )
        .unwrap();
        assert_eq!(row.import_id.as_deref(), Some("i-123"));
        let spec = spec_of_row(&row).unwrap();
        let names: Vec<&String> = spec.attributes().keys().collect();
        assert_eq!(names, ["name"]);
        let properties = properties_from_row(&mapping, row.json.clone(), true).unwrap();
        assert!(!properties.contains_key("object"));

        let numbered = Row::from_json(
            &mapping,
            json!({"id": 7, "resource_type": "server", "object": 42}),
        )
        .unwrap();
        assert_eq!(numbered.import_id.as_deref(), Some("42"));
        let without = Row::from_json(
            &mapping,
            json!({"id": 7, "resource_type": "server", "object": null}),
        )
        .unwrap();
        assert_eq!(without.import_id, None);
        let plain = Row::new(id(), self::mapping(""), json!({"id": 7, "object": "x"}));
        assert_eq!(
            plain.import_id, None,
            "without an import column there is none"
        );
    }

    #[test]
    fn columns_can_be_chosen_excluded_and_nested() {
        let row = json!({"id": 7, "resource_type": "server", "name": "web", "cpu": 2, "created_at": "2026-01-01", "spec": {"image": "linux-2", "cpu": 4}});
        let chosen = desired_from_row(
            &mapping("attribute_columns = [\"name\"]"),
            id(),
            row.clone(),
        )
        .unwrap();
        let names: Vec<&String> = chosen.get(&id()).unwrap().attributes().keys().collect();
        assert_eq!(names, ["name"]);

        let excluded = desired_from_row(
            &mapping("exclude_columns = [\"created_at\", \"spec\"]"),
            id(),
            row.clone(),
        )
        .unwrap();
        let names: Vec<&String> = excluded.get(&id()).unwrap().attributes().keys().collect();
        assert_eq!(names, ["cpu", "name"]);

        let nested = desired_from_row(&mapping("attributes_column = \"spec\""), id(), row).unwrap();
        let spec = nested.get(&id()).unwrap();
        assert_eq!(
            spec.attributes()["cpu"],
            Expr::from(Value::from(4)),
            "the nested value wins"
        );
        assert_eq!(
            spec.attributes()["image"],
            Expr::from(Value::from("linux-2"))
        );
        assert!(!spec.attributes().contains_key("spec"));
    }

    #[test]
    fn the_lifecycle_comes_from_the_mapping() {
        let row = json!({"id": 7, "resource_type": "server"});
        let desired = desired_from_row(
            &mapping(
                "[lifecycle]\nprevent_destroy = true\nretain = true\ncreate_before_destroy = true",
            ),
            id(),
            row,
        )
        .unwrap();
        let lifecycle = desired.get(&id()).unwrap().lifecycle_settings();
        assert!(lifecycle.prevent_destroy);
        assert_eq!(lifecycle.removal_policy, RemovalPolicy::Retain);
        assert_eq!(lifecycle.replace_order, ReplaceOrder::CreateBeforeDestroy);
    }

    #[test]
    fn what_is_not_a_row_or_not_an_object_is_refused() {
        assert!(desired_from_row(&mapping(""), id(), json!([1, 2])).is_err());
        let row = json!({"id": 7, "resource_type": "server", "spec": "not an object"});
        assert!(desired_from_row(&mapping("attributes_column = \"spec\""), id(), row).is_err());
    }

    #[test]
    fn rows_of_a_stack_become_one_desired_state_with_links_as_references() {
        let network = Row::new(
            "network.main".parse().unwrap(),
            mapping("stack = \"fleet\""),
            json!({"id": "main", "resource_type": "network", "name": "main", "cidr": "10.0.0.0/16"}),
        );
        let mut server = Row::new(
            id(),
            mapping("stack = \"fleet\"\nattributes_column = \"spec\""),
            json!({"id": 7, "resource_type": "server", "name": "web", "network_id": "main", "spec": {"network_id": "from the json column"}}),
        );
        server.links.push(Link {
            column: "network_id".into(),
            target: "network.main".parse().unwrap(),
            attribute: "id".into(),
        });
        let desired = desired_from_rows(&[network, server]).unwrap();
        assert_eq!(desired.len(), 2);
        let spec = desired.get(&id()).unwrap();
        assert_eq!(
            spec.attributes()["network_id"],
            Expr::reference("network.main".parse().unwrap(), "id"),
            "the link wins over the column and over the JSON column"
        );
        assert_eq!(spec.attributes()["name"], Expr::from(Value::from("web")));
        let network = desired.get(&"network.main".parse().unwrap()).unwrap();
        assert_eq!(
            network.attributes()["cidr"],
            Expr::from(Value::from("10.0.0.0/16"))
        );
        assert!(desired_from_rows(&[]).unwrap().is_empty());
    }
}

#[cfg(test)]
mod property_tests {
    use super::*;
    use serde_json::json;

    fn mapping(extra: &str) -> Mapping {
        toml::from_str(&format!(
            "table = \"resources\"\ntype_column = \"resource_type\"\n{extra}"
        ))
        .unwrap()
    }

    #[test]
    fn properties_are_camel_cased_columns_with_the_json_column_laid_over_as_written() {
        let mapping = mapping(
            "stack_column = \"env\"\nattributes_column = \"spec\"\nexclude_columns = [\"note\"]",
        );
        let row = json!({
            "id": 7, "resource_type": "server", "env": "prod", "note": "x", "gone": null,
            "machine_type": "e2-small", "boot_disk": {"auto_delete": true},
            "spec": {"network_interfaces": [{"network": "default"}], "machine_type": "e2-medium", "nothing": null}
        });
        let camel = properties_from_row(&mapping, row.clone(), true).unwrap();
        let names: Vec<&String> = camel.keys().collect();
        assert_eq!(
            names,
            [
                "bootDisk",
                "machineType",
                "machine_type",
                "network_interfaces"
            ],
            "id, kind, stack, excluded and null columns are left out; JSON entries keep their names"
        );
        assert_eq!(
            camel["bootDisk"]["auto_delete"], true,
            "values are never renamed"
        );
        assert_eq!(camel["machineType"], "e2-small");
        assert_eq!(camel["machine_type"], "e2-medium");

        let plain = properties_from_row(&mapping, row, false).unwrap();
        let names: Vec<&String> = plain.keys().collect();
        assert_eq!(names, ["boot_disk", "machine_type", "network_interfaces"]);
        assert_eq!(
            plain["machine_type"], "e2-medium",
            "the JSON column wins over a column of the same name"
        );
    }

    #[test]
    fn only_the_listed_columns_are_properties_when_a_list_is_given() {
        let mapping = mapping("attribute_columns = [\"name\", \"env\"]\nstack_column = \"env\"");
        let row =
            json!({"id": 1, "resource_type": "server", "name": "web", "env": "prod", "cpu": 2});
        let properties = properties_from_row(&mapping, row, true).unwrap();
        let names: Vec<&String> = properties.keys().collect();
        assert_eq!(
            names,
            ["name"],
            "the stack column is left out even when listed"
        );
    }

    #[test]
    fn a_json_column_that_is_not_an_object_is_refused_and_null_adds_nothing() {
        let mapping = mapping("attributes_column = \"spec\"");
        let bad = json!({"id": 1, "resource_type": "server", "spec": [1, 2]});
        let error = properties_from_row(&mapping, bad, true).unwrap_err();
        assert!(
            matches!(error, Error::Mapping(ref m) if m.contains("not an object")),
            "{error}"
        );
        let none = json!({"id": 1, "resource_type": "server", "spec": null, "name": "web"});
        let properties = properties_from_row(&mapping, none, true).unwrap();
        assert_eq!(properties.len(), 1);
        let not_a_row = properties_from_row(&mapping, json!("text"), true).unwrap_err();
        assert!(matches!(not_a_row, Error::Mapping(_)));
    }

    #[test]
    fn camel_case_handles_the_odd_names() {
        for (given, expected) in [
            ("machine_type", "machineType"),
            ("allow_stopping_for_update", "allowStoppingForUpdate"),
            ("machineType", "machineType"),
            ("zone", "zone"),
            ("", ""),
            ("_", ""),
            ("__a", "a"),
            ("a__b", "aB"),
            ("a_", "a"),
            ("A_B", "AB"),
            ("disk_1_size", "disk1Size"),
            ("a-b_c", "a-bC"),
        ] {
            assert_eq!(camel_case(given), expected, "{given:?}");
        }
    }

    #[test]
    fn a_row_carries_its_resource_and_mapping() {
        let row = Row::new("server.7".parse().unwrap(), mapping(""), json!({"id": 7}));
        assert_eq!(row.resource.name(), "7");
        assert!(format!("{row:?}").starts_with("Row"));
        assert_eq!(row.clone().json, row.json);
        assert!(row.exports.is_empty() && row.links.is_empty());
    }

    #[test]
    fn a_row_names_its_resource_from_its_columns_or_says_what_is_missing() {
        let by_column = mapping("");
        let row = Row::from_json(&by_column, json!({"id": 7, "resource_type": "server"})).unwrap();
        assert_eq!(
            row.resource.to_string(),
            "server.7",
            "a number names by its text"
        );
        let fixed: Mapping =
            toml::from_str("table = \"vms\"\nresource_type = \"vm\"\nid_column = \"uuid\"")
                .unwrap();
        let row = Row::from_json(&fixed, json!({"uuid": "a-b", "cpu": 2})).unwrap();
        assert_eq!(row.resource.to_string(), "vm.a-b");
        assert_eq!(row.json["cpu"], 2);

        let says =
            |json: serde_json::Value| Row::from_json(&by_column, json).unwrap_err().to_string();
        assert!(says(json!([1])).contains("not an object"));
        assert!(says(json!({"resource_type": "server"})).contains("has no id"));
        assert!(says(json!({"id": 7, "resource_type": null})).contains("has no resource_type"));
        assert!(says(json!({"id": "a b", "resource_type": "server"})).contains("\"a b\""));
        let nameless: Mapping = toml::from_str("table = \"t\"").unwrap();
        assert!(
            Row::from_json(&nameless, json!({"id": 1}))
                .unwrap_err()
                .to_string()
                .contains("names no kind")
        );
    }

    #[test]
    fn a_row_knows_the_unit_that_reconciles_it() {
        let own = Row::new("server.7".parse().unwrap(), mapping(""), json!({"id": 7}));
        assert_eq!(own.unit().unwrap(), own.resource);
        let fixed = Row::new(
            "server.7".parse().unwrap(),
            mapping("stack = \"fleet\""),
            json!({"id": 7}),
        );
        assert_eq!(fixed.unit().unwrap().to_string(), "stack.fleet");
        let by_column = |env: serde_json::Value| {
            Row::new(
                "server.7".parse().unwrap(),
                mapping("stack_column = \"env\""),
                json!({"id": 7, "env": env}),
            )
        };
        assert_eq!(
            by_column(json!("prod")).unit().unwrap().to_string(),
            "stack.prod"
        );
        assert_eq!(
            by_column(json!(3)).unit().unwrap().to_string(),
            "stack.3",
            "a number names a stack by its text"
        );
        assert!(by_column(json!(null)).unit().is_err());
        assert!(by_column(json!("a.b")).unit().is_err());
        let link = Link {
            column: "network".into(),
            target: "network.main".parse().unwrap(),
            attribute: "id".into(),
        };
        assert_eq!(link.clone(), link);
    }
}
