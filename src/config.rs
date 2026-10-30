//! What the controller is told, and the checks on it.
//!
//! A [`Config`] is read from one TOML file. Every table of the file is a
//! struct and every key a field, under the same names, and a key that is
//! not known is refused. The tables that are about PostgreSQL are defined
//! here: `[postgres]`, `[replication]`, `[filters]`, `[checkpoint]` and
//! `[references]`. The rest, the mappings, the engine, Morph, the providers,
//! the reconciliation and the logging, are the core's, in
//! [`morph_controller_core::config`], and are re-exported here under the
//! same names. [`Config::load`] and [`Config::parse`] let two environment
//! variables override the file and then check the whole with
//! [`Config::validate`], so a configuration that came from either can be
//! started with, as far as can be told without a database.
//!
//! Every other part takes its settings from here: the source the
//! [`ReplicationConfig`], the mapper the [`Filters`] and [`Mapping`]s, the
//! intake and the workers the [`CheckpointConfig`], the scheduler and the
//! periodic loop the [`ReconcileConfig`], and the stores and providers the
//! [`MorphConfig`] and [`ProviderConfig`]s.
//!
//! Durations are written as `500ms`, `30s`, `5m` or `1h`.

use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::Deserialize;

pub use morph_controller_core::config::{
    Backoff, CheckpointMode, EngineConfig, LifecycleConfig, LogFormat, LoggingConfig, Mapping,
    MorphConfig, ProviderConfig, PulumiConfig, ReconcileConfig, ReferenceConfig, RetryConfig,
    STACK_KIND, StateConfig, is_attribute_name, is_stack_name, validate_engine, validate_mappings,
};

use crate::error::{Error, Result};

/// Everything the controller is told, as one TOML file holds it.
///
/// Only `postgres`, `replication` and one mapping have to be given, plus
/// one provider with the Morph engine. Everything else has a default.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// How to reach the database: the `[postgres]` table. Required.
    pub postgres: PostgresConfig,
    /// The slot and publication to read from, and how the reading is paced:
    /// the `[replication]` table. Required.
    pub replication: ReplicationConfig,
    /// Which tables' changes are looked at at all: the `[filters]` table.
    /// Every table is admitted by default.
    #[serde(default)]
    pub filters: Filters,
    /// How the rows of each table become resources: one `[[mappings]]`
    /// block per table. At least one is required.
    #[serde(default)]
    pub mappings: Vec<Mapping>,
    /// How progress is recorded: the `[checkpoint]` table.
    #[serde(default)]
    pub checkpoint: CheckpointConfig,
    /// How reconciliations are run and retried: the `[reconcile]` table.
    #[serde(default)]
    pub reconcile: ReconcileConfig,
    /// How Morph is set up: the `[morph]` table.
    #[serde(default)]
    pub morph: MorphConfig,
    /// The providers that reach the resources: one `[[providers]]` block
    /// each. At least one is required with the Morph engine, and none may
    /// be given with the Pulumi engine.
    #[serde(default)]
    pub providers: Vec<ProviderConfig>,
    /// What reconciles a resource once the controller has decided to look
    /// at it: the `[engine]` table. Morph by default.
    #[serde(default)]
    pub engine: EngineConfig,
    /// What is logged, and how: the `[logging]` table.
    #[serde(default)]
    pub logging: LoggingConfig,
    /// How rows that refer to other rows are followed: the `[references]`
    /// table.
    #[serde(default)]
    pub references: ReferencesConfig,
}

/// How to reach the database: the `[postgres]` table.
///
/// The same settings serve the replication stream, the controller's own
/// queries, and Morph's records when they are kept in the database.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PostgresConfig {
    /// The database, as for `psql`: `postgres://user:password@host:port/database`,
    /// with `?sslmode=require` and the rest. Required.
    ///
    /// The variable `MPC_DATABASE_URL` takes precedence over the file. The
    /// replication connection cannot use a Unix socket, and needs the user
    /// to be named in the URL.
    pub url: String,
    /// A PEM file of certificate authorities to trust besides the public
    /// ones. None by default.
    ///
    /// A server whose certificate was signed by an authority of its own
    /// cannot be connected to over TLS without it.
    #[serde(default)]
    pub ca_certificate: Option<PathBuf>,
}

/// The slot and publication to read from, and how the reading is paced: the
/// `[replication]` table.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReplicationConfig {
    /// The replication slot. Required, and a plain identifier.
    ///
    /// The slot is the server's record of how far the controller has got,
    /// and is what lets a restart carry on where the last run stopped. The
    /// controller never drops a slot, so naming another leaves the old one
    /// holding back log on the server until it is dropped by hand.
    pub slot: String,
    /// The publication to listen to. Required, and a plain identifier.
    ///
    /// Only the tables in the publication send changes. A mapped table that
    /// is not in it is seen by the periodic loop alone.
    pub publication: String,
    /// Whether to make the slot if it does not exist. `true` by default.
    ///
    /// An existing slot is always reused. With `false`, a missing slot
    /// stops the controller from starting.
    #[serde(default = "yes")]
    pub create_slot: bool,
    /// Whether to make the publication, for the mapped tables, if it does
    /// not exist. `false` by default.
    ///
    /// With `false`, a missing publication stops the controller from
    /// starting. An existing publication is used as it is, and is not
    /// compared with the mappings.
    #[serde(default)]
    pub create_publication: bool,
    /// How often the server is told how far the controller has got. `10s`
    /// by default.
    ///
    /// The server only knows what it was last told, so a longer interval
    /// means that more is sent again after a crash.
    #[serde(default = "ten_seconds", with = "humantime_serde")]
    pub status_interval: Duration,
    /// How often the lag watcher reads how much log the slot is holding
    /// back. `30s` by default.
    #[serde(default = "thirty_seconds", with = "humantime_serde")]
    pub lag_check_interval: Duration,
    /// How many bytes of held-back log earn a warning at each reading.
    /// `1073741824`, one gibibyte, by default.
    ///
    /// The lag is reported and nothing more. No setting makes the
    /// controller act on it.
    #[serde(default = "a_gigabyte")]
    pub lag_warn_bytes: u64,
    /// How reconnecting is paced when the stream is lost or cannot be
    /// connected: the `[replication.reconnect]` table.
    ///
    /// The same pacing is used between tries at writing the dirty table.
    #[serde(default)]
    pub reconnect: Backoff,
}

/// Which tables' changes are looked at at all: the `[filters]` table.
///
/// A change to a table that the filters do not admit is dropped before it
/// is mapped. The filters apply to the stream of changes only: the periodic
/// loop and the workers read every mapped table, admitted or not.
///
/// Names are compared exactly. All four lists are empty by default, which
/// admits every table.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Filters {
    /// Only tables in these schemas are admitted, if any are given.
    #[serde(default)]
    pub include_schemas: Vec<String>,
    /// Only these tables, as `schema.table`, are admitted, if any are
    /// given.
    #[serde(default)]
    pub include_tables: Vec<String>,
    /// Tables in these schemas are never admitted.
    #[serde(default)]
    pub exclude_schemas: Vec<String>,
    /// These tables, as `schema.table`, are never admitted.
    #[serde(default)]
    pub exclude_tables: Vec<String>,
}

impl Filters {
    /// Reports whether changes to the table `schema.table` are of interest.
    ///
    /// An exclusion always wins. Otherwise the table has to pass both
    /// `include_schemas` and `include_tables`, each of which passes
    /// everything while it is empty.
    ///
    /// # Examples
    ///
    /// ```
    /// use morph_postgres_controller::config::Filters;
    ///
    /// let filters = Filters {
    ///     include_schemas: vec!["public".into()],
    ///     exclude_tables: vec!["public.audit".into()],
    ///     ..Filters::default()
    /// };
    /// assert!(filters.admits("public", "queues"));
    /// assert!(!filters.admits("public", "audit"));
    /// assert!(!filters.admits("infra", "queues"));
    ///
    /// // With nothing given, every table is admitted.
    /// assert!(Filters::default().admits("infra", "queues"));
    /// ```
    pub fn admits(&self, schema: &str, table: &str) -> bool {
        let qualified = format!("{schema}.{table}");
        if listed(&self.exclude_schemas, schema) || listed(&self.exclude_tables, &qualified) {
            return false;
        }
        let included_schema =
            self.include_schemas.is_empty() || listed(&self.include_schemas, schema);
        let included_table =
            self.include_tables.is_empty() || listed(&self.include_tables, &qualified);
        included_schema && included_table
    }
}

/// How progress is recorded: the `[checkpoint]` table.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CheckpointConfig {
    /// Where the resources still to be reconciled are kept, which decides
    /// when a transaction's position may be confirmed to the server.
    /// [`CheckpointMode::Durable`] by default.
    #[serde(default)]
    pub mode: CheckpointMode,
    /// The table that holds them in the durable mode.
    /// `morph_controller_dirty` by default, and a plain identifier.
    ///
    /// The controller makes the table if it does not exist. Naming another
    /// leaves behind whatever the old one held, to be found again only by a
    /// full reconciliation.
    #[serde(default = "dirty_table")]
    pub table: String,
}

impl Default for CheckpointConfig {
    fn default() -> Self {
        Self {
            mode: CheckpointMode::Durable,
            table: dirty_table(),
        }
    }
}

/// How rows that refer to other rows are followed: the `[references]`
/// table.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReferencesConfig {
    /// The table in which the controller remembers which resource refers
    /// to which. `morph_controller_references` by default, and a plain
    /// identifier.
    ///
    /// The controller makes the table if it does not exist. It is read
    /// before a resource is removed, so that nothing is removed while
    /// another resource still refers to it, and when what a resource
    /// exports changes, to find the resources that took the old value.
    #[serde(default = "references_table")]
    pub table: String,
}

impl Default for ReferencesConfig {
    fn default() -> Self {
        Self {
            table: references_table(),
        }
    }
}

// The defaults. They are functions because that is the form in which serde
// takes a default for a field.
fn yes() -> bool {
    true
}
fn dirty_table() -> String {
    "morph_controller_dirty".to_owned()
}
fn references_table() -> String {
    "morph_controller_references".to_owned()
}
fn a_gigabyte() -> u64 {
    1 << 30
}
fn ten_seconds() -> Duration {
    Duration::from_secs(10)
}
fn thirty_seconds() -> Duration {
    Duration::from_secs(30)
}

impl Config {
    /// Reads the configuration from the TOML file at `path`, lets the
    /// environment override what it may, and checks it.
    ///
    /// # Errors
    ///
    /// [`Error::Config`] if the file cannot be read, and for everything
    /// that [`parse`](Self::parse) refuses in its text.
    pub fn load(path: &Path) -> Result<Self> {
        let text = std::fs::read_to_string(path)
            .map_err(|error| Error::Config(format!("{}: {error}", path.display())))?;
        Self::parse(&text)
    }

    /// Reads the configuration from TOML text, lets the environment
    /// override what it may, and checks it.
    ///
    /// The text is read, then [`apply_environment`](Self::apply_environment)
    /// is applied, then the result is checked with
    /// [`validate`](Self::validate).
    ///
    /// # Errors
    ///
    /// [`Error::Config`], with a message that says where, if the text is
    /// not TOML, has a key that is not known, lacks a required key, or
    /// gives a value of the wrong type or a duration that cannot be read.
    /// The same for everything that [`validate`](Self::validate) refuses.
    ///
    /// # Examples
    ///
    /// ```
    /// use morph_postgres_controller::config::{CheckpointMode, Config};
    ///
    /// let config = Config::parse(
    ///     r#"
    ///     [postgres]
    ///     url = "postgres://morph:secret@localhost/morph"
    ///
    ///     [replication]
    ///     slot = "morph_controller"
    ///     publication = "morph_resources"
    ///
    ///     [[mappings]]
    ///     table = "queues"
    ///     resource_type = "aws_sqs_queue"
    ///
    ///     [[providers]]
    ///     kind = "mock"
    ///     "#,
    /// )?;
    /// assert_eq!(config.mappings[0].qualified_table(), "public.queues");
    /// assert_eq!(config.checkpoint.mode, CheckpointMode::Durable);
    /// assert_eq!(config.reconcile.workers, 16);
    ///
    /// // What cannot work is refused: here, a configuration with no mappings.
    /// let refused = Config::parse(
    ///     r#"
    ///     [postgres]
    ///     url = "postgres://morph:secret@localhost/morph"
    ///
    ///     [replication]
    ///     slot = "morph_controller"
    ///     publication = "morph_resources"
    ///     "#,
    /// );
    /// assert!(refused.unwrap_err().to_string().contains("no mappings"));
    /// # Ok::<(), morph_postgres_controller::Error>(())
    /// ```
    pub fn parse(text: &str) -> Result<Self> {
        let mut config: Config =
            toml::from_str(text).map_err(|error| Error::Config(error.to_string()))?;
        config.apply_environment();
        config.validate()?;
        Ok(config)
    }

    /// Lets `MPC_DATABASE_URL` override `postgres.url`, and `MPC_LOG`
    /// override `logging.level`.
    ///
    /// A variable that is not set, or that is not Unicode, leaves the value
    /// as it was. One that is set to nothing overrides with nothing.
    pub fn apply_environment(&mut self) {
        if let Ok(url) = std::env::var("MPC_DATABASE_URL") {
            self.postgres.url = url;
        }
        if let Ok(level) = std::env::var("MPC_LOG") {
            self.logging.level = level;
        }
    }

    /// Refuses what cannot work, as far as can be told without a database.
    ///
    /// [`load`](Self::load) and [`parse`](Self::parse) call this. So does
    /// [`Controller::start_with`](crate::app::Controller::start_with), so a
    /// configuration that was changed by hand after it was read is checked
    /// again.
    ///
    /// # Errors
    ///
    /// [`Error::Config`], naming the setting, for the first of these that
    /// is found:
    ///
    /// * `postgres.url` is empty.
    /// * `replication.slot`, `replication.publication`, `checkpoint.table`
    ///   or `references.table` is not a plain identifier, as
    ///   [`is_identifier`] has it.
    /// * A mapping's schema, table, `id_column`, `type_column`,
    ///   `attributes_column` or `stack_column`, or a reference's column,
    ///   schema or table, is not a plain identifier.
    /// * Anything [`validate_mappings`] refuses: no mappings, a mapping
    ///   with both or neither of `type_column` and `resource_type`, an
    ///   invalid kind, an invalid `exports` or stack, a reference on a
    ///   naming column or into an unmapped table, a table mapped twice.
    /// * Anything [`validate_engine`] refuses: no providers with Morph, or
    ///   providers, a bad project, prefix, work directory or type with
    ///   Pulumi.
    /// * Anything [`ReconcileConfig::validate`] refuses, and a
    ///   `replication.reconnect` that fails [`Backoff::validate`].
    ///
    /// Whether the URL can be read, and whether the tables, columns and
    /// kinds exist, is found out only at start or later.
    pub fn validate(&self) -> Result<()> {
        let bad = |what: String| Err(Error::Config(what));
        if self.postgres.url.is_empty() {
            return bad("postgres.url is empty".into());
        }
        for (name, value) in [
            ("replication.slot", &self.replication.slot),
            ("replication.publication", &self.replication.publication),
            ("checkpoint.table", &self.checkpoint.table),
            ("references.table", &self.references.table),
        ] {
            if !is_identifier(value) {
                return bad(format!("{name} {value:?} is not a plain identifier"));
            }
        }
        for mapping in &self.mappings {
            let table = mapping.qualified_table();
            for part in [&mapping.schema, &mapping.table, &mapping.id_column] {
                if !is_identifier(part) {
                    return bad(format!(
                        "mapping {table}: {part:?} is not a plain identifier"
                    ));
                }
            }
            for (name, column) in [
                ("type_column", &mapping.type_column),
                ("attributes_column", &mapping.attributes_column),
                ("stack_column", &mapping.stack_column),
            ] {
                if let Some(column) = column
                    && !is_identifier(column)
                {
                    return bad(format!(
                        "mapping {table}: {name} {column:?} is not a plain identifier"
                    ));
                }
            }
            for reference in &mapping.references {
                for part in [&reference.column, &reference.schema, &reference.table] {
                    if !is_identifier(part) {
                        return bad(format!(
                            "mapping {table}: reference {part:?} is not a plain identifier"
                        ));
                    }
                }
            }
        }
        validate_mappings(&self.mappings)?;
        validate_engine(&self.engine, &self.mappings, &self.providers)?;
        self.reconcile.validate()?;
        self.replication.reconnect.validate("replication.reconnect")
    }
}

/// Whether `wanted` is one of `list`.
fn listed(list: &[String], wanted: &str) -> bool {
    list.iter().any(|item| item.as_str() == wanted)
}

/// Reports whether `text` can be used as a SQL identifier without quoting
/// tricks.
///
/// That is: an ASCII letter or `_`, then ASCII letters, digits and `_`, and
/// no more than 63 bytes in all, which is as long as PostgreSQL keeps a
/// name. [`Config::validate`] holds to this every name from the
/// configuration that the controller puts into a statement.
///
/// # Examples
///
/// ```
/// use morph_postgres_controller::config::is_identifier;
///
/// assert!(is_identifier("resource_type"));
/// assert!(is_identifier("_private"));
/// assert!(!is_identifier("1abc"));
/// assert!(!is_identifier("a-b"));
/// assert!(!is_identifier(""));
/// ```
pub fn is_identifier(text: &str) -> bool {
    let mut chars = text.chars();
    matches!(chars.next(), Some(first) if first.is_ascii_alphabetic() || first == '_')
        && text.len() <= 63
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// Quotes `text` for use as an identifier in a statement.
///
/// The text is put in double quotes, and any double quote within it is
/// doubled, so PostgreSQL reads all of it as one name, whatever it holds. A
/// quoted name keeps its case.
///
/// # Examples
///
/// ```
/// use morph_postgres_controller::config::quote_identifier;
///
/// assert_eq!(quote_identifier("queues"), r#""queues""#);
/// assert_eq!(quote_identifier(r#"odd"name"#), r#""odd""name""#);
/// ```
pub fn quote_identifier(text: &str) -> String {
    format!("\"{}\"", text.replace('"', "\"\""))
}

#[cfg(test)]
mod tests {
    use super::*;

    const MINIMAL: &str = r#"
        [postgres]
        url = "postgres://morph:morph@localhost/morph"
        [replication]
        slot = "morph_controller"
        publication = "morph_resources"
        [[mappings]]
        table = "resources"
        type_column = "resource_type"
        [[providers]]
        kind = "mock"
    "#;

    #[test]
    fn the_defaults_are_sensible() {
        let config = Config::parse(MINIMAL).unwrap();
        assert_eq!(config.mappings[0].schema, "public");
        assert_eq!(config.mappings[0].id_column, "id");
        assert_eq!(config.reconcile.workers, 16);
        assert_eq!(config.reconcile.full_interval, Duration::from_secs(300));
        assert_eq!(config.checkpoint.mode, CheckpointMode::Durable);
        assert_eq!(config.morph.state, StateConfig::Postgres);
        assert!(config.replication.create_slot);
        assert!(!config.replication.create_publication);
    }

    #[test]
    fn grouping_into_stacks_is_checked() {
        let pulumi = |mapping_extra: &str| {
            format!(
                r#"
                [postgres]
                url = "postgres://morph:morph@localhost/morph"
                [replication]
                slot = "morph_controller"
                publication = "morph_resources"
                [[mappings]]
                table = "resources"
                type_column = "resource_type"
                {mapping_extra}
                [engine]
                kind = "pulumi"
                [engine.types]
                server = "random:index:RandomPet"
                "#
            )
        };
        assert!(Config::parse(&pulumi("stack = \"fleet\"")).is_ok());
        assert!(Config::parse(&pulumi("stack_column = \"env\"")).is_ok());
        let both = Config::parse(&pulumi("stack = \"fleet\"\nstack_column = \"env\"")).unwrap_err();
        assert!(both.to_string().contains("not both"), "{both}");
        let dotted = Config::parse(&pulumi("stack = \"a.b\"")).unwrap_err();
        assert!(dotted.to_string().contains("may hold only"), "{dotted}");
        let reserved = Config::parse(&pulumi("").replace(
            "type_column = \"resource_type\"",
            "resource_type = \"stack\"",
        ))
        .unwrap_err();
        assert!(reserved.to_string().contains("reserved"), "{reserved}");

        // The same checks hold with Morph, which groups rows into one state.
        let morph = |extra: &str| {
            format!("{MINIMAL}\n[[mappings]]\ntable = \"vms\"\nresource_type = \"vm\"\n{extra}")
        };
        let grouped = Config::parse(&morph("stack = \"fleet\"")).unwrap();
        assert!(grouped.mappings[1].grouped());
        assert!(Config::parse(&morph("stack_column = \"env\"")).is_ok());
        let both = Config::parse(&morph("stack = \"fleet\"\nstack_column = \"env\"")).unwrap_err();
        assert!(both.to_string().contains("not both"), "{both}");
        let dotted = Config::parse(&morph("stack = \"a.b\"")).unwrap_err();
        assert!(dotted.to_string().contains("may hold only"), "{dotted}");
        let column = Config::parse(&morph("stack_column = \"my env\"")).unwrap_err();
        assert!(column.to_string().contains("stack_column"), "{column}");
        let reserved = Config::parse(
            &morph("").replace("resource_type = \"vm\"", "resource_type = \"stack\""),
        )
        .unwrap_err();
        assert!(reserved.to_string().contains("reserved"), "{reserved}");
    }

    #[test]
    fn pulumi_settings_have_defaults_and_are_checked() {
        let base = r#"
            [postgres]
            url = "postgres://morph:morph@localhost/morph"
            [replication]
            slot = "morph_controller"
            publication = "morph_resources"
            [[mappings]]
            table = "vms"
            resource_type = "vm"
        "#;
        // Scalar keys of [engine] first, then the [engine.types] table.
        let with = |scalars: &str, types: &str| {
            format!("{base}\n[engine]\nkind = \"pulumi\"\n{scalars}\n[engine.types]\n{types}")
        };
        let config = Config::parse(&with("", "vm = \"gcp:compute:Instance\"")).unwrap();
        let EngineConfig::Pulumi(pulumi) = &config.engine else {
            panic!("the pulumi engine");
        };
        assert_eq!(pulumi.binary, PathBuf::from("pulumi"));
        assert_eq!(pulumi.project, "morph-controller");
        assert_eq!(pulumi.work_dir, PathBuf::from(".pulumi-controller"));
        assert!(pulumi.backend.is_none() && pulumi.home.is_none());
        assert!(pulumi.passphrase_file.is_none());
        assert!(pulumi.refresh && pulumi.camel_case_columns);
        assert!(pulumi.policy_packs.is_empty() && pulumi.extra_args.is_empty());
        assert!(pulumi.config.is_empty());
        assert_eq!(pulumi.stack_prefix, "");

        let refused = |scalars: &str, types: &str, expected: &str| {
            let error = Config::parse(&with(scalars, types))
                .unwrap_err()
                .to_string();
            assert!(
                error.contains(expected),
                "{scalars:?} + {types:?} gave {error:?}, expected {expected:?}"
            );
        };
        let ok = "vm = \"gcp:compute:Instance\"";
        refused("project = \"\"", ok, "engine.project");
        refused("project = \"bad name\"", ok, "engine.project");
        refused("stack_prefix = \"bad/\"", ok, "engine.stack_prefix");
        refused("work_dir = \"\"", ok, "work_dir is empty");
        refused("bogus = 1", ok, "bogus");
        refused("", "", "no Pulumi type for \"vm\"");
        refused(
            "",
            "vm = \"gcp:compute:Instance\"\n\"a.b\" = \"x:y:Z\"",
            "not a valid kind",
        );
        refused("", "vm = \"nocolon\"", "not a Pulumi type token");
        refused(
            "",
            "vm = \"gcp:compute:Instance\"\nstack = \"x:y:Z\"",
            "reserved",
        );
        let providers = format!("{}\n[[providers]]\nkind = \"mock\"", with("", ok));
        let error = Config::parse(&providers).unwrap_err().to_string();
        assert!(
            error.contains("providers are the Morph engine's"),
            "{error}"
        );
    }

    #[test]
    fn a_file_is_read_and_a_missing_or_unknown_key_is_named() {
        let dir = std::env::temp_dir().join(format!("mpc-config-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("controller.toml");
        std::fs::write(&path, MINIMAL).unwrap();
        let config = Config::load(&path).unwrap();
        assert_eq!(config.mappings.len(), 1);
        assert!(config.mappings[0].qualified_table() == "public.resources");
        assert!(!config.mappings[0].grouped());
        let error = Config::load(&dir.join("absent.toml")).unwrap_err();
        assert!(matches!(error, Error::Config(_)), "{error}");
        let error = Config::parse(&format!("{MINIMAL}\n[logging]\nbogus = true")).unwrap_err();
        assert!(error.to_string().contains("bogus"), "{error}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn every_other_refusal_names_what_is_wrong() {
        let base = |mappings: &str, rest: &str| {
            format!(
                "[postgres]\nurl = \"postgres://morph:morph@localhost/morph\"\n[replication]\nslot = \"s\"\npublication = \"p\"\n{mappings}\n[[providers]]\nkind = \"mock\"\n{rest}"
            )
        };
        let mapping = "[[mappings]]\ntable = \"t\"\ntype_column = \"kind\"";
        let refused = |text: String, expected: &str| {
            let error = Config::parse(&text).unwrap_err().to_string();
            assert!(
                error.contains(expected),
                "expected {expected:?}, got {error:?}"
            );
        };
        refused(
            base(mapping, "").replace(
                "url = \"postgres://morph:morph@localhost/morph\"",
                "url = \"\"",
            ),
            "postgres.url is empty",
        );
        refused(base("", ""), "no mappings");
        refused(
            base(
                "[[mappings]]\ntable = \"bad table\"\ntype_column = \"kind\"",
                "",
            ),
            "not a plain identifier",
        );
        refused(
            base(
                "[[mappings]]\nschema = \"my schema\"\ntable = \"t\"\ntype_column = \"kind\"",
                "",
            ),
            "not a plain identifier",
        );
        refused(
            base(
                "[[mappings]]\ntable = \"t\"\nid_column = \"my id\"\ntype_column = \"kind\"",
                "",
            ),
            "not a plain identifier",
        );
        refused(
            base("[[mappings]]\ntable = \"t\"\ntype_column = \"my kind\"", ""),
            "type_column",
        );
        refused(
            base("[[mappings]]\ntable = \"t\"\nresource_type = \"a.b\"", ""),
            "not a valid kind",
        );
        refused(
            base(
                "[[mappings]]\ntable = \"t\"\ntype_column = \"kind\"\nattributes_column = \"my spec\"",
                "",
            ),
            "attributes_column",
        );
        refused(base(mapping, "[reconcile]\nqueue_size = 0"), "queue_size");
        refused(
            base(mapping, "[reconcile.retry]\nmax_attempts = 0"),
            "max_attempts",
        );
        refused(
            base(mapping, "[reconcile.retry]\nmultiplier = 0.5"),
            "multiplier",
        );
        refused(
            base(mapping, "[replication.reconnect]\ninitial = \"0s\""),
            "initial",
        );
        // The stack column is checked too, under the Pulumi engine.
        let pulumi = String::from(
            "[postgres]\nurl = \"postgres://morph:morph@localhost/morph\"\n[replication]\nslot = \"s\"\npublication = \"p\"\n[[mappings]]\ntable = \"t\"\ntype_column = \"kind\"\nstack_column = \"my env\"\n[engine]\nkind = \"pulumi\"",
        );
        refused(pulumi, "stack_column");
    }

    #[test]
    fn what_cannot_work_is_refused() {
        let refused =
            |edit: &dyn Fn(&str) -> String| Config::parse(&edit(MINIMAL)).unwrap_err().to_string();
        assert!(
            refused(&|t| t.replace("type_column = \"resource_type\"", ""))
                .contains("type_column or resource_type")
        );
        assert!(
            refused(&|t| t.replace(
                "type_column = \"resource_type\"",
                "type_column = \"t\"\nresource_type = \"vm\""
            ))
            .contains("not both")
        );
        assert!(
            refused(&|t| t.replace("[[providers]]\n        kind = \"mock\"", ""))
                .contains("no providers")
        );
        assert!(
            refused(&|t| t.replace("slot = \"morph_controller\"", "slot = \"bad slot\""))
                .contains("not a plain identifier")
        );
        assert!(
            refused(&|t| t.replace("[[mappings]]", "[reconcile]\nworkers = 0\n[[mappings]]"))
                .contains("workers")
        );
        assert!(
            refused(&|t| t.replace(
                "[[mappings]]",
                "[[mappings]]\ntable = \"resources\"\nresource_type = \"vm\"\n[[mappings]]"
            ))
            .contains("given twice")
        );
        assert!(
            refused(&|t| t.replace("[postgres]", "[nonsense]\nx = 1\n[postgres]"))
                .contains("nonsense")
        );
    }

    #[test]
    fn durations_and_sizes_are_read_as_people_write_them() {
        let text = MINIMAL.replace(
            "[replication]",
            "[reconcile]\nfull_interval = \"2m 30s\"\ntimeout = \"1h\"\n[reconcile.retry]\nmax_attempts = 3\ninitial = \"500ms\"\n[replication]",
        );
        let config = Config::parse(&text).unwrap();
        assert_eq!(config.reconcile.full_interval, Duration::from_secs(150));
        assert_eq!(config.reconcile.timeout, Duration::from_secs(3600));
        assert_eq!(config.reconcile.retry.max_attempts, 3);
        assert_eq!(
            config.reconcile.retry.backoff.initial,
            Duration::from_millis(500)
        );
    }

    #[test]
    fn backoff_grows_and_is_capped() {
        let backoff = Backoff {
            initial: Duration::from_secs(1),
            max: Duration::from_secs(10),
            multiplier: 2.0,
        };
        let within = |attempt, low: f64, high: f64| {
            let delay = backoff.delay(attempt).as_secs_f64();
            assert!(
                delay >= low * 0.8 - 1e-9 && delay <= high * 1.2 + 1e-9,
                "attempt {attempt}: {delay}"
            );
        };
        within(1, 1.0, 1.0);
        within(2, 2.0, 2.0);
        within(4, 8.0, 8.0);
        within(10, 10.0, 10.0);
    }

    #[test]
    fn filters_admit_what_they_say() {
        let filters = Filters {
            include_schemas: vec!["public".into(), "infra".into()],
            include_tables: vec![],
            exclude_schemas: vec![],
            exclude_tables: vec!["public.audit".into()],
        };
        assert!(filters.admits("public", "resources"));
        assert!(!filters.admits("public", "audit"));
        assert!(!filters.admits("other", "resources"));
        assert!(Filters::default().admits("anything", "at_all"));
    }

    #[test]
    fn references_are_checked() {
        let with = |topics_extra: &str, subscriptions_extra: &str| {
            format!(
                r#"
                [postgres]
                url = "postgres://morph:morph@localhost/morph"
                [replication]
                slot = "morph_controller"
                publication = "morph_resources"
                [[mappings]]
                table = "topics"
                resource_type = "topic"
                {topics_extra}
                [[mappings]]
                table = "subscriptions"
                resource_type = "subscription"
                stack_column = "env"
                {subscriptions_extra}
                [engine]
                kind = "pulumi"
                [engine.types]
                topic = "gcp:pubsub:Topic"
                subscription = "gcp:pubsub:Subscription"
                "#
            )
        };
        let config = Config::parse(&with("", "")).unwrap();
        assert_eq!(config.references.table, "morph_controller_references");
        assert_eq!(config.mappings[0].exports, "id");
        assert!(config.mappings[0].foreign_keys);
        assert!(config.mappings[0].references.is_empty());

        let declared = Config::parse(&with(
            "exports = \"arn\"\nforeign_keys = false",
            "[[mappings.references]]\ncolumn = \"topic\"\ntable = \"topics\"\n[[mappings.references]]\ncolumn = \"dead_letter\"\ntable = \"topics\"\nattribute = \"selfLink\"",
        ))
        .unwrap();
        assert_eq!(declared.mappings[0].exports, "arn");
        assert!(!declared.mappings[0].foreign_keys);
        let references = &declared.mappings[1].references;
        assert_eq!(references.len(), 2);
        assert_eq!(references[0].schema, "public");
        assert_eq!(references[0].attribute, None);
        assert_eq!(references[1].attribute.as_deref(), Some("selfLink"));
        assert!(declared.mappings[1].names_resource_with("id"));
        assert!(declared.mappings[1].names_resource_with("env"));
        assert!(!declared.mappings[1].names_resource_with("topic"));

        let refused = |topics_extra: &str, subscriptions_extra: &str, expected: &str| {
            let error = Config::parse(&with(topics_extra, subscriptions_extra))
                .unwrap_err()
                .to_string();
            assert!(
                error.contains(expected),
                "{topics_extra:?} + {subscriptions_extra:?} gave {error:?}, expected {expected:?}"
            );
        };
        refused("exports = \"\"", "", "exports");
        refused("exports = \"a.b\"", "", "exports");
        refused(
            "",
            "[[mappings.references]]\ncolumn = \"my col\"\ntable = \"topics\"",
            "not a plain identifier",
        );
        refused(
            "",
            "[[mappings.references]]\ncolumn = \"topic\"\ntable = \"topics\"\nschema = \"my schema\"",
            "not a plain identifier",
        );
        refused(
            "",
            "[[mappings.references]]\ncolumn = \"id\"\ntable = \"topics\"",
            "names the resource",
        );
        refused(
            "",
            "[[mappings.references]]\ncolumn = \"env\"\ntable = \"topics\"",
            "names the resource",
        );
        refused(
            "",
            "[[mappings.references]]\ncolumn = \"topic\"\ntable = \"topics\"\n[[mappings.references]]\ncolumn = \"topic\"\ntable = \"topics\"",
            "twice",
        );
        refused(
            "",
            "[[mappings.references]]\ncolumn = \"topic\"\ntable = \"topics\"\nattribute = \"a.b\"",
            "not an attribute name",
        );
        refused(
            "",
            "[[mappings.references]]\ncolumn = \"topic\"\ntable = \"users\"",
            "not mapped",
        );
        refused(
            "",
            "[[mappings.references]]\ncolumn = \"topic\"\ntable = \"topics\"\nbogus = 1",
            "bogus",
        );
        let error = Config::parse(&format!(
            "{}\n[references]\ntable = \"bad table\"",
            with("", "")
        ))
        .unwrap_err();
        assert!(error.to_string().contains("references.table"), "{error}");
        assert!(is_attribute_name("selfLink") && is_attribute_name("self_link"));
        assert!(!is_attribute_name("") && !is_attribute_name("a.b") && !is_attribute_name("a b"));
    }

    #[test]
    fn identifiers_are_quoted_safely() {
        assert_eq!(quote_identifier("plain"), "\"plain\"");
        assert_eq!(quote_identifier("odd\"name"), "\"odd\"\"name\"");
        assert!(is_identifier("resource_type"));
        assert!(!is_identifier("1abc") && !is_identifier("a-b") && !is_identifier(""));
    }
}

/// The example files under `examples/` are documentation as much as
/// configuration, so each has to parse and pass the checks.
#[cfg(test)]
mod examples {
    use super::*;

    #[test]
    fn every_example_configuration_is_valid() {
        let directory = Path::new(env!("CARGO_MANIFEST_DIR")).join("examples");
        let mut found = 0;
        for entry in std::fs::read_dir(&directory).expect("the examples directory exists") {
            let path = entry.unwrap().path();
            if path
                .extension()
                .is_some_and(|extension| extension == "toml")
            {
                let text = std::fs::read_to_string(&path).unwrap();
                Config::parse(&text).unwrap_or_else(|error| panic!("{}: {error}", path.display()));
                found += 1;
            }
        }
        assert!(found >= 4, "only {found} examples were found");
    }
}
