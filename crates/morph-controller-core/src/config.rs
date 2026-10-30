//! The part of the configuration that every front-end shares, and the
//! checks on it.
//!
//! A front-end reads one configuration file, of which some tables are its
//! own, such as how to reach its database, and the rest are these: how rows
//! become resources ([`Mapping`]), how reconciliations are run
//! ([`ReconcileConfig`]), how Morph is set up ([`MorphConfig`] and
//! [`ProviderConfig`]), which engine reconciles ([`EngineConfig`]), and
//! what is logged ([`LoggingConfig`]). Each is a struct here and each key a
//! field, under the same names, and a key that is not known is refused.
//!
//! The checks that need no database are here too: [`validate_mappings`],
//! [`validate_engine`], [`ReconcileConfig::validate`] and
//! [`Backoff::validate`]. A front-end calls them from its own check, after
//! its own.
//!
//! Durations are written as `500ms`, `30s`, `5m` or `1h`.

use std::collections::BTreeMap;
use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hasher};
use std::path::PathBuf;
use std::time::Duration;

use serde::Deserialize;

use crate::error::{Error, Result};

/// How the rows of one table become resources: one `[[mappings]]` block.
///
/// Each row describes one resource, `kind.name`. The name is read from
/// `id_column`. The kind is either fixed for the table by `resource_type`
/// or read from `type_column`, and exactly one of the two has to be given.
/// Every other column is an attribute of the resource, unless the three
/// column settings say otherwise, and unless it is null.
///
/// Changing how a table names its resources changes which resources there
/// are. The old ids no longer have rows, and the next full reconciliation
/// hands each of them to the engine with nothing desired.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Mapping {
    /// The schema of the table. `public` by default.
    #[serde(default = "public")]
    pub schema: String,
    /// The table. Required. A table can be mapped only once.
    pub table: String,
    /// The column that names the resource. `id` by default.
    ///
    /// Its value, as text, has to be a valid Morph name: letters, digits,
    /// `_` and `-`. It is never an attribute. A delete names its resource
    /// only if the source sends the column with a delete, which for
    /// PostgreSQL means the column is in the table's replica identity, as a
    /// primary key is unless the table says otherwise.
    #[serde(default = "id")]
    pub id_column: String,
    /// The column that says what kind of resource a row describes. Either
    /// this or `resource_type`.
    ///
    /// It is never an attribute. A delete may carry only the key, so for
    /// deletes to name their kind the source has to send the whole row,
    /// which for PostgreSQL means `REPLICA IDENTITY FULL`. A delete that
    /// cannot name its kind is counted as a mapping error and left to the
    /// periodic loop.
    #[serde(default)]
    pub type_column: Option<String>,
    /// The kind of resource that every row of the table describes. Either
    /// this or `type_column`.
    #[serde(default)]
    pub resource_type: Option<String>,
    /// Columns that are not attributes of the resource. None by default.
    ///
    /// The id column, the type column and the attributes column never are
    /// attributes, listed or not.
    #[serde(default)]
    pub exclude_columns: Vec<String>,
    /// The only columns that are attributes, if given. Every column by
    /// default.
    #[serde(default)]
    pub attribute_columns: Option<Vec<String>>,
    /// A JSON column whose object holds further attributes. None by
    /// default.
    ///
    /// The object's entries are laid over the attributes that come from
    /// columns, and win where both have a name. A null adds nothing. A
    /// value that is neither an object nor null makes the row unusable, and
    /// reconciling its resource fails.
    #[serde(default)]
    pub attributes_column: Option<String>,
    /// How the engine is to treat the resources of this table: the
    /// `[mappings.lifecycle]` table.
    #[serde(default)]
    pub lifecycle: LifecycleConfig,
    /// The stack that holds every row of the table. Not set by default,
    /// and then each row is a record of its own.
    ///
    /// A stack is one record for all the rows grouped into it: with Morph,
    /// one state that is planned and applied as a whole; with Pulumi, one
    /// Pulumi stack whose program holds every row. The name may hold
    /// letters, digits, `_` and `-`. Several tables may name the same
    /// stack. A change to any row of a stack is a reconciliation of the
    /// stack, named `stack.<name>` in the log and the dirty store.
    #[serde(default)]
    pub stack: Option<String>,
    /// The column whose value names the row's stack. Not set by default.
    /// Either this or `stack`, not both.
    ///
    /// The value has the same rules as `stack`. It is never an attribute.
    /// A delete may carry only the key, so for deletes to name their stack
    /// the source has to send the whole row; a delete that cannot is left
    /// to the periodic loop.
    #[serde(default)]
    pub stack_column: Option<String>,
    /// The attribute of each of the table's resources that rows referring
    /// to them receive in place of the row id. `id` by default.
    ///
    /// Named as the engine knows it: `id`, `arn` or `self_link` for a
    /// provider written for Terraform, `id`, `arn` or `selfLink` for
    /// Pulumi. A reference that names an `attribute` of its own takes that
    /// instead.
    #[serde(default = "id")]
    pub exports: String,
    /// Whether a foreign key from a column of this table into another
    /// mapped table makes the column a reference. `true` by default.
    ///
    /// Only a key of one column counts, and only if it points at the other
    /// mapping's id column. The id, type, stack and attributes columns are
    /// never references. The keys are read from the source's catalogue at
    /// start.
    #[serde(default = "yes")]
    pub foreign_keys: bool,
    /// The column that holds the identifier by which the engine takes
    /// charge of an object that already exists, the first time it
    /// reconciles the row. Not set by default.
    ///
    /// With it, a row may describe something made outside the controller,
    /// by Terraform for one. Before the resource's first reconciliation,
    /// the engine imports the object the identifier names, so that it is
    /// brought in line rather than made again. A row whose column is null
    /// is created as usual. An identifier that names nothing fails the
    /// reconciliation, and nothing is created in its place. The column is
    /// never an attribute, and never a reference.
    #[serde(default)]
    pub import_column: Option<String>,
    /// Columns that are references without a constraint to say so, or
    /// that take another attribute than the referenced mapping's
    /// `exports`, or that give their value to another attribute: one
    /// `[[mappings.references]]` block each. None by default.
    ///
    /// A declared reference wins over a foreign key on the same column.
    #[serde(default)]
    pub references: Vec<ReferenceConfig>,
}

/// A column that holds the id of a row of another mapped table: one
/// `[[mappings.references]]` block.
///
/// When the row is reconciled, the value in the column is replaced by an
/// attribute of the resource the referenced row describes, and the row's
/// resource waits for that resource if it has not been reconciled yet.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ReferenceConfig {
    /// The column. Required, and never the mapping's id, type, stack or
    /// attributes column.
    pub column: String,
    /// The schema of the table the column points at. `public` by default.
    #[serde(default = "public")]
    pub schema: String,
    /// The table the column points at. Required, and mapped.
    pub table: String,
    /// The attribute of the referenced resource that replaces the
    /// column's value. The referenced mapping's `exports` by default.
    #[serde(default)]
    pub attribute: Option<String>,
    /// The attribute of the row's own resource that receives the value.
    /// The column's own name by default.
    ///
    /// With it, the column holds nothing but the reference, and is not an
    /// attribute itself: the value goes to the attribute named here, in
    /// place of whatever another column of that name holds. That lets one
    /// attribute be given as a plain value in some rows and taken from
    /// another row in others, a null in the reference column meaning the
    /// plain value stands. Several columns may set the same attribute,
    /// each from another table; a row fills at most one of them, and a
    /// row that fills two fails to reconcile.
    #[serde(default)]
    pub sets: Option<String>,
}

/// The kind under which a stack that groups rows is named: the resource
/// `stack.<name>` is the stack, not any one row. No mapping may use it.
pub const STACK_KIND: &str = "stack";

impl Mapping {
    /// The table as `schema.table`, unquoted, as filters and messages
    /// write it.
    pub fn qualified_table(&self) -> String {
        format!("{}.{}", self.schema, self.table)
    }

    /// Whether the rows of the table are grouped into stacks, by `stack` or
    /// by `stack_column`.
    pub fn grouped(&self) -> bool {
        self.stack.is_some() || self.stack_column.is_some()
    }

    /// Whether `column` is one of those that name the resource rather
    /// than describe it: the id, type, stack or attributes column.
    ///
    /// Such a column is never an attribute, and never a reference.
    pub fn names_resource_with(&self, column: &str) -> bool {
        column == self.id_column
            || self.type_column.as_deref() == Some(column)
            || self.stack_column.as_deref() == Some(column)
            || self.attributes_column.as_deref() == Some(column)
    }
}

/// The engine's lifecycle settings for the resources of one table: the
/// `[mappings.lifecycle]` table.
///
/// The controller passes these on in the desired state and takes no part in
/// acting on them. All three are `false` by default.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LifecycleConfig {
    /// Makes the engine refuse any plan that would delete or replace the
    /// resource.
    ///
    /// A reconciliation that needs such a plan then fails, and is retried
    /// like any other failure.
    #[serde(default)]
    pub prevent_destroy: bool,
    /// Makes the engine stop tracking the resource, rather than delete it,
    /// when its row has gone.
    ///
    /// By then there is no row to read the setting from, so the engine goes
    /// by what its record says. A change here therefore reaches a resource
    /// the next time it is reconciled while it still has its row.
    #[serde(default)]
    pub retain: bool,
    /// Makes the engine create the replacement before it deletes the
    /// original, when the resource has to be replaced.
    #[serde(default)]
    pub create_before_destroy: bool,
}

/// Where the resources still to be reconciled are kept, and so when a
/// transaction's position becomes safe to confirm.
///
/// The source reports the safe position to its server every so often. Both
/// modes are at-least-once: after a restart a transaction may be sent
/// again, and its resources reconciled again.
#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CheckpointMode {
    /// In a store the front-end keeps, such as a table in the database:
    /// `durable`.
    ///
    /// A transaction's position is safe as soon as its resources are
    /// written to the store, and the store is read again at the next start.
    /// A slow or failing resource holds back nothing.
    #[default]
    Durable,
    /// In memory: `memory`.
    ///
    /// A transaction's position is safe once every resource of it, and of
    /// every transaction before it, has been reconciled or given up on. A
    /// slow resource holds back the confirmation of everything after it,
    /// and after a restart the source sends again whatever was not
    /// confirmed.
    Memory,
}

/// How reconciliations are run: the `[reconcile]` table.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReconcileConfig {
    /// The most reconciliations in flight at once. `16` by default, and at
    /// least one.
    ///
    /// No two of them are ever for the same resource. More workers mean
    /// more provider calls at once.
    #[serde(default = "sixteen")]
    pub workers: usize,
    /// How many resources the scheduler holds, waiting or being reconciled,
    /// before a new mark has to wait. `1000` by default, and at least one.
    ///
    /// When it is full the intake waits, and behind it the source, so the
    /// log is read no faster than resources are reconciled. A larger number
    /// lets more be read ahead and held in memory.
    #[serde(default = "a_thousand")]
    pub queue_size: usize,
    /// How long the periodic loop waits between full reconciliations. `5m`
    /// by default.
    ///
    /// Each one marks every resource that has a row or a record, changed or
    /// not. A shorter interval finds drift sooner and costs more provider
    /// calls.
    #[serde(default = "five_minutes", with = "humantime_serde")]
    pub full_interval: Duration,
    /// The longest one reconciliation may take before it is abandoned and
    /// counted as failed. `10m` by default.
    #[serde(default = "ten_minutes", with = "humantime_serde")]
    pub timeout: Duration,
    /// How a failed reconciliation is tried again: the `[reconcile.retry]`
    /// table.
    #[serde(default)]
    pub retry: RetryConfig,
}

impl Default for ReconcileConfig {
    fn default() -> Self {
        Self {
            workers: sixteen(),
            queue_size: a_thousand(),
            full_interval: five_minutes(),
            timeout: ten_minutes(),
            retry: RetryConfig::default(),
        }
    }
}

impl ReconcileConfig {
    /// Refuses what cannot work.
    ///
    /// # Errors
    ///
    /// [`Error::Config`], naming the setting, if `workers`, `queue_size` or
    /// `retry.max_attempts` is nought, or if `retry` fails
    /// [`Backoff::validate`].
    pub fn validate(&self) -> Result<()> {
        if self.workers == 0 {
            return Err(Error::Config(
                "reconcile.workers must be at least one".into(),
            ));
        }
        if self.queue_size == 0 {
            return Err(Error::Config(
                "reconcile.queue_size must be at least one".into(),
            ));
        }
        if self.retry.max_attempts == 0 {
            return Err(Error::Config(
                "reconcile.retry.max_attempts must be at least one".into(),
            ));
        }
        self.retry.backoff.validate("reconcile.retry")
    }
}

/// How a failed reconciliation is tried again: the `[reconcile.retry]`
/// table.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetryConfig {
    /// How many attempts in a row may fail before the resource is reported
    /// unhealthy. `5` by default, and at least one.
    ///
    /// An unhealthy resource is not tried again until it is marked again,
    /// by a change to its row or by the next full reconciliation. With one,
    /// nothing is tried twice.
    #[serde(default = "five")]
    pub max_attempts: u32,
    /// How the waits between attempts grow. Its settings are written in the
    /// same table, beside `max_attempts`.
    #[serde(default, flatten)]
    pub backoff: Backoff,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: five(),
            backoff: Backoff::default(),
        }
    }
}

/// A wait that grows with each failure in a row.
///
/// Used by `[reconcile.retry]`, and by a source for its reconnections.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Backoff {
    /// The wait after the first failure. `1s` by default, and more than
    /// nothing.
    #[serde(default = "one_second", with = "humantime_serde")]
    pub initial: Duration,
    /// The longest wait, before it is varied at random. `1m` by default.
    #[serde(default = "one_minute", with = "humantime_serde")]
    pub max: Duration,
    /// How many times longer each wait is than the last. `2.0` by default,
    /// and at least one, with which the wait does not grow.
    #[serde(default = "two")]
    pub multiplier: f64,
}

impl Default for Backoff {
    fn default() -> Self {
        Self {
            initial: one_second(),
            max: one_minute(),
            multiplier: two(),
        }
    }
}

impl Backoff {
    /// The wait after the failure numbered `attempt`, the first being one.
    ///
    /// The wait is `initial`, multiplied by `multiplier` once for every
    /// failure before this one, and capped at `max`. It is then made up to
    /// a fifth shorter or longer at random, so that many things that failed
    /// together do not all try again together. The result can therefore
    /// exceed `max` by up to a fifth. Nought is treated as one.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::time::Duration;
    ///
    /// use morph_controller_core::config::Backoff;
    ///
    /// let backoff = Backoff {
    ///     initial: Duration::from_secs(1),
    ///     max: Duration::from_secs(10),
    ///     multiplier: 2.0,
    /// };
    ///
    /// // Four seconds after the third failure, give or take a fifth.
    /// let wait = backoff.delay(3);
    /// assert!(wait >= Duration::from_millis(3200) && wait <= Duration::from_millis(4800));
    ///
    /// // The cap is applied before the wait is varied.
    /// let wait = backoff.delay(30);
    /// assert!(wait >= Duration::from_secs(8) && wait <= Duration::from_secs(12));
    /// ```
    pub fn delay(&self, attempt: u32) -> Duration {
        let grown =
            self.initial.as_secs_f64() * self.multiplier.powi(attempt.saturating_sub(1) as i32);
        let capped = grown.min(self.max.as_secs_f64());
        let jitter = 0.8 + 0.4 * random_unit();
        Duration::from_secs_f64((capped * jitter).max(0.0))
    }

    /// Refuses what cannot work, naming the table as `name`.
    ///
    /// # Errors
    ///
    /// [`Error::Config`] if `multiplier` is less than one or not finite,
    /// or if `initial` is nought.
    pub fn validate(&self, name: &str) -> Result<()> {
        if self.multiplier < 1.0 || !self.multiplier.is_finite() {
            return Err(Error::Config(format!(
                "{name}.multiplier must be at least one"
            )));
        }
        if self.initial.is_zero() {
            return Err(Error::Config(format!(
                "{name}.initial must be more than nothing"
            )));
        }
        Ok(())
    }
}

/// A number in `[0, 1)`, from the standard library's random hashing.
///
/// Every [`RandomState`] is made with keys of its own, so hashing the same
/// input gives an unrelated number at each call. That is random enough to
/// spread retries, and needs no further dependency. The top 53 bits of the
/// hash are used, which is as many as an `f64` holds exactly.
fn random_unit() -> f64 {
    let mut hasher = RandomState::new().build_hasher();
    hasher.write_u64(0);
    (hasher.finish() >> 11) as f64 / (1u64 << 53) as f64
}

/// How Morph is set up: the `[morph]` table.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MorphConfig {
    /// Where Morph keeps its record of each resource.
    /// [`StateConfig::Postgres`] by default.
    #[serde(default)]
    pub state: StateConfig,
    /// What the name of each resource's record starts with, in the
    /// `postgres` backend. `controller/` by default.
    ///
    /// It keeps several controllers apart in one table. Changing it hides
    /// the existing records from this controller, and without a record
    /// Morph takes a resource for one it has yet to create.
    #[serde(default = "controller_prefix")]
    pub state_prefix: String,
    /// The longest Morph waits for one provider call. Not set by default.
    ///
    /// When it is not set, Morph's own default applies, unless one of the
    /// providers is written for Terraform. Then there is no limit, because
    /// such providers wait for themselves.
    #[serde(default, with = "humantime_serde")]
    pub operation_timeout: Option<Duration>,
}

impl Default for MorphConfig {
    fn default() -> Self {
        Self {
            state: StateConfig::default(),
            state_prefix: controller_prefix(),
            operation_timeout: None,
        }
    }
}

/// Where Morph keeps its record of each resource: the `state` key of
/// `[morph]`, written as `{ backend = "postgres" }`.
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(tag = "backend", rename_all = "snake_case", deny_unknown_fields)]
pub enum StateConfig {
    /// In a PostgreSQL database, in Morph's own table `morph_state`, one
    /// row per resource: `postgres`.
    ///
    /// The front-end that has the connection makes these stores, as the
    /// PostgreSQL controller does in the same database as the rows. A
    /// front-end without one refuses the setting.
    #[default]
    Postgres,
    /// In a directory of JSON files, one per resource: `file`.
    File {
        /// The directory. The record of a resource is the file
        /// `kind/name.json` under it.
        directory: PathBuf,
    },
    /// In memory, which is forgotten at exit: `memory`. For trying things
    /// out.
    ///
    /// After a restart there are no records, and Morph takes every resource
    /// for one it has yet to create.
    Memory,
}

/// A provider that reaches resources: one `[[providers]]` block, of the
/// `kind` given.
#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ProviderConfig {
    /// Morph's in-memory cloud, which makes nothing real: `mock`. For
    /// trying things out.
    Mock,
    /// A provider written for Terraform or OpenTofu, fetched from the
    /// OpenTofu registry and run as a process of its own: `terraform`.
    Terraform {
        /// The provider's address in the registry, such as `hashicorp/aws`.
        source: String,
        /// The version to run. The newest that can be spoken to, if not
        /// given, which means asking the registry at every start.
        #[serde(default)]
        version: Option<String>,
        /// The directory in which fetched providers are kept.
        /// `.morph-providers` in the working directory by default.
        #[serde(default)]
        cache: Option<PathBuf>,
        /// The provider's own configuration: what would be written in its
        /// `provider` block. Empty by default.
        #[serde(default)]
        config: toml::Table,
    },
}

/// What reconciles a resource: the `[engine]` table, of the `kind` given.
///
/// The controller decides when and which resource to look at. The engine
/// decides what to do about it: it reads the actual state, works out the
/// difference from the row, and applies it. Every other part of the
/// controller is the same whichever engine is chosen.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum EngineConfig {
    /// Morph, as a library in this process, with the `[[providers]]` given
    /// and the records kept as `[morph]` says: `morph`. The default.
    #[default]
    Morph,
    /// Pulumi, as the `pulumi` command line, with one stack per resource
    /// in the Pulumi backend of your choice: `pulumi`.
    ///
    /// Each resource is a program of one resource, written for the Pulumi
    /// YAML runtime, in a stack named after the resource. An update of the
    /// stack is a reconciliation. The row's columns are the resource's
    /// properties, and the kind of the resource is turned into a Pulumi
    /// type by `types`.
    ///
    /// Credentials for the providers, and the token or passphrase for the
    /// backend, are read by Pulumi from the environment the controller
    /// runs in, as they would be at a terminal. Nothing secret goes in
    /// this file.
    Pulumi(Box<PulumiConfig>),
}

/// How Pulumi is run: the fields of `[engine]` when its `kind` is
/// `pulumi`.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PulumiConfig {
    /// The command to run. `pulumi` by default, found on the path.
    #[serde(default = "pulumi")]
    pub binary: PathBuf,
    /// The backend that holds the stacks, as `PULUMI_BACKEND_URL` would
    /// give it: `file:///var/lib/controller/pulumi`, `s3://bucket`,
    /// `gs://bucket`, `azblob://container` or `https://api.pulumi.com`.
    ///
    /// Not set by default, and then Pulumi uses whatever it is logged in
    /// to, as `pulumi login` left it.
    #[serde(default)]
    pub backend: Option<String>,
    /// The Pulumi project that every stack belongs to. `morph-controller`
    /// by default.
    ///
    /// Changing it hides the existing stacks from this controller, and
    /// without a stack Pulumi takes a resource for one it has yet to
    /// create.
    #[serde(default = "pulumi_project")]
    pub project: String,
    /// The directory in which the programs are written, one subdirectory
    /// per stack. `.pulumi-controller` in the working directory by
    /// default.
    ///
    /// Nothing here is a record. A program is written afresh before each
    /// update, and the directory can be removed between runs.
    #[serde(default = "pulumi_work_dir")]
    pub work_dir: PathBuf,
    /// Pulumi's own home, where it keeps plugins and credentials, as
    /// `PULUMI_HOME` would give it. Not set by default, which means
    /// `~/.pulumi`.
    #[serde(default)]
    pub home: Option<PathBuf>,
    /// A file holding the passphrase that encrypts the stacks' secrets, for
    /// backends other than Pulumi Cloud. Not set by default.
    ///
    /// When it is not set, Pulumi reads `PULUMI_CONFIG_PASSPHRASE` or
    /// `PULUMI_CONFIG_PASSPHRASE_FILE` from the environment, and refuses to
    /// make a stack without one of them.
    #[serde(default)]
    pub passphrase_file: Option<PathBuf>,
    /// What the name of each stack starts with. Empty by default.
    ///
    /// A stack is named `<prefix><kind>.<name>`. The prefix keeps several
    /// controllers apart in one project. It may hold letters, digits, `-`,
    /// `_` and `.`.
    #[serde(default)]
    pub stack_prefix: String,
    /// Whether Pulumi reads the actual state of the resource before each
    /// update, so that a change made behind the controller's back is
    /// found and undone. `true` by default.
    ///
    /// Without it an update trusts the stack's record, and a resource that
    /// was deleted by hand is not made again.
    #[serde(default = "yes")]
    pub refresh: bool,
    /// Whether column names are turned from `snake_case` into the
    /// `camelCase` that Pulumi's providers expect, so that a column
    /// `machine_type` becomes the property `machineType`. `true` by
    /// default.
    ///
    /// Only the names of columns are changed. Whatever is inside the
    /// `attributes_column` of a mapping is passed on as written.
    #[serde(default = "yes")]
    pub camel_case_columns: bool,
    /// Policy packs to enforce on every update, each a path to the pack's
    /// directory, as `pulumi up --policy-pack` takes them. None by
    /// default.
    #[serde(default)]
    pub policy_packs: Vec<PathBuf>,
    /// Further arguments for `pulumi up` and `pulumi destroy`, after the
    /// controller's own. None by default.
    #[serde(default)]
    pub extra_args: Vec<String>,
    /// The Pulumi type of each kind of resource: the `[engine.types]`
    /// table, mapping a kind as the mappings name it to a type token such
    /// as `gcp:compute:Instance`.
    ///
    /// A kind that is fixed by a mapping's `resource_type` has to be here.
    /// A kind read from a `type_column` is looked up when its row is
    /// reconciled, and a kind that is not here fails the reconciliation.
    #[serde(default)]
    pub types: BTreeMap<String, String>,
    /// The configuration of every stack: the `[engine.config]` table,
    /// written into each stack's configuration as it is, with keys such as
    /// `"gcp:project"`. Empty by default.
    ///
    /// Each stack gets the same. Secrets do not belong here, because the
    /// file is plain text. Providers read their credentials from the
    /// environment.
    #[serde(default)]
    pub config: toml::Table,
}

/// What is logged, and how: the `[logging]` table.
///
/// `level` and `format` are read by the program, which sets up the
/// logging. A program that embeds the controller sets up its own.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LoggingConfig {
    /// A `tracing` filter such as `info`, `debug` or
    /// `morph_controller_core=debug,info`. `info` by default.
    ///
    /// The variable `MPC_LOG` takes precedence over the file.
    #[serde(default = "info")]
    pub level: String,
    /// How log lines are written: `text` or `json`. `text` by default.
    #[serde(default)]
    pub format: LogFormat,
    /// How often the counters are written to the log. `30s` by default.
    #[serde(default = "thirty_seconds", with = "humantime_serde")]
    pub metrics_interval: Duration,
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: info(),
            format: LogFormat::default(),
            metrics_interval: thirty_seconds(),
        }
    }
}

/// How log lines are written.
#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum LogFormat {
    /// As a line of text, for people: `text`.
    #[default]
    Text,
    /// As a JSON object, for whatever collects the log: `json`.
    Json,
}

// The defaults. They are functions because that is the form in which serde
// takes a default for a field.
fn yes() -> bool {
    true
}
fn public() -> String {
    "public".to_owned()
}
fn id() -> String {
    "id".to_owned()
}
fn info() -> String {
    "info".to_owned()
}
fn pulumi() -> PathBuf {
    PathBuf::from("pulumi")
}
fn pulumi_project() -> String {
    "morph-controller".to_owned()
}
fn pulumi_work_dir() -> PathBuf {
    PathBuf::from(".pulumi-controller")
}
fn controller_prefix() -> String {
    "controller/".to_owned()
}
fn sixteen() -> usize {
    16
}
fn a_thousand() -> usize {
    1000
}
fn five() -> u32 {
    5
}
fn two() -> f64 {
    2.0
}
fn one_second() -> Duration {
    Duration::from_secs(1)
}
fn thirty_seconds() -> Duration {
    Duration::from_secs(30)
}
fn one_minute() -> Duration {
    Duration::from_secs(60)
}
fn five_minutes() -> Duration {
    Duration::from_secs(300)
}
fn ten_minutes() -> Duration {
    Duration::from_secs(600)
}

/// Whether `text` can name an attribute that is exported or taken by a
/// reference: letters, digits, `_` and `-`, which is how both engines name
/// the attributes of a resource, and never a `.`, which is what separates
/// the resource from the attribute in the name of an export.
pub fn is_attribute_name(text: &str) -> bool {
    !text.is_empty()
        && text
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
}

/// Whether `text` can name a stack that groups rows: letters, digits, `_`
/// and `-`, and never a `.`, which with Pulumi is what tells such a stack
/// from one that holds a single resource.
pub fn is_stack_name(text: &str) -> bool {
    !text.is_empty()
        && text
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
}

/// Refuses mappings that cannot work, as far as can be told without a
/// source.
///
/// Whether the names are identifiers the source accepts is the front-end's
/// check, because that depends on the source.
///
/// # Errors
///
/// [`Error::Config`], naming the mapping, for the first of these that is
/// found:
///
/// * There are no mappings.
/// * A mapping gives both `type_column` and `resource_type`, or neither,
///   or a `resource_type` that is not a valid kind.
/// * A mapping's `exports`, or a reference's `attribute`, is not an
///   attribute name as [`is_attribute_name`] has it.
/// * A reference's column is the mapping's id, type, stack or attributes
///   column, or is given twice; or its table is not mapped.
/// * A mapping gives both `stack` and `stack_column`, a `stack` that is
///   not a stack name as [`is_stack_name`] has it, or the reserved kind
///   [`STACK_KIND`] as its `resource_type`.
/// * A table is mapped twice.
pub fn validate_mappings(mappings: &[Mapping]) -> Result<()> {
    let bad = |what: String| Err(Error::Config(what));
    if mappings.is_empty() {
        return bad("no mappings: nothing would ever be reconciled".into());
    }
    for mapping in mappings {
        let table = mapping.qualified_table();
        match (&mapping.type_column, &mapping.resource_type) {
            (Some(_), Some(_)) => {
                return bad(format!(
                    "mapping {table}: give type_column or resource_type, not both"
                ));
            }
            (None, None) => {
                return bad(format!(
                    "mapping {table}: give type_column or resource_type"
                ));
            }
            (None, Some(kind)) if morph::ResourceKind::new(kind.clone()).is_err() => {
                return bad(format!(
                    "mapping {table}: resource_type {kind:?} is not a valid kind"
                ));
            }
            _ => {}
        }
        if !is_attribute_name(&mapping.exports) {
            return bad(format!(
                "mapping {table}: exports {:?} is not an attribute name",
                mapping.exports
            ));
        }
        if let Some(column) = &mapping.import_column {
            if mapping.names_resource_with(column) {
                return bad(format!(
                    "mapping {table}: column {column} names the resource, and cannot be the import column"
                ));
            }
            if mapping
                .references
                .iter()
                .any(|reference| &reference.column == column)
            {
                return bad(format!(
                    "mapping {table}: column {column} is a reference, and cannot be the import column"
                ));
            }
        }
        let mut columns = std::collections::BTreeSet::new();
        for reference in &mapping.references {
            let column = &reference.column;
            if mapping.names_resource_with(column) {
                return bad(format!(
                    "mapping {table}: column {column} names the resource, and cannot be a reference"
                ));
            }
            if !columns.insert(column) {
                return bad(format!(
                    "mapping {table}: column {column} is given as a reference twice"
                ));
            }
            if let Some(attribute) = &reference.attribute
                && !is_attribute_name(attribute)
            {
                return bad(format!(
                    "mapping {table}: reference column {column} takes {attribute:?}, which is not an attribute name"
                ));
            }
            if let Some(sets) = &reference.sets {
                if !is_attribute_name(sets) {
                    return bad(format!(
                        "mapping {table}: reference column {column} sets {sets:?}, which is not an attribute name"
                    ));
                }
                if mapping.names_resource_with(sets)
                    || mapping.import_column.as_deref() == Some(sets)
                {
                    return bad(format!(
                        "mapping {table}: reference column {column} sets {sets}, which is not an attribute"
                    ));
                }
            }

            if !mappings
                .iter()
                .any(|other| other.schema == reference.schema && other.table == reference.table)
            {
                return bad(format!(
                    "mapping {table}: reference column {column} points at {}.{}, which is not mapped",
                    reference.schema, reference.table
                ));
            }
        }
        if mapping.stack.is_some() && mapping.stack_column.is_some() {
            return bad(format!(
                "mapping {table}: give stack or stack_column, not both"
            ));
        }
        if let Some(stack) = &mapping.stack
            && !is_stack_name(stack)
        {
            return bad(format!(
                "mapping {table}: stack {stack:?} may hold only letters, digits, '_' and '-'"
            ));
        }
        if mapping.resource_type.as_deref() == Some(STACK_KIND) {
            return bad(format!(
                "mapping {table}: the kind {STACK_KIND:?} is reserved for stacks that group rows"
            ));
        }
    }
    let mut seen = std::collections::BTreeSet::new();
    for mapping in mappings {
        if !seen.insert(mapping.qualified_table()) {
            return bad(format!(
                "mapping {} is given twice",
                mapping.qualified_table()
            ));
        }
    }
    Ok(())
}

/// Refuses an engine that cannot work with these mappings and providers.
///
/// # Errors
///
/// [`Error::Config`], naming the setting, for the first of these that is
/// found:
///
/// * With the Morph engine, there are no providers.
/// * With the Pulumi engine, there are providers; `engine.types` names the
///   reserved kind [`STACK_KIND`]; the project or the stack prefix holds a
///   character Pulumi does not allow, or the project is empty; the work
///   directory is empty; a mapping's `resource_type` has no entry in
///   `engine.types`; or an entry there is not a valid kind or not a type
///   token.
pub fn validate_engine(
    engine: &EngineConfig,
    mappings: &[Mapping],
    providers: &[ProviderConfig],
) -> Result<()> {
    let bad = |what: String| Err(Error::Config(what));
    let pulumi = match engine {
        EngineConfig::Morph => {
            if providers.is_empty() {
                return bad("no providers: nothing could reach a resource".into());
            }
            return Ok(());
        }
        EngineConfig::Pulumi(pulumi) => pulumi,
    };
    if pulumi.types.contains_key(STACK_KIND) {
        return bad(format!(
            "engine.types: the kind {STACK_KIND:?} is reserved for stacks that group rows"
        ));
    }
    if !providers.is_empty() {
        return bad(
            "providers are the Morph engine's; with the Pulumi engine, name the Pulumi types in engine.types instead"
                .into(),
        );
    }
    for (name, value) in [
        ("engine.project", &pulumi.project),
        ("engine.stack_prefix", &pulumi.stack_prefix),
    ] {
        let allowed = value
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'));
        if !allowed || (name == "engine.project" && value.is_empty()) {
            return bad(format!(
                "{name} {value:?} may hold only letters, digits, '-', '_' and '.'"
            ));
        }
    }
    if pulumi.work_dir.as_os_str().is_empty() {
        return bad("engine.work_dir is empty".into());
    }
    for mapping in mappings {
        if let Some(kind) = &mapping.resource_type
            && !pulumi.types.contains_key(kind)
        {
            return bad(format!(
                "mapping {}: engine.types has no Pulumi type for {kind:?}",
                mapping.qualified_table()
            ));
        }
    }
    for (kind, token) in &pulumi.types {
        if morph::ResourceKind::new(kind.clone()).is_err() {
            return bad(format!("engine.types: {kind:?} is not a valid kind"));
        }
        if token.is_empty() || token.split(':').count() < 2 {
            return bad(format!(
                "engine.types: {token:?} is not a Pulumi type token such as gcp:compute:Instance"
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mapping(text: &str) -> Mapping {
        toml::from_str(text).unwrap()
    }

    fn refused(mappings: &[Mapping]) -> String {
        validate_mappings(mappings).unwrap_err().to_string()
    }

    #[test]
    fn mappings_are_checked_for_what_needs_no_source() {
        assert!(refused(&[]).contains("no mappings"));
        let plain = mapping("table = \"t\"\nresource_type = \"vm\"");
        assert!(validate_mappings(std::slice::from_ref(&plain)).is_ok());
        assert!(
            refused(&[mapping(
                "table = \"t\"\nresource_type = \"vm\"\ntype_column = \"k\""
            )])
            .contains("not both")
        );
        assert!(refused(&[mapping("table = \"t\"")]).contains("give type_column or resource_type"));
        assert!(
            refused(&[mapping("table = \"t\"\nresource_type = \"a b\"")]).contains("valid kind")
        );
        assert!(
            refused(&[mapping(
                "table = \"t\"\nresource_type = \"vm\"\nexports = \"a.b\""
            )])
            .contains("attribute name")
        );
        assert!(refused(&[plain.clone(), plain.clone()]).contains("given twice"));
        assert!(
            refused(&[mapping(
                "table = \"t\"\nresource_type = \"vm\"\nstack = \"fleet\"\nstack_column = \"env\""
            )])
            .contains("not both")
        );
        assert!(
            refused(&[mapping(
                "table = \"t\"\nresource_type = \"vm\"\nstack = \"a.b\""
            )])
            .contains("may hold only")
        );
        assert!(
            refused(&[mapping("table = \"t\"\nresource_type = \"stack\"")]).contains("reserved")
        );
    }

    #[test]
    fn the_import_column_and_what_a_reference_sets_are_checked() {
        let networks = mapping("table = \"networks\"\nresource_type = \"network\"");
        let servers = |extra: &str| {
            mapping(&format!(
                "table = \"servers\"\nresource_type = \"server\"\n{extra}"
            ))
        };
        let good = servers(
            "import_column = \"object\"\n[[references]]\ncolumn = \"network_ref\"\ntable = \"networks\"\nsets = \"network_id\"",
        );
        assert!(validate_mappings(&[networks.clone(), good.clone()]).is_ok());
        assert_eq!(good.import_column.as_deref(), Some("object"));
        assert_eq!(good.references[0].sets.as_deref(), Some("network_id"));
        assert!(
            refused(&[networks.clone(), servers("import_column = \"id\"")])
                .contains("cannot be the import column")
        );
        assert!(
            refused(&[
                networks.clone(),
                servers(
                    "import_column = \"network_id\"\n[[references]]\ncolumn = \"network_id\"\ntable = \"networks\""
                )
            ])
            .contains("is a reference, and cannot be the import column")
        );
        assert!(
            refused(&[
                networks.clone(),
                servers(
                    "[[references]]\ncolumn = \"network_ref\"\ntable = \"networks\"\nsets = \"a b\""
                )
            ])
            .contains("not an attribute name")
        );
        assert!(
            refused(&[
                networks.clone(),
                servers(
                    "[[references]]\ncolumn = \"network_ref\"\ntable = \"networks\"\nsets = \"id\""
                )
            ])
            .contains("which is not an attribute")
        );
        assert!(
            refused(&[
                networks.clone(),
                servers(
                    "import_column = \"object\"\n[[references]]\ncolumn = \"network_ref\"\ntable = \"networks\"\nsets = \"object\""
                )
            ])
            .contains("which is not an attribute")
        );
        let two = servers(
            "[[references]]\ncolumn = \"a\"\ntable = \"networks\"\nsets = \"network_id\"\n[[references]]\ncolumn = \"b\"\ntable = \"networks\"\nsets = \"network_id\"",
        );
        assert!(
            validate_mappings(&[networks, two]).is_ok(),
            "two columns may set one attribute, each from its own table or the same"
        );
    }

    #[test]
    fn references_are_checked_against_the_mappings() {
        let topics = mapping("table = \"topics\"\nresource_type = \"topic\"");
        let good = mapping(
            "table = \"subs\"\nresource_type = \"sub\"\n[[references]]\ncolumn = \"topic\"\ntable = \"topics\"",
        );
        assert!(validate_mappings(&[topics.clone(), good]).is_ok());
        let naming = mapping(
            "table = \"subs\"\nresource_type = \"sub\"\n[[references]]\ncolumn = \"id\"\ntable = \"topics\"",
        );
        assert!(refused(&[topics.clone(), naming]).contains("names the resource"));
        let twice = mapping(
            "table = \"subs\"\nresource_type = \"sub\"\n[[references]]\ncolumn = \"topic\"\ntable = \"topics\"\n[[references]]\ncolumn = \"topic\"\ntable = \"topics\"",
        );
        assert!(refused(&[topics.clone(), twice]).contains("twice"));
        let odd = mapping(
            "table = \"subs\"\nresource_type = \"sub\"\n[[references]]\ncolumn = \"topic\"\ntable = \"topics\"\nattribute = \"a.b\"",
        );
        assert!(refused(&[topics.clone(), odd]).contains("attribute name"));
        let unmapped = mapping(
            "table = \"subs\"\nresource_type = \"sub\"\n[[references]]\ncolumn = \"topic\"\ntable = \"nowhere\"",
        );
        assert!(refused(&[topics, unmapped]).contains("not mapped"));
    }

    #[test]
    fn engines_are_checked_against_the_mappings_and_providers() {
        let vms = mapping("table = \"vms\"\nresource_type = \"vm\"");
        let mock = ProviderConfig::Mock;
        assert!(
            validate_engine(
                &EngineConfig::Morph,
                std::slice::from_ref(&vms),
                std::slice::from_ref(&mock)
            )
            .is_ok()
        );
        assert!(
            validate_engine(&EngineConfig::Morph, std::slice::from_ref(&vms), &[])
                .unwrap_err()
                .to_string()
                .contains("no providers")
        );
        // Scalar keys first, then more entries of the types table.
        let pulumi = |scalars: &str, types: &str| -> EngineConfig {
            let settings: PulumiConfig = toml::from_str(&format!(
                "{scalars}\n[types]\nvm = \"gcp:compute:Instance\"\n{types}"
            ))
            .unwrap();
            EngineConfig::Pulumi(Box::new(settings))
        };
        assert!(validate_engine(&pulumi("", ""), std::slice::from_ref(&vms), &[]).is_ok());
        let says = |engine: &EngineConfig, mappings: &[Mapping], providers: &[ProviderConfig]| {
            validate_engine(engine, mappings, providers)
                .unwrap_err()
                .to_string()
        };
        assert!(
            says(&pulumi("", ""), std::slice::from_ref(&vms), &[mock])
                .contains("providers are the Morph engine's")
        );
        assert!(
            says(
                &pulumi("", "stack = \"x:y\""),
                std::slice::from_ref(&vms),
                &[]
            )
            .contains("reserved")
        );
        assert!(
            says(
                &pulumi("project = \"a b\"", ""),
                std::slice::from_ref(&vms),
                &[]
            )
            .contains("engine.project")
        );
        assert!(
            says(
                &pulumi("project = \"\"", ""),
                std::slice::from_ref(&vms),
                &[]
            )
            .contains("engine.project")
        );
        assert!(
            says(
                &pulumi("stack_prefix = \"a/b\"", ""),
                std::slice::from_ref(&vms),
                &[]
            )
            .contains("stack_prefix")
        );
        assert!(
            says(
                &pulumi("work_dir = \"\"", ""),
                std::slice::from_ref(&vms),
                &[]
            )
            .contains("work_dir")
        );
        let disks = mapping("table = \"disks\"\nresource_type = \"disk\"");
        assert!(says(&pulumi("", ""), &[vms.clone(), disks], &[]).contains("no Pulumi type"));
        assert!(
            says(
                &pulumi("", "\"a b\" = \"x:y\""),
                std::slice::from_ref(&vms),
                &[]
            )
            .contains("valid kind")
        );
        assert!(says(&pulumi("", "disk = \"nocolon\""), &[vms], &[]).contains("type token"));
    }

    #[test]
    fn reconcile_settings_and_backoffs_are_checked() {
        assert!(ReconcileConfig::default().validate().is_ok());
        let says = |text: &str| {
            let config: ReconcileConfig = toml::from_str(text).unwrap();
            config.validate().unwrap_err().to_string()
        };
        assert!(says("workers = 0").contains("reconcile.workers"));
        assert!(says("queue_size = 0").contains("reconcile.queue_size"));
        assert!(says("[retry]\nmax_attempts = 0").contains("max_attempts"));
        assert!(says("[retry]\nmultiplier = 0.5").contains("reconcile.retry.multiplier"));
        assert!(says("[retry]\ninitial = \"0s\"").contains("reconcile.retry.initial"));
        let backoff: Backoff = toml::from_str("multiplier = 1.0").unwrap();
        assert!(backoff.validate("x").is_ok());
        let infinite = Backoff {
            multiplier: f64::INFINITY,
            ..Backoff::default()
        };
        assert!(
            infinite
                .validate("x")
                .unwrap_err()
                .to_string()
                .contains("x.multiplier")
        );
    }

    #[test]
    fn backoff_grows_and_is_capped() {
        let backoff = Backoff {
            initial: Duration::from_secs(1),
            max: Duration::from_secs(10),
            multiplier: 2.0,
        };
        assert!(backoff.delay(0) <= Duration::from_millis(1200));
        let third = backoff.delay(3);
        assert!(third >= Duration::from_millis(3200) && third <= Duration::from_millis(4800));
        let capped = backoff.delay(30);
        assert!(capped >= Duration::from_secs(8) && capped <= Duration::from_secs(12));
        let unit = random_unit();
        assert!((0.0..1.0).contains(&unit));
    }

    #[test]
    fn the_shared_defaults_are_sensible() {
        let reconcile = ReconcileConfig::default();
        assert_eq!(reconcile.workers, 16);
        assert_eq!(reconcile.queue_size, 1000);
        assert_eq!(reconcile.full_interval, Duration::from_secs(300));
        assert_eq!(reconcile.retry.max_attempts, 5);
        assert_eq!(MorphConfig::default().state, StateConfig::Postgres);
        assert_eq!(MorphConfig::default().state_prefix, "controller/");
        assert_eq!(LoggingConfig::default().level, "info");
        assert_eq!(LoggingConfig::default().format, LogFormat::Text);
        assert_eq!(CheckpointMode::default(), CheckpointMode::Durable);
        let pulumi: PulumiConfig = toml::from_str("").unwrap();
        assert_eq!(pulumi.project, "morph-controller");
        assert!(pulumi.refresh && pulumi.camel_case_columns);
        let mapping = mapping("table = \"t\"\nresource_type = \"vm\"");
        assert_eq!(mapping.qualified_table(), "public.t");
        assert_eq!(mapping.exports, "id");
        assert!(mapping.foreign_keys);
        assert!(!mapping.grouped());
        assert!(mapping.names_resource_with("id"));
        assert!(!mapping.names_resource_with("name"));
        assert!(
            is_attribute_name("self_link") && !is_attribute_name("a.b") && !is_attribute_name("")
        );
        assert!(is_stack_name("fleet-1") && !is_stack_name("a.b"));
    }
}
