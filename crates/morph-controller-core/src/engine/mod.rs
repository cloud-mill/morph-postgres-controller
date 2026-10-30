//! What reconciles a resource once the controller has decided to look at
//! it.
//!
//! The controller says *when* and *which*. An engine says *what*: given the
//! row that describes a resource, or the news that there is none, it reads
//! the actual state, works out the difference, applies it, and keeps a
//! record of its own. [`Engine`] is the choice the configuration makes.
//! [`MorphEngine`] runs Morph as a library in this process, with a record
//! per resource in one of Morph's stores. [`PulumiEngine`] runs the
//! `pulumi` command, with a stack per resource in a Pulumi backend.
//!
//! # Stacks
//!
//! A mapping may group its rows into a stack, and then the stack is the
//! unit of work for either engine: one record that holds every row of the
//! group, reconciled as a whole by [`Engine::reconcile_group`]. With Morph
//! the record is one state, planned and applied as one plan, in which a
//! row that links to another of the stack is a reference, so that Morph
//! orders the two and spreads a replacement from one to the other. With
//! Pulumi the record is one Pulumi stack whose program holds every row,
//! and a link is an expression Pulumi resolves.
//!
//! The rest of the controller sees only [`Engine::reconcile`], which the
//! workers call, [`Engine::managed`], which the periodic loop uses to find
//! resources whose rows have gone, and [`Engine::exported`], which gives
//! a row that refers to another resource what that resource has to offer.
//!
//! # Exports
//!
//! A row may refer to another row, as [`crate::references`] describes.
//! What the referring row receives is an attribute of the referenced
//! resource as the engine recorded it: Morph's record of the resource, in
//! its own state or in its stack's, or an output of the Pulumi stack that
//! holds it. The engine is told which
//! attributes of a resource are taken by others, in the row's `exports`,
//! and reports with every reconciliation what those were before and after,
//! so that a change reaches the resources that took the old value.
//!
//! # What an engine promises
//!
//! * A reconciliation reads the actual state before it acts, so that a
//!   change made behind the controller's back is undone.
//! * A reconciliation of a resource that is already as its row says
//!   changes nothing and says so. That is what makes looking twice
//!   harmless, and with it the controller's at-least-once delivery.
//! * A failure is an error with a reason. Nothing is tried again inside
//!   the engine; the scheduler decides when the next attempt is.

pub mod pulumi;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use morph::{
    Engine as Morph, ExecutorOptions, Import, ImportError, ObservedState, ProviderRegistry,
    ResourceId, ResourceRecord, ResourceStatus,
};

pub use self::pulumi::PulumiEngine;
use crate::config::STACK_KIND;
use crate::error::{Error, Result};
use crate::mapping::desired::{Row, desired_from_rows};
use crate::stores::StateStores;

/// What a reconciliation that succeeded came to.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Outcome {
    /// The engine's one-line summary of what it did.
    pub summary: String,
    /// Whether anything was changed, as opposed to finding the resource as
    /// it should be.
    pub changed: bool,
    /// What the reconciled resources export, before and after.
    pub exports: Exports,
}

/// The attributes that other rows take from the resources of a
/// reconciliation, as they were before it and as they are after it.
///
/// Each entry is named as [`export_name`] names it, `kind.name.attribute`,
/// so that the exports of every resource of a stack fit in one map.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Exports {
    /// The values before the reconciliation. Empty when the engine cannot
    /// tell, as for a resource that had no record.
    pub before: BTreeMap<String, serde_json::Value>,
    /// The values after it. Empty when nothing is exported, or nothing is
    /// left.
    pub after: BTreeMap<String, serde_json::Value>,
}

impl Exports {
    /// The exports whose values differ between before and after, each with
    /// its value after: `None` for one that is gone. Resources that took
    /// another value of any of these have to be reconciled again.
    pub fn changed(&self) -> BTreeMap<String, Option<serde_json::Value>> {
        let names: BTreeSet<&String> = self.before.keys().chain(self.after.keys()).collect();
        names
            .into_iter()
            .filter(|name| self.before.get(*name) != self.after.get(*name))
            .map(|name| (name.clone(), self.after.get(name).cloned()))
            .collect()
    }

    /// The resources whose exported values differ between before and
    /// after: a value added, removed or changed.
    pub fn changed_members(&self) -> BTreeSet<ResourceId> {
        self.changed()
            .keys()
            .filter_map(|name| export_member(name))
            .collect()
    }
}

/// The name under which an attribute of a resource is exported:
/// `kind.name.attribute`.
pub fn export_name(member: &ResourceId, attribute: &str) -> String {
    format!("{member}.{attribute}")
}

/// The resource and the attribute an export name is made of, or `None` if
/// the name is not of that shape.
pub fn export_parts(name: &str) -> Option<(ResourceId, String)> {
    let mut parts = name.splitn(3, '.');
    let kind = parts.next()?;
    let resource = parts.next()?;
    let attribute = parts.next()?;
    Some((ResourceId::new(kind, resource).ok()?, attribute.to_owned()))
}

/// The resource an export name belongs to: `kind.name` of
/// `kind.name.attribute`, or `None` if the name is not of that shape.
pub fn export_member(name: &str) -> Option<ResourceId> {
    export_parts(name).map(|(member, _)| member)
}

/// What a resource has to give a row that refers to it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Exported {
    /// The attribute's value.
    Value(serde_json::Value),
    /// The resource has no record, or has not been created: it has not
    /// been reconciled yet, and a row that needs it waits for it.
    NotYet,
    /// The resource has a record, and the record has no such attribute.
    /// Waiting would not bring it; the attribute is misnamed, or the
    /// record is from before it was exported and gains it at the
    /// resource's next reconciliation.
    Missing,
}

/// The engine the configuration chose.
pub enum Engine {
    /// Morph, in this process.
    Morph(MorphEngine),
    /// Pulumi, as a command.
    Pulumi(PulumiEngine),
}

impl std::fmt::Debug for Engine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Engine::Morph(engine) => f.debug_tuple("Morph").field(engine).finish(),
            Engine::Pulumi(engine) => f.debug_tuple("Pulumi").field(engine).finish(),
        }
    }
}

impl Engine {
    /// The engine's name, as the logs and the `check` command give it.
    pub fn name(&self) -> &'static str {
        match self {
            Engine::Morph(_) => "morph",
            Engine::Pulumi(_) => "pulumi",
        }
    }

    /// Brings `resource` in line with `row`, or with the absence of one.
    ///
    /// # Errors
    ///
    /// * [`Error::Mapping`] if the row cannot be made into what the engine
    ///   takes: a desired state for Morph, properties and a Pulumi type for
    ///   Pulumi.
    /// * [`Error::Checkpoint`] if Morph's store for the resource's record
    ///   cannot be connected.
    /// * [`Error::Reconcile`] if the engine fails or reports a failure,
    ///   with the engine's reasons as text.
    pub async fn reconcile(&self, resource: &ResourceId, row: Option<Row>) -> Result<Outcome> {
        match self {
            Engine::Morph(engine) => engine.reconcile(resource, row).await,
            Engine::Pulumi(engine) => engine.reconcile(resource, row).await,
        }
    }

    /// Brings the stack `group` in line with `rows`, the rows of every
    /// table grouped into it, or with the absence of any.
    ///
    /// # Errors
    ///
    /// What [`MorphEngine::reconcile_group`] or
    /// [`PulumiEngine::reconcile_group`] fails with.
    pub async fn reconcile_group(&self, group: &str, rows: Vec<Row>) -> Result<Outcome> {
        match self {
            Engine::Morph(engine) => engine.reconcile_group(group, rows).await,
            Engine::Pulumi(engine) => engine.reconcile_group(group, rows).await,
        }
    }

    /// What `member`, reconciled as part of `unit`, exports under
    /// `attribute`.
    ///
    /// `unit` is the member itself, or the stack that groups its row. Morph
    /// reads the member's record from the unit's state; Pulumi reads the
    /// unit's stack outputs.
    ///
    /// # Errors
    ///
    /// * [`Error::Checkpoint`] if Morph's store cannot be connected or
    ///   read.
    /// * [`Error::Engine`] if Pulumi cannot be run, or fails for any
    ///   reason but the stack's absence.
    pub async fn exported(
        &self,
        unit: &ResourceId,
        member: &ResourceId,
        attribute: &str,
    ) -> Result<Exported> {
        match self {
            Engine::Morph(engine) => engine.exported(unit, member, attribute).await,
            Engine::Pulumi(engine) => engine.exported(unit, member, attribute).await,
        }
    }

    /// Every resource the engine holds a record of, sorted.
    ///
    /// This is what finds a resource whose row has gone: it is still on
    /// record, and a reconciliation with no row removes it. A stack that
    /// groups rows is listed as `stack.<name>`.
    ///
    /// # Errors
    ///
    /// * [`Error::Connection`], [`Error::Query`] or [`Error::Checkpoint`]
    ///   if Morph's records cannot be listed, as
    ///   [`StateStores::managed`] describes.
    /// * [`Error::Engine`] if Pulumi cannot list the stacks of its
    ///   project.
    pub async fn managed(&self) -> Result<Vec<ResourceId>> {
        match self {
            Engine::Morph(engine) => engine.managed().await,
            Engine::Pulumi(engine) => engine.managed().await,
        }
    }
}

/// Morph as the engine: a library call per reconciliation.
///
/// Morph is handed the desired state of the one resource, or of the rows
/// of one stack, the store that holds the record of that and of nothing
/// else, and the providers. [`Morph::converge`] does the rest: it reads
/// the actual state of what the record tracks, plans the difference,
/// applies the plan, and saves the record. Because each resource, or
/// stack, has a record of its own, two reconciliations of different ones
/// never wait for each other.
///
/// Clones are not offered. One engine is shared behind an [`Arc`].
pub struct MorphEngine {
    stores: Arc<StateStores>,
    providers: Arc<ProviderRegistry>,
    executor: ExecutorOptions,
}

impl std::fmt::Debug for MorphEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MorphEngine")
            .field("stores", &self.stores)
            .finish_non_exhaustive()
    }
}

impl MorphEngine {
    /// An engine that keeps records in `stores`, reaches resources through
    /// `providers`, and runs Morph's operations as `executor` says.
    pub fn new(
        stores: StateStores,
        providers: ProviderRegistry,
        executor: ExecutorOptions,
    ) -> Self {
        Self {
            stores: Arc::new(stores),
            providers: Arc::new(providers),
            executor,
        }
    }

    /// Converges `resource` on the desired state its row makes, or on an
    /// empty desired state if there is no row.
    ///
    /// The row's `exports` are read from the record before and after, and
    /// reported. Without a row nothing is exported, and nothing is read.
    ///
    /// # Errors
    ///
    /// * [`Error::Mapping`] if the row cannot be made into a desired state,
    ///   for the reasons [`spec_of_row`](crate::mapping::desired::spec_of_row)
    ///   gives.
    /// * [`Error::Checkpoint`] if the store for the resource's record
    ///   cannot be connected.
    /// * [`Error::Reconcile`] if Morph returns an error, or if its report
    ///   is not complete, in which case the reason lists the steps that
    ///   failed, or gives Morph's summary if none is marked as failed.
    pub async fn reconcile(&self, resource: &ResourceId, row: Option<Row>) -> Result<Outcome> {
        self.converge(resource, row.into_iter().collect()).await
    }

    /// Converges the stack `group` on the desired state its rows make
    /// together, or on an empty desired state if there are none, in which
    /// case Morph removes whatever the stack's record still holds.
    ///
    /// The rows are one desired state and one plan. A row that links to
    /// another of the stack refers to it in the plan, so Morph makes the
    /// two in order, hands the value across, and replaces the one when the
    /// other is replaced in a way that demands it. The `exports` of every
    /// row are read from the stack's record before and after.
    ///
    /// # Errors
    ///
    /// * [`Error::Mapping`] if `group` is not a valid stack name, or a row
    ///   cannot be made into a specification. One such row fails the whole
    ///   stack, because a plan is made whole or not at all.
    /// * Otherwise as [`Self::reconcile`] fails.
    pub async fn reconcile_group(&self, group: &str, rows: Vec<Row>) -> Result<Outcome> {
        let stack = ResourceId::new(STACK_KIND, group)
            .map_err(|error| Error::Mapping(format!("the stack {group:?}: {error}")))?;
        self.converge(&stack, rows).await
    }

    /// Converges `unit`'s record on the desired state `rows` make: the one
    /// row of a resource, the rows of a stack, or none.
    ///
    /// A row with an import id whose resource the record does not track
    /// yet has its object imported first, as [`adopt`] does, so that the
    /// convergence brings the object in line rather than makes another.
    async fn converge(&self, unit: &ResourceId, rows: Vec<Row>) -> Result<Outcome> {
        let failed = |reason: String| Error::Reconcile {
            resource: unit.clone(),
            reason,
        };
        let exports: Vec<(ResourceId, BTreeSet<String>)> = rows
            .iter()
            .filter(|row| !row.exports.is_empty())
            .map(|row| (row.resource.clone(), row.exports.clone()))
            .collect();
        let desired = desired_from_rows(&rows)?;
        let store = self.stores.store_for(unit).await?;
        let before = if exports.is_empty() {
            BTreeMap::new()
        } else {
            exports_of_all(&store.load().await?, &exports)
        };
        let engine = Morph::new((*self.providers).clone(), Arc::clone(&store))
            .with_executor_options(self.executor);
        let adopting: Vec<&Row> = rows.iter().filter(|row| row.import_id.is_some()).collect();
        if !adopting.is_empty() {
            let state = store.load().await?;
            adopt(&engine, &state, &adopting).await.map_err(failed)?;
        }
        let convergence = engine
            .converge(&desired)
            .await
            .map_err(|error| failed(error.to_string()))?;
        let report = &convergence.report;
        if !report.is_complete() {
            let mut reasons: Vec<String> = report
                .failures()
                .map(|(step, error)| format!("{step}: {error}"))
                .collect();
            if reasons.is_empty() {
                reasons.push(report.summary());
            }
            return Err(failed(reasons.join("; ")));
        }
        let after = if exports.is_empty() {
            BTreeMap::new()
        } else {
            exports_of_all(&store.load().await?, &exports)
        };
        Ok(Outcome {
            summary: report.summary(),
            changed: convergence.plan.has_steps(),
            exports: Exports { before, after },
        })
    }

    /// What `member`'s record in `unit`'s store says under `attribute`.
    ///
    /// `unit` is the member itself, or the stack whose state holds its
    /// record. The record has to be `Ready` or `Stale`: a resource that is
    /// absent, tainted or missing has nothing to give yet. An attribute
    /// `id` that the record does not list is answered with the object's
    /// handle, which is what a provider calls the object.
    ///
    /// # Errors
    ///
    /// [`Error::Checkpoint`] if the store cannot be connected or read.
    pub async fn exported(
        &self,
        unit: &ResourceId,
        member: &ResourceId,
        attribute: &str,
    ) -> Result<Exported> {
        let store = self.stores.store_for(unit).await?;
        let state = store.load().await?;
        Ok(exported_from(&state, member, attribute))
    }

    /// Every resource with a record, as [`StateStores::managed`] lists
    /// them.
    ///
    /// # Errors
    ///
    /// Whatever [`StateStores::managed`] fails with.
    pub async fn managed(&self) -> Result<Vec<ResourceId>> {
        self.stores.managed().await
    }
}

/// Takes charge of the objects that `rows` name by their import ids and
/// that `state` does not track yet.
///
/// Morph's import reads each object through its provider and puts it on
/// record as the row's resource, with its real attributes. A resource
/// that the record already tracks is left as it is, so the rows may keep
/// their import ids for ever. Every other failure is one of the
/// reconciliation: the provider found nothing by the identifier, or the
/// object is on record as another resource. Nothing is created in either
/// case.
///
/// # Errors
///
/// The reason, in words, when the import cannot be run or an object
/// could not be taken charge of.
async fn adopt(
    engine: &Morph,
    state: &ObservedState,
    rows: &[&Row],
) -> std::result::Result<(), String> {
    let imports: Vec<Import> = rows
        .iter()
        .filter(|row| {
            !state
                .get(&row.resource)
                .is_some_and(ResourceRecord::tracks_anything)
        })
        .map(|row| {
            Import::new(
                row.resource.clone(),
                row.import_id.clone().expect("only rows with an import id"),
            )
        })
        .collect();
    if imports.is_empty() {
        return Ok(());
    }
    let report = engine
        .import(imports)
        .await
        .map_err(|error| error.to_string())?;
    for (resource, handle) in &report.imported {
        tracing::info!(%resource, %handle, "took charge of an existing object");
    }
    let reasons: Vec<String> = report
        .failures
        .iter()
        .filter(|(_, error)| !matches!(error, ImportError::AlreadyTracked))
        .map(|(resource, error)| format!("could not take charge of {resource}: {error}"))
        .collect();
    if reasons.is_empty() {
        Ok(())
    } else {
        Err(reasons.join("; "))
    }
}

/// What `member`'s record in `state` has to give under `attribute`, as
/// [`MorphEngine::exported`] describes.
fn exported_from(state: &ObservedState, member: &ResourceId, attribute: &str) -> Exported {
    let Some(record) = state.get(member) else {
        return Exported::NotYet;
    };
    let object = match &record.status {
        ResourceStatus::Ready { object } | ResourceStatus::Stale { object, .. } => object,
        _ => return Exported::NotYet,
    };
    match object.attributes.get(attribute) {
        Some(value) => match serde_json::to_value(value) {
            Ok(value) => Exported::Value(value),
            Err(_) => Exported::Missing,
        },
        None if attribute == "id" => {
            Exported::Value(serde_json::Value::String(object.handle.as_str().to_owned()))
        }
        None => Exported::Missing,
    }
}

/// The exports of `member` in `state`, by export name, for the
/// `attributes` it is to export. An attribute the record cannot give is
/// left out.
fn exports_of(
    state: &ObservedState,
    member: &ResourceId,
    attributes: &BTreeSet<String>,
) -> BTreeMap<String, serde_json::Value> {
    attributes
        .iter()
        .filter_map(|attribute| match exported_from(state, member, attribute) {
            Exported::Value(value) => Some((export_name(member, attribute), value)),
            _ => None,
        })
        .collect()
}

/// The exports of every member listed in `wanted`, in one map.
fn exports_of_all(
    state: &ObservedState,
    wanted: &[(ResourceId, BTreeSet<String>)],
) -> BTreeMap<String, serde_json::Value> {
    wanted
        .iter()
        .flat_map(|(member, attributes)| exports_of(state, member, attributes))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{Mapping, MorphConfig, StateConfig};
    use morph_mock::{Fault, MockCloud, Op};
    use serde_json::json;

    fn engine(cloud: &MockCloud) -> Engine {
        let registry = ProviderRegistry::new()
            .with(Arc::new(cloud.clone()))
            .unwrap();
        let stores = StateStores::new(&MorphConfig {
            state: StateConfig::Memory,
            ..MorphConfig::default()
        })
        .unwrap();
        Engine::Morph(MorphEngine::new(
            stores,
            registry,
            ExecutorOptions::default(),
        ))
    }

    fn row(name: &str, cidr: &str) -> Row {
        let mapping: Mapping =
            toml::from_str("table = \"t\"\nresource_type = \"network\"").unwrap();
        Row::new(
            format!("network.{name}").parse().unwrap(),
            mapping,
            json!({"id": name, "name": name, "cidr": cidr}),
        )
    }

    #[tokio::test]
    async fn morph_makes_changes_and_removes_from_rows_and_reports_what_fails() {
        let cloud = MockCloud::new();
        let engine = engine(&cloud);
        assert_eq!(engine.name(), "morph");
        assert!(format!("{engine:?}").contains("Morph"));
        let main: ResourceId = "network.main".parse().unwrap();

        assert_eq!(
            engine.exported(&main, &main, "id").await.unwrap(),
            Exported::NotYet,
            "no record yet"
        );
        let made = engine
            .reconcile(&main, Some(row("main", "10.0.0.0/16")))
            .await
            .unwrap();
        assert!(made.changed);
        assert_eq!(made.exports, Exports::default(), "nothing asked for");
        assert_eq!(cloud.objects().len(), 1);
        let same = engine
            .reconcile(&main, Some(row("main", "10.0.0.0/16")))
            .await
            .unwrap();
        assert!(!same.changed, "{}", same.summary);
        assert_eq!(engine.managed().await.unwrap(), std::slice::from_ref(&main));

        // What the record exports: the id, and nothing it does not have.
        let Exported::Value(cloud_id) = engine.exported(&main, &main, "id").await.unwrap() else {
            panic!("the id is exported");
        };
        assert_eq!(cloud_id, json!(cloud.objects()[0].handle.as_str()));
        assert_eq!(
            engine.exported(&main, &main, "name").await.unwrap(),
            Exported::Value(json!("main"))
        );
        assert_eq!(
            engine.exported(&main, &main, "nothing").await.unwrap(),
            Exported::Missing
        );
        assert_eq!(
            engine
                .exported(&main, &"network.other".parse().unwrap(), "id")
                .await
                .unwrap(),
            Exported::NotYet
        );

        // A reconciliation that is told what to export reports it before
        // and after, and a replacement changes the id.
        let mut exporting = row("main", "10.0.0.0/16");
        exporting.exports = BTreeSet::from(["id".to_owned(), "name".to_owned()]);
        let same = engine.reconcile(&main, Some(exporting)).await.unwrap();
        assert_eq!(same.exports.before, same.exports.after);
        assert_eq!(same.exports.after["network.main.id"], cloud_id);
        assert_eq!(same.exports.after["network.main.name"], json!("main"));
        assert!(same.exports.changed_members().is_empty());
        let mut replaced = row("main", "10.9.0.0/16");
        replaced.exports = BTreeSet::from(["id".to_owned()]);
        let replaced = engine.reconcile(&main, Some(replaced)).await.unwrap();
        assert!(replaced.changed);
        assert_ne!(replaced.exports.before, replaced.exports.after);
        assert_eq!(
            replaced.exports.changed_members(),
            BTreeSet::from([main.clone()])
        );

        // A step that fails is reported with Morph's reason.
        cloud.inject(Fault::on(Op::Delete, main.clone()).always().permanent());
        let error = engine.reconcile(&main, None).await.unwrap_err();
        match error {
            Error::Reconcile { resource, reason } => {
                assert_eq!(resource, main);
                assert!(reason.contains("delete"), "{reason}");
            }
            other => panic!("{other}"),
        }
        cloud.clear_faults();
        let gone = engine.reconcile(&main, None).await.unwrap();
        assert!(gone.changed);
        assert!(cloud.objects().is_empty());

        // A row that makes no desired state is a mapping error.
        let mut bad = row("main", "10.0.0.0/16");
        bad.json = json!("not a row");
        assert!(matches!(
            engine.reconcile(&main, Some(bad)).await.unwrap_err(),
            Error::Mapping(_)
        ));

        // An invalid stack name is refused before anything runs.
        let error = engine.reconcile_group("a.b", Vec::new()).await.unwrap_err();
        assert!(matches!(error, Error::Mapping(_)), "{error}");
    }

    #[tokio::test]
    async fn a_stack_is_one_plan_in_which_links_are_references() {
        use crate::mapping::desired::Link;
        let cloud = MockCloud::new();
        let engine = engine(&cloud);
        let stack: ResourceId = "stack.fleet".parse().unwrap();
        let mapping: Mapping =
            toml::from_str("table = \"t\"\ntype_column = \"kind\"\nstack = \"fleet\"").unwrap();
        let network = |cidr: &str| {
            let mut row = Row::new(
                "network.main".parse().unwrap(),
                mapping.clone(),
                json!({"id": "main", "kind": "network", "name": "main", "cidr": cidr}),
            );
            row.exports = BTreeSet::from(["id".to_owned()]);
            row
        };
        let server = || {
            let mut row = Row::new(
                "server.web".parse().unwrap(),
                mapping.clone(),
                json!({"id": "web", "kind": "server", "name": "web", "image": "linux-1", "network_id": "main"}),
            );
            row.links.push(Link {
                column: "network_id".into(),
                target: "network.main".parse().unwrap(),
                attribute: "id".into(),
            });
            row
        };

        // Both made in one plan, the network first, and the server given
        // the network's id rather than the row's.
        let outcome = engine
            .reconcile_group("fleet", vec![network("10.0.0.0/16"), server()])
            .await
            .unwrap();
        assert!(outcome.changed);
        let order: Vec<String> = cloud
            .mutations()
            .iter()
            .map(|(op, resource)| format!("{op:?} {resource}"))
            .collect();
        assert_eq!(order, ["Create network.main", "Create server.web"]);
        let network_id = cloud.objects_of(&"network.main".parse().unwrap())[0]
            .handle
            .as_str()
            .to_owned();
        let web = &cloud.objects_of(&"server.web".parse().unwrap())[0];
        assert_eq!(
            web.attributes.get("network_id"),
            Some(&morph::Value::from(network_id.as_str()))
        );
        assert_eq!(
            outcome.exports.after["network.main.id"],
            json!(network_id),
            "the stack exports what its rows are asked for"
        );
        assert!(outcome.exports.before.is_empty());
        assert_eq!(engine.managed().await.unwrap(), vec![stack.clone()]);

        // The stack's state answers for its members.
        assert_eq!(
            engine
                .exported(&stack, &"network.main".parse().unwrap(), "id")
                .await
                .unwrap(),
            Exported::Value(json!(network_id))
        );
        assert_eq!(
            engine
                .exported(&stack, &"server.web".parse().unwrap(), "name")
                .await
                .unwrap(),
            Exported::Value(json!("web"))
        );

        // Nothing to do is nothing to do.
        let same = engine
            .reconcile_group("fleet", vec![network("10.0.0.0/16"), server()])
            .await
            .unwrap();
        assert!(!same.changed, "{}", same.summary);

        // A replacement of the network spreads to the server in the same
        // plan, and the export changes.
        cloud.reset_observations();
        let replaced = engine
            .reconcile_group("fleet", vec![network("10.9.0.0/16"), server()])
            .await
            .unwrap();
        assert!(replaced.changed);
        let ops: Vec<String> = cloud
            .mutations()
            .iter()
            .map(|(op, resource)| format!("{op:?} {resource}"))
            .collect();
        assert!(
            ops.contains(&"Create network.main".to_owned())
                && ops.contains(&"Create server.web".to_owned()),
            "{ops:?}"
        );
        assert_ne!(
            replaced.exports.before["network.main.id"],
            replaced.exports.after["network.main.id"]
        );
        let new_network_id = cloud.objects_of(&"network.main".parse().unwrap())[0]
            .handle
            .as_str()
            .to_owned();
        assert_eq!(
            cloud.objects_of(&"server.web".parse().unwrap())[0]
                .attributes
                .get("network_id"),
            Some(&morph::Value::from(new_network_id.as_str()))
        );

        // One bad row fails the whole stack before Morph is asked.
        let mut bad = server();
        bad.json = json!("not a row");
        let error = engine
            .reconcile_group("fleet", vec![network("10.9.0.0/16"), bad])
            .await
            .unwrap_err();
        assert!(matches!(error, Error::Mapping(_)), "{error}");

        // No rows: everything the stack holds goes, in reverse.
        cloud.reset_observations();
        let gone = engine.reconcile_group("fleet", Vec::new()).await.unwrap();
        assert!(gone.changed);
        let order: Vec<String> = cloud
            .mutations()
            .iter()
            .map(|(op, resource)| format!("{op:?} {resource}"))
            .collect();
        assert_eq!(order, ["Delete server.web", "Delete network.main"]);
        assert!(cloud.objects().is_empty());
        assert_eq!(
            engine
                .exported(&stack, &"network.main".parse().unwrap(), "id")
                .await
                .unwrap(),
            Exported::NotYet
        );
    }

    #[test]
    fn export_names_hold_the_resource_and_the_attribute() {
        let topic: ResourceId = "topic.orders".parse().unwrap();
        assert_eq!(export_name(&topic, "id"), "topic.orders.id");
        assert_eq!(export_member("topic.orders.id"), Some(topic.clone()));
        assert_eq!(
            export_parts("topic.orders.self-link"),
            Some((topic.clone(), "self-link".to_owned()))
        );
        assert_eq!(export_member("topic.orders.self-link"), Some(topic));
        assert_eq!(export_member("topic.orders"), None, "no attribute");
        assert_eq!(export_member("topic"), None);
        assert_eq!(export_member("bad id.x.y"), None);

        let exports = Exports {
            before: BTreeMap::from([
                ("topic.orders.id".to_owned(), json!("a")),
                ("topic.gone.id".to_owned(), json!("g")),
                ("topic.same.id".to_owned(), json!("s")),
                ("odd".to_owned(), json!(1)),
            ]),
            after: BTreeMap::from([
                ("topic.orders.id".to_owned(), json!("b")),
                ("topic.new.id".to_owned(), json!("n")),
                ("topic.same.id".to_owned(), json!("s")),
            ]),
        };
        let changed: Vec<String> = exports
            .changed_members()
            .iter()
            .map(ToString::to_string)
            .collect();
        assert_eq!(changed, ["topic.gone", "topic.new", "topic.orders"]);
        let detailed = exports.changed();
        assert_eq!(detailed.len(), 4, "{detailed:?}");
        assert_eq!(detailed["topic.orders.id"], Some(json!("b")));
        assert_eq!(detailed["topic.gone.id"], None, "gone");
        assert_eq!(detailed["topic.new.id"], Some(json!("n")));
        assert_eq!(
            detailed["odd"], None,
            "every changed name, whatever its shape"
        );
        assert!(!detailed.contains_key("topic.same.id"));
    }

    #[test]
    fn a_record_gives_what_it_holds_and_an_id_falls_back_to_the_handle() {
        use morph::{ObjectHandle, ObjectState, ResourceRecord, Value};
        let web: ResourceId = "server.web".parse().unwrap();
        let mut state = ObservedState::new();
        assert_eq!(exported_from(&state, &web, "id"), Exported::NotYet);
        let object = ObjectState::new(
            ObjectHandle::new("srv-1"),
            [("name".to_owned(), Value::from("web"))]
                .into_iter()
                .collect(),
        );
        state.insert(ResourceRecord::ready(web.clone(), object.clone()));
        assert_eq!(
            exported_from(&state, &web, "id"),
            Exported::Value(json!("srv-1")),
            "the handle stands in for a missing id"
        );
        assert_eq!(
            exported_from(&state, &web, "name"),
            Exported::Value(json!("web"))
        );
        assert_eq!(exported_from(&state, &web, "size"), Exported::Missing);
        let mut record = ResourceRecord::ready(web.clone(), object.clone());
        record.status = ResourceStatus::Stale {
            object,
            reason: "an update failed".into(),
        };
        state.insert(record);
        assert_eq!(
            exported_from(&state, &web, "name"),
            Exported::Value(json!("web")),
            "a stale record still gives what it knew"
        );
        let mut record = ResourceRecord::ready(
            web.clone(),
            ObjectState::new(ObjectHandle::new("x"), Default::default()),
        );
        record.status = ResourceStatus::Absent;
        state.insert(record);
        assert_eq!(exported_from(&state, &web, "id"), Exported::NotYet);
        assert!(exports_of(&state, &web, &BTreeSet::from(["id".to_owned()])).is_empty());
    }
}
