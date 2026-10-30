//! Pulumi as the engine: one stack per resource, driven through the
//! `pulumi` command line.
//!
//! A resource is a Pulumi program of one resource, written for the YAML
//! runtime. The program is written as JSON, which YAML admits, so that no
//! YAML writer is needed. It lives in a directory of its own under the
//! work directory, and its stack is named after the resource. Reconciling
//! the resource is `pulumi up` on that stack, with the program written
//! afresh from the row first, and with a refresh of the actual state
//! before the update unless the configuration says otherwise. A resource
//! whose row has gone is `pulumi destroy --remove`, which deletes what the
//! stack holds and then the stack itself.
//!
//! A mapping may group its rows into one stack instead, by a fixed name or
//! by a column. Such a stack is a program of every row of the group, and
//! it is reconciled as a whole: a change to any row updates the stack, and
//! a row that has gone leaves the program, so that Pulumi deletes what it
//! stood for. The stack's name has no `.`, which is what tells it from a
//! stack of one resource, whose name is the resource id. Within the
//! program a row's resource is keyed `kind_name`, so that another row of
//! the stack can refer to it in an expression, which `kind.name` could
//! not.
//!
//! What a row's resource has to offer rows that refer to it is written as
//! a stack output named `kind.name.attribute`, and read back from the
//! stack when such a row is reconciled. A row that refers to a row of the
//! same stack is written with the expression `${kind_name.attribute}`
//! instead, and Pulumi orders the two itself.
//!
//! What Pulumi prints with `--json` is a stream of events, one object per
//! line. The engine reads the resource events to tell whether anything
//! changed, the summary event to tell whether the update succeeded, the
//! diagnostic events for the reasons of a failure, and the stack's own
//! event for its outputs before and after.
//!
//! The stacks are ordinary Pulumi stacks. Everything Pulumi offers works
//! on them as usual: the Pulumi Cloud console and its history, `pulumi
//! stack` at a terminal, policy packs, secrets, and any backend.
//!
//! # Concurrency
//!
//! Each reconciliation runs one `pulumi` process at a time, in its own
//! directory, and Pulumi locks each stack for itself. Reconciliations of
//! different resources run side by side, as the scheduler allows, and the
//! scheduler never runs two of the same resource at once.
//!
//! The process is killed if the reconciliation is dropped, as it is on a
//! timeout. Pulumi then leaves the stack as it was at the last step it
//! recorded, and the next update carries on from there.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::process::Stdio;

use morph::ResourceId;
use serde_json::{Map, Value, json};
use tokio::process::Command;

use super::{Exported, Exports, Outcome, export_name};
use crate::config::{LifecycleConfig, PulumiConfig, STACK_KIND};
use crate::error::{Error, Result};
use crate::mapping::desired::{Link, Row, camel_case, properties_from_row};

/// The type of the resource that stands for a stack itself in Pulumi's
/// events. It is not one of ours.
const STACK_TYPE: &str = "pulumi:pulumi:Stack";

/// The directory under the work directory that stands for the project
/// when nothing stack-specific is asked, as when the stacks are listed.
const PROJECT_DIR: &str = ".project";

/// How much of a reason is kept, in characters.
const REASON_LENGTH: usize = 400;

/// The Pulumi command line as an engine.
///
/// Clones are not offered. One engine is shared behind an [`Arc`].
///
/// [`Arc`]: std::sync::Arc
pub struct PulumiEngine {
    settings: PulumiConfig,
    passphrase: Option<String>,
}

impl std::fmt::Debug for PulumiEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PulumiEngine")
            .field("binary", &self.settings.binary)
            .field("backend", &self.settings.backend)
            .field("project", &self.settings.project)
            .field("work_dir", &self.settings.work_dir)
            .finish_non_exhaustive()
    }
}

impl PulumiEngine {
    /// An engine set up as `settings` says.
    ///
    /// The work directory and the project directory under it are made, as
    /// is the backend's directory if the backend is a `file://` URL with an
    /// absolute path, and the passphrase file is read, if one is named.
    /// Pulumi itself is not run until [`version`](Self::version) or a
    /// reconciliation.
    ///
    /// # Errors
    ///
    /// [`Error::Engine`] if a directory cannot be made or the passphrase
    /// file cannot be read.
    pub fn new(settings: &PulumiConfig) -> Result<Self> {
        let passphrase = match &settings.passphrase_file {
            Some(path) => {
                let text = std::fs::read_to_string(path).map_err(|error| {
                    Error::Engine(format!(
                        "the passphrase file {} cannot be read: {error}",
                        path.display()
                    ))
                })?;
                Some(text.trim_end_matches(['\r', '\n']).to_owned())
            }
            None => None,
        };
        let engine = Self {
            settings: settings.clone(),
            passphrase,
        };
        let project = engine.project_dir();
        write_json(&project.join("Pulumi.yaml"), &engine.empty_program()).map_err(Error::Engine)?;
        // Pulumi refuses a file backend whose directory is missing, rather
        // than make it. A URL that Pulumi expands itself, such as
        // `file://~`, is left to it.
        if let Some(path) = settings
            .backend
            .as_deref()
            .and_then(|url| url.strip_prefix("file://"))
            && path.starts_with('/')
        {
            std::fs::create_dir_all(path).map_err(|error| {
                Error::Engine(format!(
                    "the backend directory {path} cannot be made: {error}"
                ))
            })?;
        }
        Ok(engine)
    }

    /// What `pulumi version` prints, such as `v3.259.0`.
    ///
    /// # Errors
    ///
    /// [`Error::Engine`] if the command cannot be run or fails.
    pub async fn version(&self) -> Result<String> {
        let run = self.run(&self.project_dir(), &["version"]).await?;
        if !run.ok {
            return Err(Error::Engine(format!(
                "pulumi version failed: {}",
                tidy(&run.stderr)
            )));
        }
        Ok(run.stdout.trim().to_owned())
    }

    /// The backend the stacks are in, as the configuration gives it, or
    /// `whatever pulumi is logged in to` if it does not.
    pub fn backend(&self) -> String {
        self.settings
            .backend
            .clone()
            .unwrap_or_else(|| "whatever pulumi is logged in to".to_owned())
    }

    /// The project every stack belongs to.
    pub fn project(&self) -> &str {
        &self.settings.project
    }

    /// The stack that holds `resource`: the prefix followed by the
    /// resource's id, `kind.name`, or by the group's name alone when the
    /// resource is a stack that groups rows.
    pub fn stack_name(&self, resource: &ResourceId) -> String {
        if resource.kind().as_str() == STACK_KIND {
            format!("{}{}", self.settings.stack_prefix, resource.name())
        } else {
            format!("{}{resource}", self.settings.stack_prefix)
        }
    }

    /// The resource a stack holds, if the stack is one of this engine's:
    /// the inverse of [`stack_name`](Self::stack_name). A name with a `.`
    /// is a resource id; one without is a stack that groups rows.
    pub fn resource_of(&self, stack: &str) -> Option<ResourceId> {
        let rest = stack.strip_prefix(self.settings.stack_prefix.as_str())?;
        if rest.contains('.') {
            rest.parse().ok()
        } else {
            ResourceId::new(STACK_KIND, rest).ok()
        }
    }

    /// The program of `resource` alone: one resource of the Pulumi type
    /// `token`, with these properties, and with the options that the
    /// mapping's lifecycle settings ask for.
    ///
    /// `prevent_destroy` becomes `protect`, which makes Pulumi refuse to
    /// delete or replace the resource. `retain` becomes `retainOnDelete`,
    /// which makes Pulumi drop the resource from the stack without
    /// deleting it. `create_before_destroy` is Pulumi's own order of
    /// replacement; without it, `deleteBeforeReplace` is set, so that the
    /// two engines replace resources the same way.
    pub fn program(
        &self,
        resource: &ResourceId,
        token: &str,
        properties: Map<String, Value>,
        lifecycle: &LifecycleConfig,
    ) -> Value {
        self.program_of(
            &format!("{resource}, reconciled by the Morph controller"),
            vec![Entry {
                key: resource.name().to_owned(),
                resource: resource.clone(),
                token: token.to_owned(),
                properties,
                lifecycle: lifecycle.clone(),
                exports: BTreeSet::new(),
                links: Vec::new(),
                import_id: None,
            }],
        )
    }

    /// A program of these entries, each a resource under its key, with
    /// the options [`program`](Self::program) describes.
    ///
    /// Each entry's exports become outputs of the stack, named by
    /// [`export_name`] and reading `${key.attribute}`. Each of its links
    /// becomes a property reading `${key.attribute}` of the linked
    /// member, under the column's name as the other properties are named.
    fn program_of(&self, description: &str, entries: Vec<Entry>) -> Value {
        let mut resources = Map::new();
        let mut outputs = Map::new();
        for entry in entries {
            let mut properties = entry.properties;
            for link in &entry.links {
                let name = if self.settings.camel_case_columns {
                    camel_case(&link.column)
                } else {
                    link.column.clone()
                };
                properties.insert(
                    name,
                    Value::String(format!(
                        "${{{}.{}}}",
                        member_key(&link.target),
                        link.attribute
                    )),
                );
            }
            for attribute in &entry.exports {
                outputs.insert(
                    export_name(&entry.resource, attribute),
                    Value::String(format!("${{{}.{attribute}}}", entry.key)),
                );
            }
            resources.insert(
                entry.key,
                json!({
                    "type": entry.token,
                    "properties": properties,
                    "options": {
                        "protect": entry.lifecycle.prevent_destroy,
                        "deleteBeforeReplace": !entry.lifecycle.create_before_destroy,
                        "retainOnDelete": entry.lifecycle.retain,
                    },
                }),
            );
        }
        let mut program = json!({
            "name": self.settings.project,
            "runtime": "yaml",
            "description": description,
            "resources": resources,
        });
        if !outputs.is_empty() {
            program["outputs"] = Value::Object(outputs);
        }
        program
    }

    /// The entry a row makes in a program, under `key`.
    ///
    /// # Errors
    ///
    /// [`Error::Mapping`] if `engine.types` has no Pulumi type for the
    /// row's kind, or if the row cannot be made into properties.
    fn entry_of(&self, row: Row, key: String) -> Result<Entry> {
        let kind = row.resource.kind().as_str();
        let token =
            self.settings.types.get(kind).cloned().ok_or_else(|| {
                Error::Mapping(format!("engine.types has no Pulumi type for {kind}"))
            })?;
        let properties =
            properties_from_row(&row.mapping, row.json, self.settings.camel_case_columns)?;
        Ok(Entry {
            key,
            resource: row.resource,
            token,
            properties,
            lifecycle: row.mapping.lifecycle,
            exports: row.exports,
            links: row.links,
            import_id: row.import_id,
        })
    }

    /// Brings `resource` in line with `row`, or removes its stack if there
    /// is no row.
    ///
    /// With a row: the program and the stack's configuration are written,
    /// the stack is selected or made, and `pulumi up` is run on it. A row
    /// with an import id, when the stack is being made, has the id set as
    /// the resource's `import` option, so that Pulumi takes charge of the
    /// object that exists rather than makes another; once the stack
    /// exists the option is not set again, which is what Pulumi asks.
    /// Without a row: if the stack exists, which is asked of the backend
    /// for that one stack rather than by listing them all, `pulumi
    /// destroy --remove` is run on it and its directory is removed; if it
    /// does not, nothing is done.
    ///
    /// # Errors
    ///
    /// * [`Error::Mapping`] if the row cannot be made into properties, or
    ///   if `engine.types` has no Pulumi type for the resource's kind.
    /// * [`Error::Reconcile`] if the files cannot be written, if `pulumi`
    ///   cannot be run, or if it fails, in which case the reason carries
    ///   the errors it reported, policy violations among them.
    pub async fn reconcile(&self, resource: &ResourceId, row: Option<Row>) -> Result<Outcome> {
        let stack = self.stack_name(resource);
        match row {
            Some(row) => {
                let entry = self.entry_of(row, resource.name().to_owned())?;
                let imports = imports_of(std::slice::from_ref(&entry));
                let program = self.program_of(
                    &format!("{resource}, reconciled by the Morph controller"),
                    vec![entry],
                );
                self.update_stack(resource, &stack, program, imports).await
            }
            None => self.remove_stack(resource, &stack).await,
        }
    }

    /// Brings the stack `group` in line with `rows`, every row of the
    /// tables grouped into it, or removes the stack if there are none.
    ///
    /// The rows become one program, each under the key [`member_key`]
    /// makes of its resource, and `pulumi up` is run on the stack as
    /// [`Self::reconcile`] runs it for one resource. A row that is no
    /// longer among them is no longer in the program, and Pulumi deletes
    /// what it stood for. One row that cannot be made into an entry fails
    /// the whole stack, because a program is written whole or not at all.
    ///
    /// # Errors
    ///
    /// * [`Error::Mapping`] if `group` is not a valid stack name, if two
    ///   rows make the same key, or if a row cannot be made into an entry,
    ///   as [`Self::reconcile`] says.
    /// * [`Error::Reconcile`] as [`Self::reconcile`] fails.
    pub async fn reconcile_group(&self, group: &str, rows: Vec<Row>) -> Result<Outcome> {
        let resource = ResourceId::new(STACK_KIND, group)
            .map_err(|error| Error::Mapping(format!("the stack {group:?}: {error}")))?;
        let stack = self.stack_name(&resource);
        if rows.is_empty() {
            return self.remove_stack(&resource, &stack).await;
        }
        let mut entries = Vec::with_capacity(rows.len());
        let mut keys: BTreeMap<String, ResourceId> = BTreeMap::new();
        for row in rows {
            let key = member_key(&row.resource);
            if let Some(other) = keys.insert(key.clone(), row.resource.clone()) {
                return Err(Error::Mapping(format!(
                    "the rows of {other} and {} make the same key {key:?} in the program of the stack {group}",
                    row.resource
                )));
            }
            entries.push(self.entry_of(row, key)?);
        }
        let imports = imports_of(&entries);
        let program = self.program_of(
            &format!(
                "{} rows of the stack {group}, reconciled by the Morph controller",
                entries.len()
            ),
            entries,
        );
        self.update_stack(&resource, &stack, program, imports).await
    }

    /// Writes `program` as the stack's, selects or makes the stack, and
    /// runs `pulumi up` on it.
    ///
    /// `imports` are the keys of the program's resources that have an
    /// object to take charge of, with its identifier. If there are any
    /// and the stack does not exist yet, each gets the identifier as its
    /// `import` option before the stack is made.
    ///
    /// # Errors
    ///
    /// [`Error::Reconcile`] for `resource` if the files cannot be written,
    /// if the stack cannot be looked up, if `pulumi` cannot be run, or if
    /// it fails.
    async fn update_stack(
        &self,
        resource: &ResourceId,
        stack: &str,
        mut program: Value,
        imports: Vec<(String, String)>,
    ) -> Result<Outcome> {
        let failed = |reason: String| Error::Reconcile {
            resource: resource.clone(),
            reason,
        };
        let settings = &self.settings;
        let dir = self.stack_dir(stack);
        self.write_stack_files(&dir, stack, &program)
            .map_err(&failed)?;
        if !imports.is_empty()
            && !self
                .stack_exists(&dir, stack)
                .await
                .map_err(|error| failed(error.to_string()))?
        {
            for (key, id) in &imports {
                tracing::info!(%resource, key, id, "taking charge of an existing object");
                program["resources"][key]["options"]["import"] = Value::String(id.clone());
            }
            self.write_stack_files(&dir, stack, &program)
                .map_err(&failed)?;
        }
        let select = self
            .run(
                &dir,
                &["stack", "select", "--create", stack, "--non-interactive"],
            )
            .await
            .map_err(|error| failed(error.to_string()))?;
        if !select.ok {
            return Err(failed(format!(
                "the stack could not be selected or made: {}",
                tidy(&select.stderr)
            )));
        }
        let mut args = vec![
            "up",
            "--yes",
            "--non-interactive",
            "--skip-preview",
            "--json",
            "--stack",
            stack,
        ];
        if settings.refresh {
            args.push("--refresh");
        }
        let packs: Vec<String> = settings
            .policy_packs
            .iter()
            .map(|pack| pack.display().to_string())
            .collect();
        for pack in &packs {
            args.push("--policy-pack");
            args.push(pack);
        }
        args.extend(settings.extra_args.iter().map(String::as_str));
        let run = self
            .run(&dir, &args)
            .await
            .map_err(|error| failed(error.to_string()))?;
        let report = read_events(&run.stdout);
        if !run.ok || report.result.as_deref() != Some("succeeded") {
            return Err(failed(report.reason(&run)));
        }
        Ok(Outcome {
            summary: report.summary(),
            changed: report.changed,
            exports: report.exports(),
        })
    }

    /// What `member`, a resource of the stack that holds `unit`, exports
    /// under `attribute`: the stack's output of that name, as `pulumi
    /// stack output` gives it.
    ///
    /// The command is run in the project directory, not the stack's, so
    /// that an update of the stack under way in its own directory is not
    /// disturbed. A stack that does not exist has nothing to give yet; one
    /// that exists without the output has it missing, as when its program
    /// was last written before the attribute was exported.
    ///
    /// # Errors
    ///
    /// [`Error::Engine`] if `pulumi` cannot be run, fails for any reason
    /// but the stack's absence, or prints something that is not a JSON
    /// object.
    pub async fn exported(
        &self,
        unit: &ResourceId,
        member: &ResourceId,
        attribute: &str,
    ) -> Result<Exported> {
        let stack = self.stack_name(unit);
        let run = self
            .run(
                &self.project_dir(),
                &[
                    "stack",
                    "output",
                    "--json",
                    "--show-secrets",
                    "--stack",
                    &stack,
                ],
            )
            .await?;
        if !run.ok {
            if run.stderr.contains("no stack named") {
                return Ok(Exported::NotYet);
            }
            return Err(Error::Engine(format!(
                "the outputs of the stack {stack} could not be read: {}",
                tidy(&run.stderr)
            )));
        }
        let mut outputs: Map<String, Value> = if run.stdout.trim().is_empty() {
            Map::new()
        } else {
            serde_json::from_str(&run.stdout).map_err(|error| {
                Error::Engine(format!(
                    "pulumi stack output printed something unexpected for {stack}: {error}"
                ))
            })?
        };
        Ok(match outputs.remove(&export_name(member, attribute)) {
            Some(value) => Exported::Value(value),
            None => Exported::Missing,
        })
    }

    /// Destroys and removes the stack if it exists, and does nothing if it
    /// does not.
    ///
    /// # Errors
    ///
    /// [`Error::Reconcile`] for `resource` if the stack cannot be looked
    /// up, or if `pulumi destroy` cannot be run or fails.
    async fn remove_stack(&self, resource: &ResourceId, stack: &str) -> Result<Outcome> {
        let failed = |reason: String| Error::Reconcile {
            resource: resource.clone(),
            reason,
        };
        let settings = &self.settings;
        let dir = self.stack_dir(stack);
        // A destroy does not run the program, but it needs a project to
        // stand in, and the directory may be gone since the stack was
        // made, as after a move to another host.
        self.write_stack_files(&dir, stack, &self.empty_program())
            .map_err(&failed)?;
        if !self
            .stack_exists(&dir, stack)
            .await
            .map_err(|error| failed(error.to_string()))?
        {
            return Ok(Outcome {
                summary: "no stack to remove".to_owned(),
                changed: false,
                exports: Exports::default(),
            });
        }
        let mut args = vec![
            "destroy",
            "--yes",
            "--non-interactive",
            "--remove",
            "--json",
            "--stack",
            stack,
        ];
        if settings.refresh {
            args.push("--refresh");
        }
        args.extend(settings.extra_args.iter().map(String::as_str));
        let run = self
            .run(&dir, &args)
            .await
            .map_err(|error| failed(error.to_string()))?;
        let report = read_events(&run.stdout);
        if !run.ok || report.result.as_deref() != Some("succeeded") {
            return Err(failed(report.reason(&run)));
        }
        // Nothing in the directory is a record. A failure to remove it
        // leaves a program that the next update, if the rows come back,
        // writes over.
        let _ = std::fs::remove_dir_all(&dir);
        Ok(Outcome {
            summary: report.summary(),
            changed: report.changed,
            exports: report.exports(),
        })
    }

    /// Whether the stack exists, asked of the backend for that one stack.
    ///
    /// This is `pulumi stack select` without `--create`, which fails with
    /// `no stack named` when the stack is not there. It costs the same
    /// however many stacks the project has, which listing them does not.
    ///
    /// # Errors
    ///
    /// [`Error::Engine`] if the command cannot be run, or fails for any
    /// reason but the stack's absence.
    async fn stack_exists(&self, dir: &Path, stack: &str) -> Result<bool> {
        let run = self
            .run(dir, &["stack", "select", stack, "--non-interactive"])
            .await?;
        if run.ok {
            return Ok(true);
        }
        if run.stderr.contains("no stack named") {
            return Ok(false);
        }
        Err(Error::Engine(format!(
            "the stack {stack} could not be looked up: {}",
            tidy(&run.stderr)
        )))
    }

    /// Every resource that has a stack in the project, sorted.
    ///
    /// Pulumi reads every stack of the project to list them, so this grows
    /// with their number: on a file backend, seconds for thousands of
    /// stacks and minutes for tens of thousands. The periodic loop calls
    /// it once per full pass, and nothing else does.
    ///
    /// Stacks whose names do not start with the prefix, or do not parse as
    /// a resource id after it, are not this engine's and are left out.
    ///
    /// # Errors
    ///
    /// [`Error::Engine`] if the stacks cannot be listed.
    pub async fn managed(&self) -> Result<Vec<ResourceId>> {
        let mut resources: Vec<ResourceId> = self
            .stacks()
            .await?
            .iter()
            .filter_map(|stack| self.resource_of(stack))
            .collect();
        resources.sort();
        resources.dedup();
        Ok(resources)
    }

    /// The names of every stack in the project, as `pulumi stack ls`
    /// lists them.
    ///
    /// # Errors
    ///
    /// [`Error::Engine`] if the command cannot be run, fails, or prints
    /// something that is not a list of stacks.
    pub async fn stacks(&self) -> Result<Vec<String>> {
        let run = self
            .run(&self.project_dir(), &["stack", "ls", "--json"])
            .await?;
        if !run.ok {
            return Err(Error::Engine(format!(
                "the stacks could not be listed: {}",
                tidy(&run.stderr)
            )));
        }
        let listed: Vec<Value> = serde_json::from_str(&run.stdout).map_err(|error| {
            Error::Engine(format!(
                "pulumi stack ls printed something unexpected: {error}"
            ))
        })?;
        Ok(listed
            .iter()
            .filter_map(|stack| stack.get("name").and_then(Value::as_str))
            .map(str::to_owned)
            .collect())
    }

    /// The directory that stands for the project.
    fn project_dir(&self) -> PathBuf {
        self.settings.work_dir.join(PROJECT_DIR)
    }

    /// The directory of a stack's program.
    fn stack_dir(&self, stack: &str) -> PathBuf {
        self.settings.work_dir.join(stack)
    }

    /// A program with no resources, for a directory that only has to be a
    /// project.
    fn empty_program(&self) -> Value {
        json!({ "name": self.settings.project, "runtime": "yaml" })
    }

    /// Writes a stack's program and configuration into `dir`, making the
    /// directory if it is missing.
    ///
    /// The configuration is the same for every stack, and is written each
    /// time, so that a change in the controller's configuration reaches
    /// every stack at its next update.
    fn write_stack_files(
        &self,
        dir: &Path,
        stack: &str,
        program: &Value,
    ) -> std::result::Result<(), String> {
        write_json(&dir.join("Pulumi.yaml"), program)?;
        let config = json!({ "config": toml_to_json(&self.settings.config) });
        write_json(&dir.join(format!("Pulumi.{stack}.yaml")), &config)
    }

    /// Runs `pulumi` with `args` in `dir`, and waits for it to end.
    ///
    /// The process is given the controller's environment, with
    /// `PULUMI_SKIP_UPDATE_CHECK` set, and `PULUMI_BACKEND_URL`,
    /// `PULUMI_HOME` and `PULUMI_CONFIG_PASSPHRASE` set from the
    /// configuration where it gives them. Its input is closed. It is
    /// killed if this future is dropped.
    ///
    /// # Errors
    ///
    /// [`Error::Engine`] if the process cannot be started or waited for.
    /// A process that ran and failed is not an error here: `ok` is false.
    async fn run(&self, dir: &Path, args: &[&str]) -> Result<Run> {
        let mut command = Command::new(&self.settings.binary);
        command
            .args(args)
            .current_dir(dir)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .env("PULUMI_SKIP_UPDATE_CHECK", "true");
        if let Some(backend) = &self.settings.backend {
            command.env("PULUMI_BACKEND_URL", backend);
        }
        if let Some(home) = &self.settings.home {
            command.env("PULUMI_HOME", home);
        }
        if let Some(passphrase) = &self.passphrase {
            command.env("PULUMI_CONFIG_PASSPHRASE", passphrase);
        }
        let output = command.output().await.map_err(|error| {
            Error::Engine(format!(
                "{} cannot be run: {error}",
                self.settings.binary.display()
            ))
        })?;
        Ok(Run {
            ok: output.status.success(),
            status: output.status.to_string(),
            stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
            stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
        })
    }
}

/// One resource of a program: what a row becomes.
#[derive(Debug)]
struct Entry {
    /// The resource's key in the program, and so the last part of its URN.
    key: String,
    /// The resource itself, which names its outputs.
    resource: ResourceId,
    /// Its Pulumi type.
    token: String,
    properties: Map<String, Value>,
    lifecycle: LifecycleConfig,
    /// The attributes to write as outputs of the stack.
    exports: BTreeSet<String>,
    /// The properties that read another member of the same stack.
    links: Vec<Link>,
    /// The identifier of an object that exists already, which the stack
    /// is to take charge of when it is first made.
    import_id: Option<String>,
}

/// The keys of `entries` that carry an import id, with the id.
fn imports_of(entries: &[Entry]) -> Vec<(String, String)> {
    entries
        .iter()
        .filter_map(|entry| entry.import_id.clone().map(|id| (entry.key.clone(), id)))
        .collect()
}

/// The key of a row's resource in the program of a stack that groups
/// rows: `kind_name`.
///
/// A key is what an expression such as `${key.id}` names, and Pulumi reads
/// a `.` in an expression as the step from a resource to its property, so
/// the `.` of the resource id gives way to `_`. Two rows whose ids differ
/// only in where the `.` falls would make the same key, and the engine
/// refuses to write their program.
pub fn member_key(resource: &ResourceId) -> String {
    format!("{}_{}", resource.kind().as_str(), resource.name())
}

/// What a `pulumi` process came to.
#[derive(Debug)]
struct Run {
    ok: bool,
    status: String,
    stdout: String,
    stderr: String,
}

/// What Pulumi's events said about an update or a destroy.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Report {
    /// Whether any resource but the stack itself was created, updated,
    /// replaced, deleted or imported.
    pub changed: bool,
    /// The summary's count of each operation, by Pulumi's names for them.
    pub changes: BTreeMap<String, u64>,
    /// The summary's result: `succeeded` or `failed`. None if no summary
    /// was printed, as when Pulumi stopped before it got that far.
    pub result: Option<String>,
    /// The messages of the diagnostics of severity `error`, tidied.
    pub errors: Vec<String>,
    /// The stack's outputs before the run, from the stack's own resource
    /// event. Empty if there was none, or the stack was new.
    pub outputs_before: BTreeMap<String, Value>,
    /// The stack's outputs after the run. Empty if there was no event, or
    /// the stack was destroyed.
    pub outputs_after: BTreeMap<String, Value>,
}

impl Report {
    /// The outputs before and after, as the engine's exports.
    pub fn exports(&self) -> Exports {
        Exports {
            before: self.outputs_before.clone(),
            after: self.outputs_after.clone(),
        }
    }

    /// The counts as one line, such as `create 1, same 1`, or `nothing to
    /// do` if there were none.
    pub fn summary(&self) -> String {
        if self.changes.is_empty() {
            return "nothing to do".to_owned();
        }
        self.changes
            .iter()
            .map(|(op, count)| format!("{op} {count}"))
            .collect::<Vec<_>>()
            .join(", ")
    }

    /// Why a run failed: its error diagnostics, or what it wrote to its
    /// standard error, or failing both, its result and exit status.
    fn reason(&self, run: &Run) -> String {
        if !self.errors.is_empty() {
            return self.errors.join("; ");
        }
        let stderr = tidy(&run.stderr);
        if !stderr.is_empty() {
            return stderr;
        }
        format!(
            "pulumi {} with result {}",
            run.status,
            self.result.as_deref().unwrap_or("unknown")
        )
    }
}

/// Reads the events that `pulumi --json` printed, one object per line.
///
/// A line that is not JSON is passed over. A resource event counts as a
/// change unless its operation leaves the resource as it was: `same`,
/// `read`, `refresh` and the discards. The stack's own resource never
/// counts as a change; its event gives the stack's outputs before and
/// after instead. The errors are the diagnostics of severity `error` and
/// the violations of mandatory policies, in the order they were printed.
pub fn read_events(stdout: &str) -> Report {
    let mut report = Report::default();
    for line in stdout.lines() {
        let Ok(event) = serde_json::from_str::<Value>(line) else {
            continue;
        };
        if let Some(metadata) = event.get("resOutputsEvent").and_then(|e| e.get("metadata")) {
            let op = metadata.get("op").and_then(Value::as_str).unwrap_or("");
            let ty = metadata.get("type").and_then(Value::as_str).unwrap_or("");
            let unchanged = matches!(
                op,
                "same"
                    | "read"
                    | "refresh"
                    | "discard"
                    | "discard-replaced"
                    | "remove-pending-replace"
            );
            if ty == STACK_TYPE {
                let outputs = |side: &str| -> BTreeMap<String, Value> {
                    match metadata.get(side).and_then(|state| state.get("outputs")) {
                        Some(Value::Object(outputs)) => outputs
                            .iter()
                            .map(|(name, value)| (name.clone(), value.clone()))
                            .collect(),
                        _ => BTreeMap::new(),
                    }
                };
                report.outputs_before = outputs("old");
                report.outputs_after = outputs("new");
            } else if !unchanged {
                report.changed = true;
            }
        }
        if let Some(summary) = event.get("summaryEvent") {
            report.result = summary
                .get("result")
                .and_then(Value::as_str)
                .map(str::to_owned);
            if let Some(Value::Object(changes)) = summary.get("resourceChanges") {
                report.changes.clear();
                for (op, count) in changes {
                    report
                        .changes
                        .insert(op.clone(), count.as_u64().unwrap_or(0));
                }
            }
        }
        if let Some(diagnostic) = event.get("diagnosticEvent")
            && diagnostic.get("severity").and_then(Value::as_str) == Some("error")
            && let Some(message) = diagnostic.get("message").and_then(Value::as_str)
        {
            let message = tidy(message);
            if !message.is_empty() {
                report.errors.push(message);
            }
        }
        // A policy violation is its own kind of event, not a diagnostic. A
        // mandatory one is why an update failed, so it is a reason.
        if let Some(policy) = event.get("policyEvent")
            && policy.get("enforcementLevel").and_then(Value::as_str) == Some("mandatory")
        {
            let name = policy
                .get("policyName")
                .and_then(Value::as_str)
                .unwrap_or("unnamed");
            let message = policy
                .get("message")
                .and_then(Value::as_str)
                .map(tidy)
                .unwrap_or_default();
            report.errors.push(format!("policy {name}: {message}"));
        }
    }
    report
}

/// Text as one line without colour codes, cut to [`REASON_LENGTH`].
fn tidy(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut chars = text.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '\x1b' && chars.peek() == Some(&'[') {
            chars.next();
            for next in chars.by_ref() {
                if next == 'm' {
                    break;
                }
            }
            continue;
        }
        if c.is_whitespace() {
            if !out.ends_with(' ') && !out.is_empty() {
                out.push(' ');
            }
            continue;
        }
        out.push(c);
    }
    let trimmed = out.trim_end();
    if trimmed.chars().count() > REASON_LENGTH {
        let cut: String = trimmed.chars().take(REASON_LENGTH).collect();
        format!("{cut}...")
    } else {
        trimmed.to_owned()
    }
}

/// Writes `value` as JSON to `path`, making the parent directory if it is
/// missing.
fn write_json(path: &Path, value: &Value) -> std::result::Result<(), String> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|error| format!("{} cannot be made: {error}", parent.display()))?;
    }
    let text = serde_json::to_string_pretty(value).map_err(|error| error.to_string())?;
    std::fs::write(path, text)
        .map_err(|error| format!("{} cannot be written: {error}", path.display()))
}

/// A TOML table as JSON, for a stack's configuration.
///
/// A date or a time becomes its TOML text, which is the only form JSON
/// has for it.
fn toml_to_json(table: &toml::Table) -> Value {
    fn convert(value: &toml::Value) -> Value {
        match value {
            toml::Value::String(text) => Value::String(text.clone()),
            toml::Value::Integer(number) => Value::from(*number),
            toml::Value::Float(number) => Value::from(*number),
            toml::Value::Boolean(flag) => Value::Bool(*flag),
            toml::Value::Datetime(datetime) => Value::String(datetime.to_string()),
            toml::Value::Array(items) => Value::Array(items.iter().map(convert).collect()),
            toml::Value::Table(table) => toml_to_json(table),
        }
    }
    Value::Object(
        table
            .iter()
            .map(|(key, value)| (key.clone(), convert(value)))
            .collect(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn engine(extra: &str) -> PulumiEngine {
        let settings: PulumiConfig = toml::from_str(&format!(
            "work_dir = \"{}\"\n{extra}",
            std::env::temp_dir().join("mpc-pulumi-unit").display()
        ))
        .unwrap();
        PulumiEngine::new(&settings).unwrap()
    }

    #[test]
    fn a_stack_is_named_after_its_resource_and_the_name_parses_back() {
        let engine = engine("stack_prefix = \"prod-\"");
        let resource: ResourceId = "gcp_instance.web-1".parse().unwrap();
        assert_eq!(engine.stack_name(&resource), "prod-gcp_instance.web-1");
        assert_eq!(
            engine.resource_of("prod-gcp_instance.web-1"),
            Some(resource)
        );
        assert_eq!(engine.resource_of("gcp_instance.web-1"), None);
        let group: ResourceId = "stack.fleet".parse().unwrap();
        assert_eq!(engine.stack_name(&group), "prod-fleet");
        assert_eq!(engine.resource_of("prod-fleet"), Some(group));
    }

    #[test]
    fn a_group_is_one_program_with_a_resource_per_row() {
        let engine = engine("project = \"demo\"\ntypes = { vm = \"gcp:compute:Instance\" }");
        let mapping: crate::config::Mapping =
            toml::from_str("table = \"vms\"\nresource_type = \"vm\"\nstack = \"fleet\"").unwrap();
        let rows: Vec<Row> = ["web-1", "web-2"]
            .iter()
            .map(|name| {
                Row::new(
                    format!("vm.{name}").parse().unwrap(),
                    mapping.clone(),
                    json!({"id": name, "machine_type": "e2-small"}),
                )
            })
            .collect();
        let entries: Vec<Entry> = rows
            .into_iter()
            .map(|row| {
                let key = member_key(&row.resource);
                engine.entry_of(row, key).unwrap()
            })
            .collect();
        let program = engine.program_of("two rows", entries);
        let resources = program["resources"].as_object().unwrap();
        assert_eq!(resources.len(), 2);
        assert_eq!(resources["vm_web-1"]["type"], "gcp:compute:Instance");
        assert_eq!(
            resources["vm_web-2"]["properties"]["machineType"],
            "e2-small"
        );
        assert!(program.get("outputs").is_none(), "nothing is exported");
    }

    #[test]
    fn exports_become_outputs_and_links_become_expressions() {
        let engine = engine(
            "project = \"demo\"\ntypes = { topic = \"gcp:pubsub:Topic\", sub = \"gcp:pubsub:Subscription\" }",
        );
        let mapping: crate::config::Mapping =
            toml::from_str("table = \"t\"\ntype_column = \"kind\"\nstack = \"prod\"").unwrap();
        let mut topic = Row::new(
            "topic.orders".parse().unwrap(),
            mapping.clone(),
            json!({"id": "orders", "kind": "topic", "name": "orders"}),
        );
        topic.exports = BTreeSet::from(["id".to_owned(), "name".to_owned()]);
        let mut sub = Row::new(
            "sub.billing".parse().unwrap(),
            mapping,
            json!({"id": "billing", "kind": "sub", "topic_name": "orders", "ack_deadline": 20}),
        );
        sub.links.push(Link {
            column: "topic_name".into(),
            target: "topic.orders".parse().unwrap(),
            attribute: "id".into(),
        });
        let entries = vec![
            engine.entry_of(topic, "topic_orders".into()).unwrap(),
            engine.entry_of(sub, "sub_billing".into()).unwrap(),
        ];
        let program = engine.program_of("two rows", entries);
        assert_eq!(
            program["outputs"],
            json!({
                "topic.orders.id": "${topic_orders.id}",
                "topic.orders.name": "${topic_orders.name}",
            })
        );
        assert_eq!(
            program["resources"]["sub_billing"]["properties"]["topicName"], "${topic_orders.id}",
            "the link replaces the column's value, under the camel-cased name"
        );
        assert_eq!(
            program["resources"]["sub_billing"]["properties"]["ackDeadline"],
            20
        );

        let plain = PulumiEngine::new(
            &toml::from_str(&format!(
                "work_dir = \"{}\"\ncamel_case_columns = false\ntypes = {{ sub = \"x:y:Z\" }}",
                std::env::temp_dir().join("mpc-pulumi-unit").display()
            ))
            .unwrap(),
        )
        .unwrap();
        let mapping: crate::config::Mapping =
            toml::from_str("table = \"t\"\nresource_type = \"sub\"").unwrap();
        let mut sub = Row::new(
            "sub.a".parse().unwrap(),
            mapping,
            json!({"id": "a", "topic_name": "orders"}),
        );
        sub.links.push(Link {
            column: "topic_name".into(),
            target: "topic.orders".parse().unwrap(),
            attribute: "arn".into(),
        });
        let program = plain.program_of("one", vec![plain.entry_of(sub, "sub_a".into()).unwrap()]);
        assert_eq!(
            program["resources"]["sub_a"]["properties"]["topic_name"],
            "${topic_orders.arn}"
        );
        assert_eq!(member_key(&"vm.web-1".parse().unwrap()), "vm_web-1");
    }

    #[test]
    fn the_program_carries_the_lifecycle_as_options() {
        let engine = engine("project = \"demo\"");
        let resource: ResourceId = "vm.web-1".parse().unwrap();
        let lifecycle: LifecycleConfig =
            toml::from_str("prevent_destroy = true\nretain = true").unwrap();
        let mut properties = Map::new();
        properties.insert("machineType".into(), json!("e2-small"));
        let program = engine.program(&resource, "gcp:compute:Instance", properties, &lifecycle);
        assert_eq!(program["name"], "demo");
        assert_eq!(program["runtime"], "yaml");
        let web = &program["resources"]["web-1"];
        assert_eq!(web["type"], "gcp:compute:Instance");
        assert_eq!(web["properties"]["machineType"], "e2-small");
        assert_eq!(web["options"]["protect"], true);
        assert_eq!(web["options"]["retainOnDelete"], true);
        assert_eq!(web["options"]["deleteBeforeReplace"], true);
    }

    #[test]
    fn events_say_whether_anything_changed_and_why_a_run_failed() {
        let created = concat!(
            r#"{"sequence":1,"resourcePreEvent":{"metadata":{"op":"create","type":"pulumi:pulumi:Stack"}}}"#,
            "\n",
            r#"not json"#,
            "\n",
            r#"{"sequence":2,"resOutputsEvent":{"metadata":{"op":"create","type":"random:index/randomPet:RandomPet"}}}"#,
            "\n",
            r#"{"sequence":3,"resOutputsEvent":{"metadata":{"op":"create","type":"pulumi:pulumi:Stack"}}}"#,
            "\n",
            r#"{"sequence":4,"summaryEvent":{"resourceChanges":{"create":2},"result":"succeeded"}}"#,
            "\n",
        );
        let report = read_events(created);
        assert!(report.changed);
        assert_eq!(report.result.as_deref(), Some("succeeded"));
        assert_eq!(report.summary(), "create 2");

        let same = concat!(
            r#"{"resOutputsEvent":{"metadata":{"op":"same","type":"random:index/randomPet:RandomPet"}}}"#,
            "\n",
            r#"{"resOutputsEvent":{"metadata":{"op":"same","type":"pulumi:pulumi:Stack"}}}"#,
            "\n",
            r#"{"summaryEvent":{"resourceChanges":{"same":2},"result":"succeeded"}}"#,
            "\n",
        );
        let report = read_events(same);
        assert!(!report.changed);
        assert_eq!(report.summary(), "same 2");

        let only_the_stack = concat!(
            r#"{"resOutputsEvent":{"metadata":{"op":"create","type":"pulumi:pulumi:Stack"}}}"#,
            "\n",
            r#"{"diagnosticEvent":{"prefix":"error: ","message":"Error: random:index/randomPet:RandomPet is not assignable\n\n  on Pulumi.yaml line 7","severity":"error"}}"#,
            "\n",
            r#"{"diagnosticEvent":{"prefix":"debug: ","message":"Registering resource","severity":"debug"}}"#,
            "\n",
            r#"{"summaryEvent":{"resourceChanges":{"create":1},"result":"failed"}}"#,
            "\n",
        );
        let report = read_events(only_the_stack);
        assert!(!report.changed);
        assert_eq!(report.result.as_deref(), Some("failed"));
        assert_eq!(
            report.errors,
            ["Error: random:index/randomPet:RandomPet is not assignable on Pulumi.yaml line 7"]
        );
        let run = Run {
            ok: false,
            status: "exit status: 1".into(),
            stdout: String::new(),
            stderr: String::new(),
        };
        assert!(report.reason(&run).starts_with("Error: random"));
        assert_eq!(
            Report::default().reason(&run),
            "pulumi exit status: 1 with result unknown"
        );
    }

    #[test]
    fn a_mandatory_policy_violation_is_a_reason() {
        let refused = concat!(
            r#"{"sequence":11,"timestamp":1791461761,"policyEvent":{"resourceUrn":"urn:pulumi:pet.one::mpc-try::random:index/randomPet:RandomPet::pet","message":"Pet names have at most two words.\na pet may have at most two words; lower length in the row\n","color":"never","policyName":"short-pets-only","policyPackName":"scratch-guardrails","policyPackVersion":"0.1.0","policyPackVersionTag":"0.1.0","enforcementLevel":"mandatory"}}"#,
            "
",
            r#"{"policyEvent":{"message":"only advice","policyName":"advice","enforcementLevel":"advisory"}}"#,
            "
",
            r#"{"summaryEvent":{"resourceChanges":{"same":1},"result":"failed"}}"#,
            "
",
        );
        let report = read_events(refused);
        assert_eq!(report.result.as_deref(), Some("failed"));
        assert_eq!(
            report.errors,
            [
                "policy short-pets-only: Pet names have at most two words. a pet may have at most two words; lower length in the row"
            ]
        );
    }

    // ---- the engine over a stand-in for `pulumi` -----------------------------------------------
    //
    // A shell script records every call it gets, with its working directory
    // and the PULUMI_* variables, and answers from files the test writes
    // beside it. Everything the engine does with a process can be seen
    // without Pulumi, a backend or a network.

    use std::path::PathBuf;
    use std::time::Duration;

    /// A directory of the test's own, with the script in it.
    struct Fake {
        dir: PathBuf,
    }

    impl Fake {
        fn new(name: &str) -> Self {
            let dir = std::env::temp_dir().join(format!(
                "mpc-fake-{}-{name}-{}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            ));
            let _ = std::fs::remove_dir_all(&dir);
            std::fs::create_dir_all(&dir).unwrap();
            let script = format!(
                r#"#!/bin/sh
D="{dir}"
echo "$PWD :: $*" >> "$D/calls.log"
env | grep -E '^PULUMI_' | sort >> "$D/env.log"
read_or() {{ if [ -f "$D/$1" ]; then cat "$D/$1"; else echo "$2"; fi; }}
case "$1" in
  version) read_or version.out v9.9.9; exit "$(read_or version.exit 0)";;
  stack)
    case "$2" in
      ls) read_or ls.out '[]'; exit "$(read_or ls.exit 0)";;
      output) [ -f "$D/output.err" ] && cat "$D/output.err" 1>&2; read_or output.out '{{}}'; exit "$(read_or output.exit 0)";;
      select)
        case "$*" in
          *--create*) exit "$(read_or create.exit 0)";;
          *) [ -f "$D/select.err" ] && cat "$D/select.err" 1>&2; exit "$(read_or select.exit 0)";;
        esac;;
    esac;;
  up)
    if [ -f "$D/up.sleep" ]; then echo $$ > "$D/up.pid"; sleep "$(cat "$D/up.sleep")"; fi
    [ -f "$D/up.out" ] && cat "$D/up.out"; [ -f "$D/up.err" ] && cat "$D/up.err" 1>&2
    exit "$(read_or up.exit 0)";;
  destroy) [ -f "$D/destroy.out" ] && cat "$D/destroy.out"; exit "$(read_or destroy.exit 0)";;
esac
exit 0
"#,
                dir = dir.display()
            );
            let path = dir.join("pulumi");
            std::fs::write(&path, script).unwrap();
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
            }
            Self { dir }
        }

        fn write(&self, name: &str, text: &str) {
            std::fs::write(self.dir.join(name), text).unwrap();
        }

        fn calls(&self) -> Vec<String> {
            std::fs::read_to_string(self.dir.join("calls.log"))
                .unwrap_or_default()
                .lines()
                .map(str::to_owned)
                .collect()
        }

        fn env_log(&self) -> String {
            std::fs::read_to_string(self.dir.join("env.log")).unwrap_or_default()
        }

        fn engine(&self, extra: &str) -> PulumiEngine {
            std::fs::write(self.dir.join("pass"), "s3cret\n").unwrap();
            let settings: PulumiConfig = toml::from_str(&format!(
                "binary = \"{bin}\"\nwork_dir = \"{work}\"\nbackend = \"file://{backend}\"\nhome = \"{home}\"\npassphrase_file = \"{pass}\"\nproject = \"demo\"\n{extra}",
                bin = self.dir.join("pulumi").display(),
                work = self.dir.join("work").display(),
                backend = self.dir.join("backend").display(),
                home = self.dir.join("home").display(),
                pass = self.dir.join("pass").display(),
            ))
            .unwrap();
            PulumiEngine::new(&settings).unwrap()
        }

        fn program(&self, stack: &str) -> Value {
            serde_json::from_str(
                &std::fs::read_to_string(self.dir.join("work").join(stack).join("Pulumi.yaml"))
                    .unwrap(),
            )
            .unwrap()
        }
    }

    impl Drop for Fake {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }

    fn row(kind: &str, name: &str, json: Value) -> Row {
        let mapping: crate::config::Mapping =
            toml::from_str(&format!("table = \"t\"\nresource_type = \"{kind}\"")).unwrap();
        Row::new(format!("{kind}.{name}").parse().unwrap(), mapping, json)
    }

    const SUCCEEDED: &str = concat!(
        r#"{"resOutputsEvent":{"metadata":{"op":"create","type":"pulumi:pulumi:Stack"}}}"#,
        "\n",
        r#"{"resOutputsEvent":{"metadata":{"op":"create","type":"random:index/randomPet:RandomPet"}}}"#,
        "\n",
        r#"{"summaryEvent":{"resourceChanges":{"create":2},"result":"succeeded"}}"#,
        "\n",
    );
    const SAME: &str = concat!(
        r#"{"resOutputsEvent":{"metadata":{"op":"same","type":"random:index/randomPet:RandomPet"}}}"#,
        "\n",
        r#"{"summaryEvent":{"resourceChanges":{"same":2},"result":"succeeded"}}"#,
        "\n",
    );

    #[tokio::test]
    async fn the_version_comes_from_the_command_and_a_failure_is_an_engine_error() {
        let fake = Fake::new("version");
        let engine = fake.engine("");
        fake.write("version.out", "v3.259.0\n");
        assert_eq!(engine.version().await.unwrap(), "v3.259.0");
        fake.write("version.exit", "1");
        let error = engine.version().await.unwrap_err();
        assert!(matches!(error, Error::Engine(_)), "{error}");
        assert!(
            fake.calls().iter().all(|call| call.ends_with(":: version")),
            "{:?}",
            fake.calls()
        );
        assert!(fake.env_log().contains("PULUMI_SKIP_UPDATE_CHECK=true"));
        assert!(fake.env_log().contains("PULUMI_CONFIG_PASSPHRASE=s3cret"));
        assert!(fake.env_log().contains("PULUMI_BACKEND_URL=file://"));
        assert!(fake.env_log().contains("PULUMI_HOME="));
    }

    #[tokio::test]
    async fn stacks_are_listed_and_sorted_into_resources_and_groups() {
        let fake = Fake::new("stacks");
        let engine = fake.engine("stack_prefix = \"p-\"");
        fake.write(
            "ls.out",
            r#"[{"name":"p-vm.web-2"},{"name":"p-fleet"},{"name":"other.thing"},{"name":"p-vm.web-1"},{"name":"p-bad name"},{"name":"p-vm.web-1"}]"#,
        );
        assert_eq!(engine.stacks().await.unwrap().len(), 6);
        let managed = engine.managed().await.unwrap();
        let names: Vec<String> = managed.iter().map(ToString::to_string).collect();
        assert_eq!(
            names,
            ["stack.fleet", "vm.web-1", "vm.web-2"],
            "prefix stripped, foreign and invalid names dropped, sorted, deduplicated"
        );
        fake.write("ls.out", "not json");
        let error = engine.stacks().await.unwrap_err();
        assert!(error.to_string().contains("unexpected"), "{error}");
        fake.write("ls.exit", "1");
        assert!(matches!(
            engine.stacks().await.unwrap_err(),
            Error::Engine(_)
        ));
    }

    #[tokio::test]
    async fn a_row_with_an_import_id_is_imported_when_the_stack_is_made_and_not_after() {
        let fake = Fake::new("import");
        let engine = fake.engine("[types]\nvm = \"gcp:compute:Instance\"\n");
        let mapping: crate::config::Mapping =
            toml::from_str("table = \"t\"\nresource_type = \"vm\"\nimport_column = \"object\"")
                .unwrap();
        let row = || {
            Row::new(
                "vm.web".parse().unwrap(),
                mapping.clone(),
                json!({"id": "web", "object": "projects/lab/zones/z/instances/web", "machine_type": "e2-small"}),
            )
        };
        // The stack is not there yet: the object is imported.
        fake.write("select.err", "error: no stack named 'vm.web' found\n");
        fake.write("select.exit", "1");
        fake.write("up.out", SUCCEEDED);
        engine
            .reconcile(&"vm.web".parse().unwrap(), Some(row()))
            .await
            .unwrap();
        let program = fake.program("vm.web");
        let web = &program["resources"]["web"];
        assert_eq!(
            web["options"]["import"],
            "projects/lab/zones/z/instances/web"
        );
        assert!(
            web["properties"].get("object").is_none(),
            "the import column is not a property: {web}"
        );
        let calls = fake.calls();
        let looked_up = calls
            .iter()
            .position(|call| call.ends_with(":: stack select vm.web --non-interactive"))
            .expect("the stack was looked up");
        let made = calls
            .iter()
            .position(|call| call.contains("stack select --create vm.web"))
            .expect("the stack was made");
        assert!(looked_up < made, "{calls:?}");

        // The stack exists now: the option is not set again.
        std::fs::remove_file(fake.dir.join("select.err")).unwrap();
        std::fs::remove_file(fake.dir.join("select.exit")).unwrap();
        fake.write("up.out", SAME);
        engine
            .reconcile(&"vm.web".parse().unwrap(), Some(row()))
            .await
            .unwrap();
        let program = fake.program("vm.web");
        assert!(
            program["resources"]["web"]["options"]
                .get("import")
                .is_none(),
            "{program}"
        );

        // A stack that cannot be looked up fails the reconciliation.
        fake.write("select.err", "error: the backend is unreachable\n");
        fake.write("select.exit", "1");
        let error = engine
            .reconcile(&"vm.web".parse().unwrap(), Some(row()))
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains("could not be looked up"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn a_row_is_written_as_a_program_and_updated_with_the_configured_arguments() {
        let fake = Fake::new("update");
        let engine = fake.engine(concat!(
            "stack_prefix = \"p-\"\n",
            "policy_packs = [\"/packs/guard\"]\n",
            "extra_args = [\"--diff\"]\n",
            "[types]\nvm = \"gcp:compute:Instance\"\n",
            "[config]\n\"gcp:project\" = \"demo\"\nretries = 3\n"
        ));
        fake.write("up.out", SUCCEEDED);
        let outcome = engine
            .reconcile(
                &"vm.web-1".parse().unwrap(),
                Some(row(
                    "vm",
                    "web-1",
                    json!({"id": "web-1", "machine_type": "e2-small", "labels": {"my_label": 1}, "gone": null}),
                )),
            )
            .await
            .unwrap();
        assert_eq!(
            outcome,
            Outcome {
                summary: "create 2".into(),
                changed: true,
                exports: Exports::default(),
            }
        );

        let program = fake.program("p-vm.web-1");
        assert_eq!(program["name"], "demo");
        assert_eq!(program["runtime"], "yaml");
        let web = &program["resources"]["web-1"];
        assert_eq!(web["type"], "gcp:compute:Instance");
        assert_eq!(
            web["properties"]["machineType"], "e2-small",
            "columns are camel cased"
        );
        assert_eq!(
            web["properties"]["labels"]["my_label"], 1,
            "values are passed as written"
        );
        assert!(
            web["properties"].get("id").is_none(),
            "the id column is not a property"
        );
        assert!(
            web["properties"].get("gone").is_none(),
            "a null column is not a property"
        );
        assert_eq!(web["options"]["deleteBeforeReplace"], true);
        let config: Value = serde_json::from_str(
            &std::fs::read_to_string(fake.dir.join("work/p-vm.web-1/Pulumi.p-vm.web-1.yaml"))
                .unwrap(),
        )
        .unwrap();
        assert_eq!(config["config"]["gcp:project"], "demo");
        assert_eq!(config["config"]["retries"], 3);

        let calls = fake.calls();
        assert_eq!(calls.len(), 2, "{calls:?}");
        assert!(
            calls[0].ends_with(":: stack select --create p-vm.web-1 --non-interactive"),
            "{}",
            calls[0]
        );
        assert!(
            calls[1].ends_with(":: up --yes --non-interactive --skip-preview --json --stack p-vm.web-1 --refresh --policy-pack /packs/guard --diff"),
            "{}",
            calls[1]
        );
        assert!(
            calls[1].starts_with(&fake.dir.join("work/p-vm.web-1").display().to_string()),
            "runs in the stack's directory: {}",
            calls[1]
        );
    }

    #[tokio::test]
    async fn refresh_can_be_turned_off_and_a_no_op_is_not_a_change() {
        let fake = Fake::new("norefresh");
        let engine = fake.engine("refresh = false\n[types]\nvm = \"gcp:compute:Instance\"");
        fake.write("up.out", SAME);
        let outcome = engine
            .reconcile(
                &"vm.a".parse().unwrap(),
                Some(row("vm", "a", json!({"id": "a"}))),
            )
            .await
            .unwrap();
        assert_eq!(
            outcome,
            Outcome {
                summary: "same 2".into(),
                changed: false,
                exports: Exports::default(),
            }
        );
        assert!(!fake.calls()[1].contains("--refresh"), "{:?}", fake.calls());
    }

    #[tokio::test]
    async fn exports_are_read_from_the_stack_and_reported_from_the_events() {
        let fake = Fake::new("exports");
        let engine = fake.engine("stack_prefix = \"p-\"\n[types]\ntopic = \"gcp:pubsub:Topic\"");
        let orders: ResourceId = "topic.orders".parse().unwrap();

        // Nothing there yet.
        fake.write("output.exit", "6");
        fake.write(
            "output.err",
            "error: no stack named 'p-topic.orders' found\n",
        );
        assert_eq!(
            engine.exported(&orders, &orders, "id").await.unwrap(),
            Exported::NotYet
        );
        let last = fake.calls().last().unwrap().clone();
        assert!(
            last.ends_with(":: stack output --json --show-secrets --stack p-topic.orders"),
            "{last}"
        );
        assert!(
            last.starts_with(&fake.dir.join("work/.project").display().to_string()),
            "runs in the project directory: {last}"
        );

        // An update that writes the output and reports it.
        fake.write("output.exit", "0");
        fake.write("output.err", "");
        fake.write(
            "up.out",
            concat!(
                r#"{"resOutputsEvent":{"metadata":{"op":"create","type":"gcp:pubsub/topic:Topic"}}}"#, "\n",
                r#"{"resOutputsEvent":{"metadata":{"op":"create","type":"pulumi:pulumi:Stack","old":null,"new":{"outputs":{"topic.orders.id":"projects/lab/topics/orders"}}}}}"#, "\n",
                r#"{"summaryEvent":{"resourceChanges":{"create":2},"result":"succeeded"}}"#, "\n"
            ),
        );
        let mut row = row("topic", "orders", json!({"id": "orders", "name": "orders"}));
        row.exports = BTreeSet::from(["id".to_owned()]);
        let outcome = engine.reconcile(&orders, Some(row)).await.unwrap();
        assert!(outcome.exports.before.is_empty());
        assert_eq!(
            outcome.exports.after["topic.orders.id"],
            "projects/lab/topics/orders"
        );
        assert_eq!(
            outcome.exports.changed_members(),
            BTreeSet::from([orders.clone()])
        );
        let program = fake.program("p-topic.orders");
        assert_eq!(program["outputs"]["topic.orders.id"], "${orders.id}");

        // The stack's outputs, read back.
        fake.write(
            "output.out",
            r#"{"topic.orders.id": "projects/lab/topics/orders"}"#,
        );
        assert_eq!(
            engine.exported(&orders, &orders, "id").await.unwrap(),
            Exported::Value(json!("projects/lab/topics/orders"))
        );
        assert_eq!(
            engine.exported(&orders, &orders, "arn").await.unwrap(),
            Exported::Missing
        );
        fake.write("output.out", "");
        assert_eq!(
            engine.exported(&orders, &orders, "id").await.unwrap(),
            Exported::Missing,
            "no outputs at all"
        );
        fake.write("output.out", "not json");
        assert!(matches!(
            engine.exported(&orders, &orders, "id").await.unwrap_err(),
            Error::Engine(_)
        ));
        fake.write("output.out", "{}");
        fake.write("output.exit", "1");
        fake.write("output.err", "error: the backend is unreachable\n");
        let error = engine.exported(&orders, &orders, "id").await.unwrap_err();
        assert!(
            matches!(error, Error::Engine(ref m) if m.contains("could not be read")),
            "{error}"
        );

        // A destroy reports what the stack exported, and nothing after.
        fake.write("select.exit", "0");
        fake.write(
            "destroy.out",
            concat!(
                r#"{"resOutputsEvent":{"metadata":{"op":"delete","type":"gcp:pubsub/topic:Topic"}}}"#, "\n",
                r#"{"resOutputsEvent":{"metadata":{"op":"delete","type":"pulumi:pulumi:Stack","old":{"outputs":{"topic.orders.id":"projects/lab/topics/orders"}},"new":null}}}"#, "\n",
                r#"{"summaryEvent":{"resourceChanges":{"delete":2},"result":"succeeded"}}"#, "\n"
            ),
        );
        let outcome = engine.reconcile(&orders, None).await.unwrap();
        assert_eq!(
            outcome.exports.before["topic.orders.id"],
            "projects/lab/topics/orders"
        );
        assert!(outcome.exports.after.is_empty());
    }

    #[tokio::test]
    async fn every_way_an_update_can_fail_is_a_reconcile_error_with_a_reason() {
        let fake = Fake::new("failures");
        let engine = fake.engine("[types]\nvm = \"gcp:compute:Instance\"");
        let id: ResourceId = "vm.a".parse().unwrap();
        let some = || Some(row("vm", "a", json!({"id": "a"})));
        let reason = |error: Error| match error {
            Error::Reconcile { resource, reason } => {
                assert_eq!(resource.to_string(), "vm.a");
                reason
            }
            other => panic!("not a reconcile error: {other}"),
        };

        // The stack cannot be selected or made.
        fake.write("create.exit", "1");
        let why = reason(engine.reconcile(&id, some()).await.unwrap_err());
        assert!(why.contains("could not be selected or made"), "{why}");
        fake.write("create.exit", "0");

        // Pulumi fails and says why in a diagnostic.
        fake.write("up.exit", "1");
        fake.write(
            "up.out",
            r#"{"diagnosticEvent":{"severity":"error","message":"Error: quota exceeded"}}"#,
        );
        let why = reason(engine.reconcile(&id, some()).await.unwrap_err());
        assert_eq!(why, "Error: quota exceeded");

        // A policy refuses the resource.
        fake.write(
            "up.out",
            concat!(
                r#"{"policyEvent":{"policyName":"no-ssh","message":"No SSH.\nnarrow the range","enforcementLevel":"mandatory"}}"#, "\n",
                r#"{"summaryEvent":{"resourceChanges":{"same":1},"result":"failed"}}"#, "\n"
            ),
        );
        let why = reason(engine.reconcile(&id, some()).await.unwrap_err());
        assert_eq!(why, "policy no-ssh: No SSH. narrow the range");

        // Nothing in the stream, something on standard error.
        fake.write("up.out", "");
        fake.write("up.err", "error: could not reach the backend\n");
        let why = reason(engine.reconcile(&id, some()).await.unwrap_err());
        assert_eq!(why, "error: could not reach the backend");

        // Nothing anywhere: the exit status and the result.
        fake.write("up.err", "");
        let why = reason(engine.reconcile(&id, some()).await.unwrap_err());
        assert!(
            why.starts_with("pulumi exit status: 1 with result unknown"),
            "{why}"
        );

        // A clean exit whose summary says failed is still a failure.
        fake.write("up.exit", "0");
        fake.write(
            "up.out",
            r#"{"summaryEvent":{"resourceChanges":{"create":1},"result":"failed"}}"#,
        );
        let why = reason(engine.reconcile(&id, some()).await.unwrap_err());
        assert!(why.contains("result failed"), "{why}");

        // A kind with no type never reaches Pulumi.
        let before = fake.calls().len();
        let error = engine
            .reconcile(
                &"cat.x".parse().unwrap(),
                Some(row("cat", "x", json!({"id": "x"}))),
            )
            .await
            .unwrap_err();
        assert!(
            matches!(error, Error::Mapping(ref m) if m.contains("no Pulumi type for cat")),
            "{error}"
        );
        assert_eq!(fake.calls().len(), before);
    }

    #[tokio::test]
    async fn a_missing_row_removes_the_stack_only_if_it_exists() {
        let fake = Fake::new("remove");
        let engine = fake.engine("extra_args = [\"--color\", \"never\"]");
        let id: ResourceId = "vm.a".parse().unwrap();

        // No stack: nothing to do, and no destroy.
        fake.write("select.exit", "6");
        fake.write("select.err", "error: no stack named 'vm.a' found\n");
        let outcome = engine.reconcile(&id, None).await.unwrap();
        assert_eq!(
            outcome,
            Outcome {
                summary: "no stack to remove".into(),
                changed: false,
                exports: Exports::default(),
            }
        );
        assert!(
            fake.calls().iter().all(|c| c.contains("stack select vm.a")),
            "{:?}",
            fake.calls()
        );
        assert!(
            fake.dir.join("work/vm.a/Pulumi.yaml").exists(),
            "a project stands in for the destroy"
        );

        // A stack: destroyed, removed, and its directory gone.
        fake.write("select.exit", "0");
        fake.write(
            "destroy.out",
            concat!(
                r#"{"resOutputsEvent":{"metadata":{"op":"delete","type":"random:index/randomPet:RandomPet"}}}"#, "\n",
                r#"{"summaryEvent":{"resourceChanges":{"delete":2},"result":"succeeded"}}"#, "\n"
            ),
        );
        let outcome = engine.reconcile(&id, None).await.unwrap();
        assert_eq!(
            outcome,
            Outcome {
                summary: "delete 2".into(),
                changed: true,
                exports: Exports::default(),
            }
        );
        let last = fake.calls().last().unwrap().clone();
        assert!(
            last.ends_with(":: destroy --yes --non-interactive --remove --json --stack vm.a --refresh --color never"),
            "{last}"
        );
        assert!(
            !fake.dir.join("work/vm.a").exists(),
            "the stack's directory is removed"
        );

        // The probe fails for another reason.
        fake.write("select.exit", "1");
        fake.write("select.err", "error: the backend is unreachable\n");
        let error = engine.reconcile(&id, None).await.unwrap_err();
        assert!(
            error.to_string().contains("could not be looked up"),
            "{error}"
        );

        // The destroy fails.
        fake.write("select.exit", "0");
        fake.write("destroy.exit", "1");
        fake.write(
            "destroy.out",
            r#"{"diagnosticEvent":{"severity":"error","message":"protected"}}"#,
        );
        let error = engine.reconcile(&id, None).await.unwrap_err();
        assert!(error.to_string().contains("protected"), "{error}");
    }

    #[tokio::test]
    async fn a_group_is_one_program_and_an_empty_group_removes_its_stack() {
        let fake = Fake::new("group");
        let engine =
            fake.engine("[types]\nvm = \"gcp:compute:Instance\"\ntopic = \"gcp:pubsub:Topic\"");
        fake.write("up.out", SUCCEEDED);
        let rows = vec![
            row(
                "vm",
                "web-1",
                json!({"id": "web-1", "machine_type": "e2-small"}),
            ),
            row("topic", "orders", json!({"id": "orders", "name": "orders"})),
        ];
        let outcome = engine.reconcile_group("prod", rows).await.unwrap();
        assert!(outcome.changed);
        let program = fake.program("prod");
        let resources = program["resources"].as_object().unwrap();
        assert_eq!(resources.len(), 2);
        assert_eq!(resources["vm_web-1"]["type"], "gcp:compute:Instance");
        assert_eq!(resources["topic_orders"]["properties"]["name"], "orders");
        assert!(
            program["description"]
                .as_str()
                .unwrap()
                .starts_with("2 rows of the stack prod")
        );
        assert!(
            fake.calls()[0].ends_with(":: stack select --create prod --non-interactive"),
            "{:?}",
            fake.calls()
        );

        // One bad row fails the whole group before Pulumi is asked.
        let before = fake.calls().len();
        let error = engine
            .reconcile_group(
                "prod",
                vec![
                    row("vm", "a", json!({"id": "a"})),
                    row("cat", "b", json!({"id": "b"})),
                ],
            )
            .await
            .unwrap_err();
        assert!(matches!(error, Error::Mapping(_)), "{error}");
        assert_eq!(fake.calls().len(), before);

        // An invalid group name is refused before anything runs.
        let error = engine.reconcile_group("a.b", Vec::new()).await.unwrap_err();
        assert!(matches!(error, Error::Mapping(_)), "{error}");

        // Two rows whose ids make one key are refused before anything runs.
        let before = fake.calls().len();
        let error = engine
            .reconcile_group(
                "prod",
                vec![
                    row("vm", "a_b", json!({"id": "a_b"})),
                    row("vm_a", "b", json!({"id": "b"})),
                ],
            )
            .await
            .unwrap_err();
        assert!(
            matches!(error, Error::Mapping(ref m) if m.contains("same key")),
            "{error}"
        );
        assert_eq!(fake.calls().len(), before);

        // No rows: the stack goes, if it is there.
        fake.write("select.exit", "6");
        fake.write("select.err", "error: no stack named 'prod' found\n");
        let outcome = engine.reconcile_group("prod", Vec::new()).await.unwrap();
        assert!(!outcome.changed);
    }

    #[tokio::test]
    async fn a_binary_that_cannot_run_is_reported_as_such() {
        let fake = Fake::new("nobinary");
        let settings: PulumiConfig = toml::from_str(&format!(
            "binary = \"{}\"\nwork_dir = \"{}\"",
            fake.dir.join("missing-pulumi").display(),
            fake.dir.join("work").display()
        ))
        .unwrap();
        let engine = PulumiEngine::new(&settings).unwrap();
        let error = engine.version().await.unwrap_err();
        assert!(
            matches!(error, Error::Engine(ref m) if m.contains("cannot be run")),
            "{error}"
        );
        let error = engine
            .reconcile(&"vm.a".parse().unwrap(), None)
            .await
            .unwrap_err();
        assert!(matches!(error, Error::Reconcile { .. }), "{error}");
        assert_eq!(engine.backend(), "whatever pulumi is logged in to");
        assert_eq!(engine.project(), "morph-controller");
    }

    #[test]
    fn setting_up_reads_the_passphrase_and_makes_the_directories() {
        let fake = Fake::new("setup");
        let engine = fake.engine("");
        assert!(fake.dir.join("work/.project/Pulumi.yaml").exists());
        assert!(
            fake.dir.join("backend").is_dir(),
            "a file backend's directory is made"
        );
        assert_eq!(
            engine.passphrase.as_deref(),
            Some("s3cret"),
            "trailing newline trimmed"
        );
        assert!(format!("{engine:?}").contains("PulumiEngine"));
        assert!(
            !format!("{engine:?}").contains("s3cret"),
            "the passphrase is never printed"
        );

        let missing: PulumiConfig = toml::from_str(&format!(
            "work_dir = \"{}\"\npassphrase_file = \"{}\"",
            fake.dir.join("w2").display(),
            fake.dir.join("nope").display()
        ))
        .unwrap();
        let error = PulumiEngine::new(&missing).unwrap_err();
        assert!(
            matches!(error, Error::Engine(ref m) if m.contains("passphrase file")),
            "{error}"
        );

        let tilde: PulumiConfig = toml::from_str(&format!(
            "work_dir = \"{}\"\nbackend = \"file://~/pulumi\"",
            fake.dir.join("w3").display()
        ))
        .unwrap();
        PulumiEngine::new(&tilde).expect("a home-relative backend is left to Pulumi");

        std::fs::write(fake.dir.join("afile"), "x").unwrap();
        let under_a_file: PulumiConfig = toml::from_str(&format!(
            "work_dir = \"{}\"",
            fake.dir.join("afile/work").display()
        ))
        .unwrap();
        let error = PulumiEngine::new(&under_a_file).unwrap_err();
        assert!(matches!(error, Error::Engine(_)), "{error}");

        let backend_under_a_file: PulumiConfig = toml::from_str(&format!(
            "work_dir = \"{}\"\nbackend = \"file://{}\"",
            fake.dir.join("w4").display(),
            fake.dir.join("afile/backend").display()
        ))
        .unwrap();
        let error = PulumiEngine::new(&backend_under_a_file).unwrap_err();
        assert!(
            matches!(error, Error::Engine(ref m) if m.contains("backend directory")),
            "{error}"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn a_dropped_reconciliation_kills_its_process() {
        let fake = Fake::new("timeout");
        let engine = fake.engine("[types]\nvm = \"gcp:compute:Instance\"");
        fake.write("up.sleep", "30");
        let id: ResourceId = "vm.a".parse().unwrap();
        let work = engine.reconcile(&id, Some(row("vm", "a", json!({"id": "a"}))));
        let outcome = tokio::time::timeout(Duration::from_millis(700), work).await;
        assert!(outcome.is_err(), "the reconciliation was abandoned on time");
        let pid = std::fs::read_to_string(fake.dir.join("up.pid"))
            .expect("the update had started")
            .trim()
            .to_owned();
        let mut alive = true;
        for _ in 0..40 {
            alive = std::process::Command::new("kill")
                .args(["-0", &pid])
                .status()
                .map(|status| status.success())
                .unwrap_or(false);
            if !alive {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(!alive, "the pulumi process {pid} was not killed");
    }

    #[test]
    fn events_edge_cases() {
        let text = concat!(
            r#"{"resOutputsEvent":{"metadata":{"op":"refresh","type":"random:index/randomPet:RandomPet"}}}"#,
            "\n",
            r#"{"resOutputsEvent":{"metadata":{"op":"read","type":"random:index/randomPet:RandomPet"}}}"#,
            "\n",
            r#"{"resOutputsEvent":{"metadata":{"op":"discard","type":"random:index/randomPet:RandomPet"}}}"#,
            "\n",
            r#"{"resOutputsEvent":{"metadata":{"op":"delete","type":"pulumi:pulumi:Stack"}}}"#,
            "\n",
            "\n",
            r#"{"summaryEvent":{"resourceChanges":{"same":"two"},"result":"succeeded"}}"#,
            "\n",
            r#"{"summaryEvent":{"resourceChanges":{"update":1},"result":"succeeded"}}"#,
            "\n",
            r#"{"diagnosticEvent":{"severity":"error","message":"   "}}"#,
            "\n",
            r#"{"diagnosticEvent":{"severity":"warning","message":"not an error"}}"#,
            "\n",
            r#"{"policyEvent":{"enforcementLevel":"mandatory"}}"#,
            "\n",
        );
        let report = read_events(text);
        assert!(
            !report.changed,
            "refresh, read, discard and the stack never count"
        );
        assert!(
            report.outputs_before.is_empty() && report.outputs_after.is_empty(),
            "a stack event without outputs gives none"
        );
        assert_eq!(
            report.changes.get("update"),
            Some(&1),
            "the last summary wins"
        );
        assert_eq!(report.changes.get("same"), None);
        assert_eq!(
            report.errors,
            ["policy unnamed: "],
            "a blank diagnostic is dropped, a bare policy event is kept"
        );
        assert_eq!(Report::default().summary(), "nothing to do");

        // An operation the engine does not know, or none at all, is taken
        // for a change rather than hidden: a wrong `changed=true` costs a
        // log line, a wrong `changed=false` hides work that was done.
        let unknown = read_events(r#"{"resOutputsEvent":{"metadata":{}}}"#);
        assert!(unknown.changed);
    }

    #[test]
    fn tidy_edge_cases() {
        assert_eq!(tidy(""), "");
        assert_eq!(tidy(" \t\n "), "");
        assert_eq!(
            tidy("\x1b[31"),
            "",
            "an unterminated colour code is swallowed"
        );
        assert_eq!(
            tidy("a\x1bb"),
            "a\x1bb",
            "an escape that starts no code is kept"
        );
    }

    #[test]
    fn toml_scalars_of_every_kind_become_json() {
        let table: toml::Table = toml::from_str(
            "f = 1.5\nb = false\nd = 1979-05-27T07:32:00Z\nn = -3\ns = \"x\"\na = [1, \"two\"]",
        )
        .unwrap();
        let json = toml_to_json(&table);
        assert_eq!(json["f"], 1.5);
        assert_eq!(json["b"], false);
        assert_eq!(json["d"], "1979-05-27T07:32:00Z");
        assert_eq!(json["n"], -3);
        assert_eq!(json["a"], json!([1, "two"]));
    }

    #[test]
    fn tidy_strips_colour_and_folds_whitespace() {
        assert_eq!(
            tidy("\x1b[31merror:\x1b[0m  two\n lines \n"),
            "error: two lines"
        );
        let long = "x".repeat(REASON_LENGTH + 10);
        assert_eq!(tidy(&long).chars().count(), REASON_LENGTH + 3);
    }

    #[test]
    fn toml_becomes_json_as_pulumi_reads_its_configuration() {
        let table: toml::Table = toml::from_str(
            "\"gcp:project\" = \"demo\"\n\"gcp:zone\" = \"europe-west1-b\"\nretries = 3\nnested = { a = [1, true] }",
        )
        .unwrap();
        assert_eq!(
            toml_to_json(&table),
            json!({"gcp:project": "demo", "gcp:zone": "europe-west1-b", "retries": 3, "nested": {"a": [1, true]}})
        );
    }

    #[test]
    fn a_program_cannot_be_written_under_a_file_and_a_summary_without_changes_leaves_them() {
        let file = std::env::temp_dir().join(format!("mcc-pulumi-file-{}", std::process::id()));
        std::fs::write(&file, "a file, not a directory").unwrap();
        let error = write_json(&file.join("Pulumi.yaml"), &json!({})).unwrap_err();
        assert!(error.contains("cannot be made"), "{error}");
        let _ = std::fs::remove_file(&file);

        let report = read_events(concat!(
            r#"{"summaryEvent":{"resourceChanges":{"create":2},"result":"succeeded"}}"#,
            "\n",
            r#"{"summaryEvent":{"result":"failed"}}"#,
        ));
        assert_eq!(report.changes.get("create"), Some(&2));
        assert_eq!(report.result.as_deref(), Some("failed"));
    }
}
