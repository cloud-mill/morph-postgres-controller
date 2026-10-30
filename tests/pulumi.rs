//! The controller against a real PostgreSQL, with Pulumi as the engine.
//!
//! These need a server with `wal_level = logical`, named by
//! `MPC_TEST_DATABASE_URL`, and the `pulumi` command on the path, and say
//! that they were skipped without either:
//!
//! ```sh
//! docker run -d --rm -p 5432:5432 -e POSTGRES_PASSWORD=morph postgres:16-alpine -c wal_level=logical
//! MPC_TEST_DATABASE_URL=postgres://postgres:morph@localhost/postgres cargo test --test pulumi
//! ```
//!
//! The stacks go to a file backend in a directory of the test's own, and
//! hold resources of Pulumi's `random` provider, so nothing outside this
//! machine is touched. The provider's plugin is fetched on the first run
//! and kept under `target/pulumi-home`, or under `MPC_TEST_PULUMI_HOME` if
//! that is set.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use morph::{ExecutorOptions, ProviderRegistry, ResourceId};
use morph_postgres_controller::config::Config;
use morph_postgres_controller::providers::Providers;
use morph_postgres_controller::scheduler::Origin;
use morph_postgres_controller::{Controller, Event, Options};
use tokio::sync::broadcast;

/// Tells the fixtures of one process apart.
static COUNTER: AtomicU64 = AtomicU64::new(0);

/// How long a test waits for an event. The first update fetches a plugin.
const PATIENCE: Duration = Duration::from_secs(240);

/// Everything one test makes: in the database, and on disk.
struct Fixture {
    url: String,
    client: tokio_postgres::Client,
    table: String,
    slot: String,
    publication: String,
    dirty: String,
    references: String,
    dir: PathBuf,
    home: PathBuf,
    project: String,
}

fn id(text: &str) -> ResourceId {
    text.parse().unwrap()
}

/// Whether `pulumi` can be run at all.
fn pulumi_available() -> bool {
    std::process::Command::new("pulumi")
        .arg("version")
        .env("PULUMI_SKIP_UPDATE_CHECK", "true")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .is_ok_and(|status| status.success())
}

impl Fixture {
    /// Connects, makes the test's table and its directory, or gives `None`
    /// if no database is named or Pulumi is not there.
    async fn new(test: &str) -> Option<Self> {
        let url = std::env::var("MPC_TEST_DATABASE_URL").ok()?;
        if !pulumi_available() {
            return None;
        }
        let (client, connection) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
            .await
            .expect("the database is reachable");
        tokio::spawn(connection);
        let unique = format!(
            "{}_{}_{}",
            test,
            std::process::id(),
            COUNTER.fetch_add(1, Ordering::SeqCst)
        );
        let dir = std::env::temp_dir().join(format!("mpc_{unique}"));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("passphrase"), "test\n").unwrap();
        let home = std::env::var("MPC_TEST_PULUMI_HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|_| {
                PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("target/pulumi-home")
            });
        let fixture = Self {
            url,
            client,
            table: format!("t_{unique}"),
            slot: format!("s_{unique}"),
            publication: format!("p_{unique}"),
            dirty: format!("d_{unique}"),
            references: format!("r_{unique}"),
            dir,
            home,
            project: format!("mpc-{}", unique.replace('_', "-")),
        };
        fixture
            .client
            .batch_execute(&format!(
                "CREATE TABLE {} (id text PRIMARY KEY, resource_type text NOT NULL, length int, prefix text, env text); \
                 ALTER TABLE {} REPLICA IDENTITY FULL",
                fixture.table, fixture.table
            ))
            .await
            .unwrap();
        Some(fixture)
    }

    /// A configuration over what this fixture made, with Pulumi as the
    /// engine and a file backend in the fixture's directory.
    fn config(&self, mapping_extra: &str) -> Config {
        let text = format!(
            r#"
            [postgres]
            url = "{url}"
            [replication]
            slot = "{slot}"
            publication = "{publication}"
            create_publication = true
            status_interval = "300ms"
            [checkpoint]
            table = "{dirty}"
            [references]
            table = "{references}"
            [reconcile]
            workers = 2
            queue_size = 100
            full_interval = "1h"
            timeout = "5m"
            [reconcile.retry]
            max_attempts = 2
            initial = "100ms"
            max = "500ms"
            [[mappings]]
            table = "{table}"
            type_column = "resource_type"
            {mapping_extra}
            [engine]
            kind = "pulumi"
            backend = "file://{dir}/backend"
            work_dir = "{dir}/work"
            home = "{home}"
            passphrase_file = "{dir}/passphrase"
            project = "{project}"
            stack_prefix = "t-"
            [engine.types]
            pet = "random:index:RandomPet"
            rid = "random:index:RandomId"
            [logging]
            metrics_interval = "1h"
            "#,
            url = self.url,
            slot = self.slot,
            publication = self.publication,
            dirty = self.dirty,
            references = self.references,
            table = self.table,
            dir = self.dir.display(),
            home = self.home.display(),
            project = self.project,
            mapping_extra = mapping_extra,
        );
        Config::parse(&text).expect("a valid configuration")
    }

    async fn start(&self, mapping_extra: &str) -> Controller {
        let providers = Providers::ready(ProviderRegistry::new(), ExecutorOptions::default(), None);
        let options = Options {
            acknowledge: true,
            full_run_at_start: false,
        };
        Controller::start_with(self.config(mapping_extra), providers, options)
            .await
            .expect("the controller starts")
    }

    /// The stacks of the test's project, by name, as `pulumi stack ls`
    /// lists them from the engine's project directory.
    fn stacks(&self) -> Vec<String> {
        self.listed()
            .iter()
            .filter_map(|stack| stack["name"].as_str().map(str::to_owned))
            .collect()
    }

    /// How many resources a stack holds, counting the stack itself and its
    /// provider, or `None` if there is no such stack.
    fn resources_in(&self, name: &str) -> Option<u64> {
        self.listed()
            .iter()
            .find(|stack| stack["name"].as_str() == Some(name))
            .and_then(|stack| stack["resourceCount"].as_u64())
    }

    /// What `pulumi stack ls --json` says about the test's project.
    fn listed(&self) -> Vec<serde_json::Value> {
        let output = std::process::Command::new("pulumi")
            .args(["stack", "ls", "--json"])
            .current_dir(self.dir.join("work/.project"))
            .env(
                "PULUMI_BACKEND_URL",
                format!("file://{}/backend", self.dir.display()),
            )
            .env("PULUMI_CONFIG_PASSPHRASE", "test")
            .env("PULUMI_HOME", &self.home)
            .env("PULUMI_SKIP_UPDATE_CHECK", "true")
            .output()
            .expect("pulumi runs");
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        serde_json::from_slice(&output.stdout).unwrap()
    }

    /// Removes everything the fixture made.
    async fn remove(&self) {
        let _ = self
            .client
            .batch_execute(&format!(
                "DROP TABLE IF EXISTS {}; DROP TABLE IF EXISTS {}; DROP TABLE IF EXISTS {}; DROP PUBLICATION IF EXISTS {}",
                self.table, self.dirty, self.references, self.publication
            ))
            .await;
        for _ in 0..50 {
            let dropped = self
                .client
                .batch_execute(&format!(
                    "SELECT pg_drop_replication_slot('{}') FROM pg_replication_slots WHERE slot_name = '{}'",
                    self.slot, self.slot
                ))
                .await;
            if dropped.is_ok() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// The controller's events, kept until they are asked for.
struct Events {
    receiver: broadcast::Receiver<Event>,
    kept: std::collections::VecDeque<Event>,
}

impl Events {
    fn of(controller: &Controller) -> Self {
        Self {
            receiver: controller.events(),
            kept: std::collections::VecDeque::new(),
        }
    }

    async fn wait_for(&mut self, wanted: impl Fn(&Event) -> bool) -> Event {
        if let Some(index) = self.kept.iter().position(&wanted) {
            return self.kept.remove(index).expect("just found");
        }
        tokio::time::timeout(PATIENCE, async {
            loop {
                match self.receiver.recv().await {
                    Ok(event) if wanted(&event) => return event,
                    Ok(event) => self.kept.push_back(event),
                    Err(broadcast::error::RecvError::Lagged(_)) => {}
                    Err(broadcast::error::RecvError::Closed) => {
                        panic!("the controller stopped sending events")
                    }
                }
            }
        })
        .await
        .expect("the event arrives in time")
    }

    /// Waits for the end of a reconciliation of `resource`: whether it
    /// succeeded and whether it changed anything.
    async fn reconciled(&mut self, resource: &str) -> (bool, bool) {
        let wanted = id(resource);
        let event = self
            .wait_for(
                |event| matches!(event, Event::Reconciled { resource, .. } if *resource == wanted),
            )
            .await;
        match event {
            Event::Reconciled { ok, changed, .. } => (ok, changed),
            _ => unreachable!(),
        }
    }
}

#[tokio::test]
async fn rows_become_stacks_and_go_with_them() {
    let Some(fixture) = Fixture::new("pulumi").await else {
        eprintln!("skipped: MPC_TEST_DATABASE_URL is not set, or pulumi is not on the path");
        return;
    };
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info")
        .with_test_writer()
        .try_init();
    let fixture = Arc::new(fixture);
    let outcome = tokio::spawn({
        let fixture = Arc::clone(&fixture);
        async move {
            let controller = fixture.start("").await;
            let mut events = Events::of(&controller);
            events
                .wait_for(|event| matches!(event, Event::Connected))
                .await;

            // A row makes a stack with the resource in it.
            fixture
                .client
                .execute(
                    &format!(
                        "INSERT INTO {} (id, resource_type, length, prefix) VALUES ('one', 'pet', 2, 'mpc')",
                        fixture.table
                    ),
                    &[],
                )
                .await
                .unwrap();
            assert_eq!(events.reconciled("pet.one").await, (true, true));
            assert_eq!(fixture.stacks(), ["t-pet.one"]);

            // A change that forces a replacement is a change.
            fixture
                .client
                .execute(
                    &format!("UPDATE {} SET length = 3 WHERE id = 'one'", fixture.table),
                    &[],
                )
                .await
                .unwrap();
            assert_eq!(events.reconciled("pet.one").await, (true, true));

            // A change that changes nothing is reconciled, and changes
            // nothing.
            fixture
                .client
                .execute(
                    &format!("UPDATE {} SET length = 3 WHERE id = 'one'", fixture.table),
                    &[],
                )
                .await
                .unwrap();
            assert_eq!(events.reconciled("pet.one").await, (true, false));

            // A kind with no Pulumi type fails on every attempt, and does
            // not hold up the others.
            fixture
                .client
                .execute(
                    &format!(
                        "INSERT INTO {} (id, resource_type, length) VALUES ('two', 'cat', 1)",
                        fixture.table
                    ),
                    &[],
                )
                .await
                .unwrap();
            for _attempt in 0..2 {
                assert_eq!(events.reconciled("cat.two").await, (false, false));
            }

            // The row goes, and the stack with it.
            fixture
                .client
                .execute(
                    &format!("DELETE FROM {} WHERE id = 'one'", fixture.table),
                    &[],
                )
                .await
                .unwrap();
            assert_eq!(events.reconciled("pet.one").await, (true, true));
            assert!(fixture.stacks().is_empty(), "{:?}", fixture.stacks());

            // A row that was never a stack is nothing to do.
            fixture
                .client
                .execute(
                    &format!("DELETE FROM {} WHERE id = 'two'", fixture.table),
                    &[],
                )
                .await
                .unwrap();
            assert_eq!(events.reconciled("cat.two").await, (true, false));

            controller.stop(Duration::from_secs(30)).await;
        }
    })
    .await;
    fixture.remove().await;
    if let Err(error) = outcome {
        std::panic::resume_unwind(error.into_panic());
    }
}

#[tokio::test]
async fn rows_of_a_group_share_one_stack_and_the_stack_goes_with_the_last() {
    let Some(fixture) = Fixture::new("pulumigroup").await else {
        eprintln!("skipped: MPC_TEST_DATABASE_URL is not set, or pulumi is not on the path");
        return;
    };
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info")
        .with_test_writer()
        .try_init();
    let fixture = Arc::new(fixture);
    let outcome = tokio::spawn({
        let fixture = Arc::clone(&fixture);
        async move {
            let controller = fixture.start("stack_column = \"env\"").await;
            let mut events = Events::of(&controller);
            events
                .wait_for(|event| matches!(event, Event::Connected))
                .await;

            // Two rows in one transaction are one reconciliation of their
            // stack, which then holds both.
            fixture
                .client
                .batch_execute(&format!(
                    "INSERT INTO {t} (id, resource_type, length, env) VALUES ('one', 'pet', 2, 'prod'); \
                     INSERT INTO {t} (id, resource_type, length, env) VALUES ('two', 'pet', 2, 'prod')",
                    t = fixture.table
                ))
                .await
                .unwrap();
            assert_eq!(events.reconciled("stack.prod").await, (true, true));
            assert_eq!(fixture.stacks(), ["t-prod"]);
            assert_eq!(fixture.resources_in("t-prod"), Some(4), "two pets, the stack and its provider");

            // A change to one row updates the stack; the other row is
            // untouched, which Pulumi reports as same.
            fixture
                .client
                .execute(
                    &format!("UPDATE {} SET length = 3 WHERE id = 'one'", fixture.table),
                    &[],
                )
                .await
                .unwrap();
            assert_eq!(events.reconciled("stack.prod").await, (true, true));

            // A row that goes leaves the program, and its resource is
            // deleted from the stack.
            fixture
                .client
                .execute(
                    &format!("DELETE FROM {} WHERE id = 'two'", fixture.table),
                    &[],
                )
                .await
                .unwrap();
            assert_eq!(events.reconciled("stack.prod").await, (true, true));
            assert_eq!(fixture.resources_in("t-prod"), Some(3), "one pet, the stack and its provider");

            // A row that moves to another group moves its resource: a new
            // stack appears with it.
            fixture
                .client
                .execute(
                    &format!("UPDATE {} SET env = 'staging' WHERE id = 'one'", fixture.table),
                    &[],
                )
                .await
                .unwrap();
            assert_eq!(events.reconciled("stack.staging").await, (true, true));
            assert_eq!(fixture.resources_in("t-staging"), Some(3));

            // The last row of a group takes its stack with it.
            fixture
                .client
                .execute(&format!("DELETE FROM {}", fixture.table), &[])
                .await
                .unwrap();
            assert_eq!(events.reconciled("stack.staging").await, (true, true));
            assert_eq!(fixture.resources_in("t-staging"), None);
            // The old group, prod, still holds the pet from before the move:
            // the update named the new stack, and the old is the periodic
            // pass's to find.
            assert_eq!(fixture.resources_in("t-prod"), Some(3));

            controller.stop(Duration::from_secs(30)).await;
        }
    })
    .await;
    fixture.remove().await;
    if let Err(error) = outcome {
        std::panic::resume_unwind(error.into_panic());
    }
}

#[tokio::test]
async fn single_and_grouped_tables_share_a_project_and_the_full_pass_removes_an_emptied_group() {
    let Some(fixture) = Fixture::new("pulumimix").await else {
        eprintln!("skipped: MPC_TEST_DATABASE_URL is not set, or pulumi is not on the path");
        return;
    };
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info")
        .with_test_writer()
        .try_init();
    let fixture = Arc::new(fixture);
    let grouped = format!("{}_g", fixture.table);
    fixture
        .client
        .batch_execute(&format!(
            "CREATE TABLE {g} (id text PRIMARY KEY, resource_type text NOT NULL, length int, env text); \
             ALTER TABLE {g} REPLICA IDENTITY FULL",
            g = grouped
        ))
        .await
        .unwrap();
    let mapping_extra = format!(
        "\n[[mappings]]\ntable = \"{grouped}\"\ntype_column = \"resource_type\"\nstack_column = \"env\""
    );
    let outcome = tokio::spawn({
        let fixture = Arc::clone(&fixture);
        let grouped = grouped.clone();
        let mapping_extra = mapping_extra.clone();
        async move {
            let controller = fixture.start(&mapping_extra).await;
            let mut events = Events::of(&controller);
            events
                .wait_for(|event| matches!(event, Event::Connected))
                .await;
            fixture
                .client
                .batch_execute(&format!(
                    "INSERT INTO {t} (id, resource_type, length) VALUES ('one', 'pet', 2); \
                     INSERT INTO {g} (id, resource_type, length, env) VALUES ('g1', 'pet', 2, 'prod')",
                    t = fixture.table,
                    g = grouped
                ))
                .await
                .unwrap();
            assert_eq!(events.reconciled("pet.one").await, (true, true));
            assert_eq!(events.reconciled("stack.prod").await, (true, true));
            let mut stacks = fixture.stacks();
            stacks.sort();
            assert_eq!(stacks, ["t-pet.one", "t-prod"]);

            // The grouped row moves on; prod keeps its pet until a full pass.
            fixture
                .client
                .execute(&format!("UPDATE {grouped} SET env = 'staging' WHERE id = 'g1'"), &[])
                .await
                .unwrap();
            assert_eq!(events.reconciled("stack.staging").await, (true, true));
            assert_eq!(fixture.resources_in("t-prod"), Some(3));
            controller.stop(Duration::from_secs(30)).await;

            // A restart with a full pass at start lists the stacks, finds
            // prod without rows, and removes it; the others are unchanged.
            let providers = Providers::ready(ProviderRegistry::new(), ExecutorOptions::default(), None);
            let options = Options {
                acknowledge: true,
                full_run_at_start: true,
            };
            let controller = Controller::start_with(fixture.config(&mapping_extra), providers, options)
                .await
                .expect("the controller starts again");
            let mut events = Events::of(&controller);
            events
                .wait_for(|event| matches!(event, Event::FullRun { resources } if *resources == 3))
                .await;
            assert_eq!(events.reconciled("stack.prod").await, (true, true));
            assert_eq!(events.reconciled("stack.staging").await, (true, false));
            assert_eq!(events.reconciled("pet.one").await, (true, false));
            let mut stacks = fixture.stacks();
            stacks.sort();
            assert_eq!(stacks, ["t-pet.one", "t-staging"]);

            fixture
                .client
                .batch_execute(&format!("DELETE FROM {}; DELETE FROM {grouped}", fixture.table))
                .await
                .unwrap();
            assert_eq!(events.reconciled("pet.one").await, (true, true));
            assert_eq!(events.reconciled("stack.staging").await, (true, true));
            assert!(fixture.stacks().is_empty(), "{:?}", fixture.stacks());
            controller.stop(Duration::from_secs(30)).await;
        }
    })
    .await;
    let _ = fixture
        .client
        .batch_execute(&format!("DROP TABLE IF EXISTS {grouped}"))
        .await;
    fixture.remove().await;
    if let Err(error) = outcome {
        std::panic::resume_unwind(error.into_panic());
    }
}

#[tokio::test]
async fn a_group_with_a_row_it_cannot_write_fails_whole_until_the_row_is_fixed() {
    let Some(fixture) = Fixture::new("pulumibad").await else {
        eprintln!("skipped: MPC_TEST_DATABASE_URL is not set, or pulumi is not on the path");
        return;
    };
    let fixture = Arc::new(fixture);
    let outcome = tokio::spawn({
        let fixture = Arc::clone(&fixture);
        async move {
            let controller = fixture.start("stack_column = \"env\"").await;
            let mut events = Events::of(&controller);
            events
                .wait_for(|event| matches!(event, Event::Connected))
                .await;
            fixture
                .client
                .batch_execute(&format!(
                    "INSERT INTO {t} (id, resource_type, length, env) VALUES ('good', 'pet', 2, 'prod'); \
                     INSERT INTO {t} (id, resource_type, length, env) VALUES ('bad', 'cat', 2, 'prod')",
                    t = fixture.table
                ))
                .await
                .unwrap();
            // max_attempts is 2: both attempts fail, nothing is made.
            assert_eq!(events.reconciled("stack.prod").await, (false, false));
            assert_eq!(events.reconciled("stack.prod").await, (false, false));
            assert!(fixture.stacks().is_empty(), "{:?}", fixture.stacks());
            assert_eq!(controller.unhealthy().await, [id("stack.prod")]);

            // Fixing the row fixes the group.
            fixture
                .client
                .execute(
                    &format!("UPDATE {} SET resource_type = 'pet' WHERE id = 'bad'", fixture.table),
                    &[],
                )
                .await
                .unwrap();
            assert_eq!(events.reconciled("stack.prod").await, (true, true));
            assert_eq!(fixture.resources_in("t-prod"), Some(4));
            assert!(controller.unhealthy().await.is_empty());

            fixture
                .client
                .execute(&format!("DELETE FROM {}", fixture.table), &[])
                .await
                .unwrap();
            assert_eq!(events.reconciled("stack.prod").await, (true, true));
            controller.stop(Duration::from_secs(30)).await;
        }
    })
    .await;
    fixture.remove().await;
    if let Err(error) = outcome {
        std::panic::resume_unwind(error.into_panic());
    }
}

#[tokio::test]
async fn a_delete_that_cannot_name_its_stack_is_left_to_the_full_pass() {
    let Some(fixture) = Fixture::new("pulumiident").await else {
        eprintln!("skipped: MPC_TEST_DATABASE_URL is not set, or pulumi is not on the path");
        return;
    };
    let fixture = Arc::new(fixture);
    // A table whose replica identity is the default: a delete carries only
    // the key, so the stack column is not sent.
    let plain = format!("{}_plain", fixture.table);
    fixture
        .client
        .batch_execute(&format!(
            "CREATE TABLE {plain} (id text PRIMARY KEY, resource_type text NOT NULL, length int, env text)"
        ))
        .await
        .unwrap();
    let mapping_extra = format!(
        "\n[[mappings]]\ntable = \"{plain}\"\ntype_column = \"resource_type\"\nstack_column = \"env\""
    );
    let outcome = tokio::spawn({
        let fixture = Arc::clone(&fixture);
        let plain = plain.clone();
        let mapping_extra = mapping_extra.clone();
        async move {
            let controller = fixture.start(&mapping_extra).await;
            let mut events = Events::of(&controller);
            events
                .wait_for(|event| matches!(event, Event::Connected))
                .await;
            fixture
                .client
                .execute(
                    &format!("INSERT INTO {plain} (id, resource_type, length, env) VALUES ('p1', 'pet', 2, 'prod')"),
                    &[],
                )
                .await
                .unwrap();
            assert_eq!(events.reconciled("stack.prod").await, (true, true));

            fixture
                .client
                .execute(&format!("DELETE FROM {plain} WHERE id = 'p1'"), &[])
                .await
                .unwrap();
            // The delete is a mapping error, not a reconciliation.
            events
                .wait_for(|event| matches!(event, Event::Transaction { .. }))
                .await;
            let metrics = controller.metrics();
            let mut errors = 0;
            for _ in 0..50 {
                errors = morph_postgres_controller::observe::Metrics::get(&metrics.mapping_errors);
                if errors > 0 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            assert_eq!(errors, 1, "the delete could not name its stack");
            assert_eq!(fixture.resources_in("t-prod"), Some(3), "untouched so far");
            controller.stop(Duration::from_secs(30)).await;

            // The full pass at the next start finds the stack without rows.
            let providers = Providers::ready(ProviderRegistry::new(), ExecutorOptions::default(), None);
            let options = Options {
                acknowledge: true,
                full_run_at_start: true,
            };
            let controller = Controller::start_with(fixture.config(&mapping_extra), providers, options)
                .await
                .expect("the controller starts again");
            let mut events = Events::of(&controller);
            assert_eq!(events.reconciled("stack.prod").await, (true, true));
            assert!(fixture.stacks().is_empty(), "{:?}", fixture.stacks());
            controller.stop(Duration::from_secs(30)).await;
        }
    })
    .await;
    let _ = fixture
        .client
        .batch_execute(&format!("DROP TABLE IF EXISTS {plain}"))
        .await;
    fixture.remove().await;
    if let Err(error) = outcome {
        std::panic::resume_unwind(error.into_panic());
    }
}

impl Fixture {
    /// The outputs of a stack, as `pulumi stack output --json` gives them.
    fn outputs(&self, stack: &str) -> serde_json::Value {
        let output = std::process::Command::new("pulumi")
            .args([
                "stack",
                "output",
                "--json",
                "--show-secrets",
                "--stack",
                stack,
            ])
            .current_dir(self.dir.join("work/.project"))
            .env(
                "PULUMI_BACKEND_URL",
                format!("file://{}/backend", self.dir.display()),
            )
            .env("PULUMI_CONFIG_PASSPHRASE", "test")
            .env("PULUMI_HOME", &self.home)
            .env("PULUMI_SKIP_UPDATE_CHECK", "true")
            .output()
            .expect("pulumi runs");
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        serde_json::from_slice(&output.stdout).unwrap()
    }

    /// The random id in a stack's state, as `pulumi stack export` gives
    /// it, or `None` if there is no such stack or no random id in it.
    fn random_id_in(&self, stack: &str) -> Option<serde_json::Value> {
        let output = std::process::Command::new("pulumi")
            .args(["stack", "export", "--stack", stack])
            .current_dir(self.dir.join("work/.project"))
            .env(
                "PULUMI_BACKEND_URL",
                format!("file://{}/backend", self.dir.display()),
            )
            .env("PULUMI_CONFIG_PASSPHRASE", "test")
            .env("PULUMI_HOME", &self.home)
            .env("PULUMI_SKIP_UPDATE_CHECK", "true")
            .output()
            .expect("pulumi runs");
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            assert!(stderr.contains("no stack named"), "{stderr}");
            return None;
        }
        let exported: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        exported["deployment"]["resources"]
            .as_array()?
            .iter()
            .find(|resource| resource["type"] == "random:index/randomId:RandomId")
            .cloned()
    }

    /// The program the engine last wrote for a stack.
    fn program(&self, stack: &str) -> serde_json::Value {
        let path = self.dir.join("work").join(stack).join("Pulumi.yaml");
        serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap()
    }
}

#[tokio::test]
async fn a_row_that_refers_to_another_stack_takes_its_output_and_follows_a_change() {
    let Some(fixture) = Fixture::new("pulumirefs").await else {
        eprintln!("skipped: MPC_TEST_DATABASE_URL is not set, or pulumi is not on the path");
        return;
    };
    let fixture = Arc::new(fixture);
    // Pets whose prefix is the id of another pet: a foreign key into the
    // same table.
    let pets = format!("{}_pets", fixture.table);
    fixture
        .client
        .batch_execute(&format!(
            "CREATE TABLE {pets} (id text PRIMARY KEY, resource_type text NOT NULL, length int, \
                                  prefix text REFERENCES {pets}(id), env text); \
             ALTER TABLE {pets} REPLICA IDENTITY FULL"
        ))
        .await
        .unwrap();
    let mapping_extra =
        format!("\n[[mappings]]\ntable = \"{pets}\"\ntype_column = \"resource_type\"");
    let outcome = tokio::spawn({
        let fixture = Arc::clone(&fixture);
        let pets = pets.clone();
        async move {
            let controller = fixture.start(&mapping_extra).await;
            let mut events = Events::of(&controller);
            events
                .wait_for(|event| matches!(event, Event::Connected))
                .await;

            // Both rows in one transaction. The child is handed the base's
            // id, which Pulumi only knows once the base exists.
            fixture
                .client
                .batch_execute(&format!(
                    "INSERT INTO {pets} (id, resource_type, length) VALUES ('base', 'pet', 2); \
                     INSERT INTO {pets} (id, resource_type, length, prefix) VALUES ('child', 'pet', 1, 'base')"
                ))
                .await
                .unwrap();
            assert_eq!(events.reconciled("pet.base").await, (true, true));
            assert_eq!(events.reconciled("pet.child").await, (true, true));
            let base_id = fixture.outputs("t-pet.base")["pet.base.id"]
                .as_str()
                .expect("the base exports its id")
                .to_owned();
            assert_eq!(
                fixture.program("t-pet.base")["outputs"]["pet.base.id"],
                "${base.id}"
            );
            let child = fixture.program("t-pet.child");
            assert_eq!(
                child["resources"]["child"]["properties"]["prefix"], base_id,
                "the child's program holds the base's id, not the row id"
            );
            assert_eq!(
                child["outputs"]["pet.child.id"], "${child.id}",
                "pets refer to pets, so every pet exports its id"
            );
            assert_eq!(
                morph_postgres_controller::observe::Metrics::get(
                    &controller.metrics().reconciliations_failed
                ),
                0
            );

            // A longer name replaces the base, which gives it a new id. The
            // child is marked and follows.
            fixture
                .client
                .execute(
                    &format!("UPDATE {pets} SET length = 3 WHERE id = 'base'"),
                    &[],
                )
                .await
                .unwrap();
            assert_eq!(events.reconciled("pet.base").await, (true, true));
            let event = events
                .wait_for(|event| {
                    matches!(event, Event::Reconciled { resource, origin: Origin::Propagated, .. } if *resource == id("pet.child"))
                })
                .await;
            assert!(matches!(event, Event::Reconciled { ok: true, changed: true, .. }), "{event:?}");
            let new_base_id = fixture.outputs("t-pet.base")["pet.base.id"]
                .as_str()
                .unwrap()
                .to_owned();
            assert_ne!(new_base_id, base_id);
            assert_eq!(
                fixture.program("t-pet.child")["resources"]["child"]["properties"]["prefix"],
                new_base_id
            );

            // Both rows go in one statement. The base's stack waits for the
            // child's to go first, or finds it gone already.
            fixture
                .client
                .execute(
                    &format!("DELETE FROM {pets} WHERE id IN ('base', 'child')"),
                    &[],
                )
                .await
                .unwrap();
            assert_eq!(events.reconciled("pet.child").await, (true, true));
            assert_eq!(events.reconciled("pet.base").await, (true, true));
            assert!(fixture.stacks().is_empty(), "{:?}", fixture.stacks());
            assert_eq!(
                morph_postgres_controller::observe::Metrics::get(
                    &controller.metrics().reconciliations_failed
                ),
                0
            );
            controller.stop(Duration::from_secs(30)).await;
        }
    })
    .await;
    let _ = fixture
        .client
        .batch_execute(&format!("DROP TABLE IF EXISTS {pets}"))
        .await;
    fixture.remove().await;
    if let Err(error) = outcome {
        std::panic::resume_unwind(error.into_panic());
    }
}

#[tokio::test]
async fn rows_of_one_stack_are_linked_in_the_program_and_a_stack_output_serves_outsiders() {
    let Some(fixture) = Fixture::new("pulumilinks").await else {
        eprintln!("skipped: MPC_TEST_DATABASE_URL is not set, or pulumi is not on the path");
        return;
    };
    let fixture = Arc::new(fixture);
    let grouped = format!("{}_grp", fixture.table);
    let outside = format!("{}_out", fixture.table);
    fixture
        .client
        .batch_execute(&format!(
            "CREATE TABLE {grouped} (id text PRIMARY KEY, resource_type text NOT NULL, length int, \
                                     prefix text REFERENCES {grouped}(id)); \
             CREATE TABLE {outside} (id text PRIMARY KEY, resource_type text NOT NULL, length int, \
                                     prefix text REFERENCES {grouped}(id)); \
             ALTER TABLE {grouped} REPLICA IDENTITY FULL; \
             ALTER TABLE {outside} REPLICA IDENTITY FULL"
        ))
        .await
        .unwrap();
    let mapping_extra = format!(
        "\n[[mappings]]\ntable = \"{grouped}\"\ntype_column = \"resource_type\"\nstack = \"pets\"\n\
         [[mappings]]\ntable = \"{outside}\"\ntype_column = \"resource_type\""
    );
    let outcome = tokio::spawn({
        let fixture = Arc::clone(&fixture);
        let grouped = grouped.clone();
        let outside = outside.clone();
        async move {
            let controller = fixture.start(&mapping_extra).await;
            let mut events = Events::of(&controller);
            events
                .wait_for(|event| matches!(event, Event::Connected))
                .await;

            // Two rows of one stack, one referring to the other: one update,
            // with the reference as an expression Pulumi resolves itself.
            fixture
                .client
                .batch_execute(&format!(
                    "INSERT INTO {grouped} (id, resource_type, length) VALUES ('base', 'pet', 2); \
                     INSERT INTO {grouped} (id, resource_type, length, prefix) VALUES ('child', 'pet', 1, 'base')"
                ))
                .await
                .unwrap();
            assert_eq!(events.reconciled("stack.pets").await, (true, true));
            let program = fixture.program("t-pets");
            assert_eq!(
                program["resources"]["pet_child"]["properties"]["prefix"],
                "${pet_base.id}"
            );
            assert!(program["resources"]["pet_base"]["properties"].get("prefix").is_none());
            let outputs = fixture.outputs("t-pets");
            let base_id = outputs["pet.base.id"].as_str().unwrap().to_owned();
            let child_id = outputs["pet.child.id"].as_str().unwrap().to_owned();
            assert!(
                child_id.starts_with(&format!("{base_id}-")),
                "{child_id} is prefixed with {base_id}"
            );
            assert_eq!(fixture.resources_in("t-pets"), Some(4));

            // A row of another table, in a stack of its own, refers to a
            // row of the group: it reads the group's output.
            fixture
                .client
                .execute(
                    &format!("INSERT INTO {outside} (id, resource_type, length, prefix) VALUES ('far', 'pet', 1, 'child')"),
                    &[],
                )
                .await
                .unwrap();
            assert_eq!(events.reconciled("pet.far").await, (true, true));
            assert_eq!(
                fixture.program("t-pet.far")["resources"]["far"]["properties"]["prefix"],
                child_id
            );

            // The child and the outsider go in one transaction. The group
            // may only drop the child once the outsider is gone.
            fixture
                .client
                .batch_execute(&format!(
                    "DELETE FROM {outside} WHERE id = 'far'; \
                     DELETE FROM {grouped} WHERE id = 'child'"
                ))
                .await
                .unwrap();
            assert_eq!(events.reconciled("pet.far").await, (true, true));
            assert_eq!(events.reconciled("stack.pets").await, (true, true));
            assert_eq!(fixture.resources_in("t-pets"), Some(3));
            assert_eq!(fixture.stacks(), ["t-pets"]);
            assert_eq!(
                morph_postgres_controller::observe::Metrics::get(
                    &controller.metrics().reconciliations_failed
                ),
                0
            );
            controller.stop(Duration::from_secs(30)).await;
        }
    })
    .await;
    let _ = fixture
        .client
        .batch_execute(&format!(
            "DROP TABLE IF EXISTS {outside}; DROP TABLE IF EXISTS {grouped}"
        ))
        .await;
    fixture.remove().await;
    if let Err(error) = outcome {
        std::panic::resume_unwind(error.into_panic());
    }
}

#[tokio::test]
async fn the_controller_starts_from_its_configuration_alone_with_pulumi_as_the_engine() {
    let Some(fixture) = Fixture::new("pulumi_start").await else {
        eprintln!("skipped: MPC_TEST_DATABASE_URL is not set, or pulumi is not on the path");
        return;
    };
    let fixture = Arc::new(fixture);
    let outcome = tokio::spawn({
        let fixture = Arc::clone(&fixture);
        async move {
            let controller = Controller::start(fixture.config(""))
                .await
                .expect("the controller starts from the configuration alone");
            let mut events = Events::of(&controller);
            // The full reconciliation at start finds no stacks and no rows.
            events
                .wait_for(|event| matches!(event, Event::FullRun { resources: 0 }))
                .await;
            fixture
                .client
                .execute(
                    &format!(
                        "INSERT INTO {} (id, resource_type, length, prefix) VALUES ('solo', 'pet', 2, 'mpc')",
                        fixture.table
                    ),
                    &[],
                )
                .await
                .unwrap();
            assert_eq!(events.reconciled("pet.solo").await, (true, true));
            assert_eq!(fixture.stacks(), ["t-pet.solo"]);
            controller.stop(Duration::from_secs(60)).await;
        }
    })
    .await;
    fixture.remove().await;
    if let Err(error) = outcome {
        std::panic::resume_unwind(error.into_panic());
    }
}

#[tokio::test]
async fn a_row_with_an_import_id_takes_charge_of_what_pulumi_made_before() {
    let Some(fixture) = Fixture::new("adopt").await else {
        eprintln!("skipped: MPC_TEST_DATABASE_URL is not set, or pulumi is not on the path");
        return;
    };
    let fixture = Arc::new(fixture);
    let outcome = tokio::spawn({
        let fixture = Arc::clone(&fixture);
        async move {
            fixture
                .client
                .batch_execute(&format!(
                    "ALTER TABLE {} ADD COLUMN byte_length int, ADD COLUMN object text",
                    fixture.table
                ))
                .await
                .unwrap();
            let controller = fixture
                .start("import_column = \"object\"\n[mappings.lifecycle]\nprevent_destroy = true")
                .await;
            let mut events = Events::of(&controller);
            events
                .wait_for(|event| matches!(event, Event::Connected))
                .await;

            // Something made before, by another stack: what Terraform
            // would have left behind.
            fixture
                .client
                .execute(
                    &format!(
                        "INSERT INTO {} (id, resource_type, byte_length) VALUES ('a', 'rid', 4)",
                        fixture.table
                    ),
                    &[],
                )
                .await
                .unwrap();
            assert_eq!(events.reconciled("rid.a").await, (true, true));
            let made = fixture.random_id_in("t-rid.a").expect("a random id in the stack");
            let id = made["id"].as_str().expect("an id").to_owned();

            // A row naming it by that id takes charge of it rather than
            // making another: the program asks Pulumi to import, and the
            // stack ends up holding the same id.
            fixture
                .client
                .execute(
                    &format!(
                        "INSERT INTO {} (id, resource_type, byte_length, object) VALUES ('b', 'rid', 4, $1)",
                        fixture.table
                    ),
                    &[&id],
                )
                .await
                .unwrap();
            let (ok, _) = events.reconciled("rid.b").await;
            assert!(ok, "the import succeeds");
            assert_eq!(
                fixture.program("t-rid.b")["resources"]["b"]["options"]["import"],
                id
            );
            let adopted = fixture.random_id_in("t-rid.b").expect("the stack holds the random id");
            assert_eq!(adopted["id"].as_str(), Some(id.as_str()));
            assert_eq!(adopted["inputs"]["byteLength"], 4);

            // From then on the row drives it, and the option is not set
            // again.
            fixture
                .client
                .execute(
                    &format!("UPDATE {} SET byte_length = 4 WHERE id = 'b'", fixture.table),
                    &[],
                )
                .await
                .unwrap();
            assert_eq!(events.reconciled("rid.b").await, (true, false));
            assert!(
                fixture.program("t-rid.b")["resources"]["b"]["options"]
                    .get("import")
                    .is_none()
            );

            // A row that describes the object otherwise than it is fails:
            // Pulumi records the import and then refuses the difference.
            // The retry finds the stack made, plans the replacement the
            // difference calls for, and prevent_destroy refuses that too.
            // The object is on record and untouched, and the row is
            // unhealthy: the gate holds.
            fixture
                .client
                .execute(
                    &format!(
                        "INSERT INTO {} (id, resource_type, byte_length, object) VALUES ('c', 'rid', 8, $1)",
                        fixture.table
                    ),
                    &[&id],
                )
                .await
                .unwrap();
            assert_eq!(events.reconciled("rid.c").await, (false, false));
            assert_eq!(events.reconciled("rid.c").await, (false, false));
            assert!(
                controller
                    .unhealthy()
                    .await
                    .iter()
                    .any(|resource| resource.to_string() == "rid.c")
            );
            let kept = fixture.random_id_in("t-rid.c").expect("the import was recorded");
            assert_eq!(kept["id"].as_str(), Some(id.as_str()));
            assert!(
                fixture.program("t-rid.c")["resources"]["c"]["options"]
                    .get("import")
                    .is_none(),
                "the retry does not ask to import again"
            );

            controller.stop(Duration::from_secs(30)).await;
        }
    })
    .await;
    fixture.remove().await;
    if let Err(error) = outcome {
        std::panic::resume_unwind(error.into_panic());
    }
}
