//! The program itself: its commands, its exit codes and what `check`
//! prints.
//!
//! `check` needs a server with `wal_level = logical`, named by
//! `MPC_TEST_DATABASE_URL`, and says that it was skipped without one. The
//! other tests need nothing.

use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// How long a test waits for the program to say something.
const PATIENCE: Duration = Duration::from_secs(90);

fn binary() -> Command {
    Command::new(env!("CARGO_BIN_EXE_morph-postgres-controller"))
}

fn scratch(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("mpc-cli-{}-{name}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

#[test]
fn help_lists_the_commands() {
    let output = binary().arg("--help").output().unwrap();
    assert!(output.status.success());
    let text = String::from_utf8_lossy(&output.stdout);
    assert!(text.contains("check"), "{text}");
    assert!(text.contains("run"), "{text}");
    assert!(text.contains("--config"), "{text}");
}

#[test]
fn a_missing_or_invalid_configuration_fails_before_anything_runs() {
    let dir = scratch("config");
    let output = binary()
        .args([
            "--config",
            dir.join("absent.toml").to_str().unwrap(),
            "check",
        ])
        .output()
        .unwrap();
    assert!(!output.status.success());
    let text = String::from_utf8_lossy(&output.stderr);
    assert!(
        text.contains("absent.toml") || text.contains("configuration"),
        "{text}"
    );

    let invalid = dir.join("invalid.toml");
    std::fs::write(&invalid, "[postgres]\nurl = \"postgres://x@localhost/x\"\n").unwrap();
    let output = binary()
        .args(["--config", invalid.to_str().unwrap(), "run"])
        .output()
        .unwrap();
    assert!(!output.status.success());
    let text = String::from_utf8_lossy(&output.stderr);
    assert!(text.contains("configuration"), "{text}");
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn check_describes_the_morph_setup() {
    let Ok(url) = std::env::var("MPC_TEST_DATABASE_URL") else {
        eprintln!("skipped: MPC_TEST_DATABASE_URL is not set");
        return;
    };
    let dir = scratch("check-morph");
    let path = dir.join("controller.toml");
    std::fs::write(
        &path,
        format!(
            r#"
            [postgres]
            url = "{url}"
            [replication]
            slot = "mpc_cli_check_{pid}"
            publication = "mpc_cli_check_{pid}"
            [[mappings]]
            table = "things"
            type_column = "kind"
            [[providers]]
            kind = "mock"
            "#,
            pid = std::process::id()
        ),
    )
    .unwrap();
    let output = binary()
        .args(["--config", path.to_str().unwrap(), "check"])
        .output()
        .unwrap();
    let text = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success(),
        "{text}\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(text.contains("wal_level = logical"), "{text}");

    // The environment overrides the file: a bogus URL in the file, the real
    // one in MPC_DATABASE_URL, and a debug level that reaches the log.
    let overridden = dir.join("overridden.toml");
    std::fs::write(
        &overridden,
        std::fs::read_to_string(&path)
            .unwrap()
            .replace(&url, "postgres://nobody@127.0.0.1:1/nothing"),
    )
    .unwrap();
    let output = binary()
        .args(["--config", overridden.to_str().unwrap(), "check"])
        .env("MPC_DATABASE_URL", &url)
        .env("MPC_LOG", "debug")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(text.contains("does not exist, and would be made"), "{text}");
    assert!(
        text.contains("does not exist, and would not be made"),
        "{text}"
    );
    assert!(
        text.contains("mapping       public.things -> from column kind, named by id"),
        "{text}"
    );
    assert!(text.contains("engine        morph"), "{text}");
    assert!(text.contains("providers     "), "{text}");
    assert!(text.contains("morph state   Postgres"), "{text}");
    assert!(
        text.contains("references    0 between rows, kept in morph_controller_references"),
        "{text}"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn check_describes_the_pulumi_setup_with_its_groupings() {
    let Ok(url) = std::env::var("MPC_TEST_DATABASE_URL") else {
        eprintln!("skipped: MPC_TEST_DATABASE_URL is not set");
        return;
    };
    let dir = scratch("check-pulumi");
    // A stand-in for pulumi, so that the check needs no backend.
    let fake = dir.join("pulumi");
    std::fs::write(
        &fake,
        "#!/bin/sh\ncase \"$1\" in version) echo v0.0.0-fake;; stack) echo '[{\"name\":\"fleet\"},{\"name\":\"vm.a\"},{\"name\":\"bad name\"}]';; esac\n",
    )
    .unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&fake, std::fs::Permissions::from_mode(0o755)).unwrap();
    }
    let path = dir.join("controller.toml");
    std::fs::write(
        &path,
        format!(
            r#"
            [postgres]
            url = "{url}"
            [replication]
            slot = "mpc_cli_pulumi_{pid}"
            publication = "mpc_cli_pulumi_{pid}"
            [[mappings]]
            table = "vms"
            resource_type = "vm"
            stack = "fleet"
            [[mappings]]
            table = "things"
            type_column = "kind"
            stack_column = "env"
            [engine]
            kind = "pulumi"
            binary = "{fake}"
            work_dir = "{work}"
            backend = "file://{backend}"
            [engine.types]
            vm = "gcp:compute:Instance"
            "#,
            pid = std::process::id(),
            fake = fake.display(),
            work = dir.join("work").display(),
            backend = dir.join("backend").display(),
        ),
    )
    .unwrap();
    let output = binary()
        .args(["--config", path.to_str().unwrap(), "check"])
        .output()
        .unwrap();
    let text = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success(),
        "{text}\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        text.contains("vm (gcp:compute:Instance), named by id, all in the stack fleet"),
        "{text}"
    );
    assert!(
        text.contains("from column kind, named by id, in the stack named by column env"),
        "{text}"
    );
    assert!(
        text.contains(
            "engine        pulumi v0.0.0-fake, project morph-controller, backend file://"
        ),
        "{text}"
    );
    assert!(
        text.contains("stacks        3 in the project, 2 of them this controller's"),
        "{text}"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// Drops `slot` once the server has noticed that nothing holds it.
async fn drop_slot(client: &tokio_postgres::Client, slot: &str) {
    for _ in 0..100 {
        let dropped = client
            .execute(
                "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = $1",
                &[&slot],
            )
            .await;
        if dropped.is_ok() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("the slot {slot} could not be dropped");
}

/// `url` with a socket that is not there as its first host and the real
/// server as its second: the ordinary connection reaches the server, the
/// stream cannot.
fn socket_first(url: &str) -> String {
    let config: tokio_postgres::Config = url.parse().unwrap();
    let host = match &config.get_hosts()[0] {
        tokio_postgres::config::Host::Tcp(host) => host.clone(),
        other => panic!("the test database is not on a TCP host: {other:?}"),
    };
    let port = config.get_ports().first().copied().unwrap_or(5432);
    let user = config.get_user().unwrap_or("postgres");
    let password = config
        .get_password()
        .map(|password| format!(" password={}", String::from_utf8_lossy(password)))
        .unwrap_or_default();
    let database = config.get_dbname().unwrap_or(user);
    // The key-value form, because in the URL form a list of hosts cannot
    // hold a socket.
    format!(
        "host=/nonexistent/socket,{host} port=5432,{port} user={user}{password} dbname={database}"
    )
}

#[tokio::test]
async fn check_describes_what_exists_and_the_references_between_the_tables() {
    let Ok(url) = std::env::var("MPC_TEST_DATABASE_URL") else {
        eprintln!("skipped: MPC_TEST_DATABASE_URL is not set");
        return;
    };
    let (client, connection) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
        .await
        .unwrap();
    tokio::spawn(connection);
    let pid = std::process::id();
    let networks = format!("cli_networks_{pid}");
    let servers = format!("cli_servers_{pid}");
    let name = format!("mpc_cli_exists_{pid}");
    client
        .batch_execute(&format!(
            "CREATE TABLE {networks} (id text PRIMARY KEY, cidr text); \
             CREATE TABLE {servers} (id text PRIMARY KEY, network_id text REFERENCES {networks}(id)); \
             CREATE PUBLICATION {name} FOR TABLE {networks}, {servers}"
        ))
        .await
        .unwrap();
    client
        .execute(
            "SELECT pg_create_logical_replication_slot($1, 'pgoutput')",
            &[&name],
        )
        .await
        .unwrap();
    let dir = scratch("check-exists");
    let path = dir.join("controller.toml");
    std::fs::write(
        &path,
        format!(
            r#"
            [postgres]
            url = "{url}"
            [replication]
            slot = "{name}"
            publication = "{name}"
            [[mappings]]
            table = "{networks}"
            resource_type = "network"
            [[mappings]]
            table = "{servers}"
            resource_type = "server"
            [[providers]]
            kind = "mock"
            "#
        ),
    )
    .unwrap();
    let output = binary()
        .args(["--config", path.to_str().unwrap(), "check"])
        .output()
        .unwrap();
    let text = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success(),
        "{text}\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        text.contains(&format!("publication   {name} exists")),
        "{text}"
    );
    assert!(
        text.contains(&format!("slot          {name} exists")),
        "{text}"
    );
    assert!(text.contains("bytes behind, not active"), "{text}");
    assert!(
        text.contains(&format!(
            "mapping       public.{networks} -> network, named by id"
        )),
        "{text}"
    );
    assert!(
        text.contains(&format!(
            "reference     public.{servers}.network_id -> public.{networks} (id), from the constraint {servers}_network_id_fkey"
        )),
        "{text}"
    );
    assert!(text.contains("references    1 between rows"), "{text}");

    // Names that do not exist, with the publication to be made and the
    // slot not: the check says which is which, and makes neither.
    let absent = dir.join("absent.toml");
    std::fs::write(
        &absent,
        std::fs::read_to_string(&path)
            .unwrap()
            .replace(
                &format!("publication = \"{name}\""),
                &format!("publication = \"{name}_absent\"\ncreate_publication = true\ncreate_slot = false"),
            )
            .replace(&format!("slot = \"{name}\""), &format!("slot = \"{name}_absent\"")),
    )
    .unwrap();
    let output = binary()
        .args(["--config", absent.to_str().unwrap(), "check"])
        .output()
        .unwrap();
    let text = String::from_utf8_lossy(&output.stdout);
    assert!(output.status.success(), "{text}");
    assert!(
        text.contains(&format!(
            "publication   {name}_absent does not exist, and would be made"
        )),
        "{text}"
    );
    assert!(
        text.contains(&format!(
            "slot          {name}_absent does not exist, and would not be made"
        )),
        "{text}"
    );
    let made: i64 = client
        .query_one(
            "SELECT count(*) FROM pg_publication WHERE pubname = $1",
            &[&format!("{name}_absent")],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(made, 0, "the check makes nothing");
    drop_slot(&client, &name).await;
    client
        .batch_execute(&format!(
            "DROP PUBLICATION {name}; DROP TABLE {servers}; DROP TABLE {networks}"
        ))
        .await
        .unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn check_refuses_a_certificate_file_that_is_not_there() {
    let dir = scratch("check-ca");
    let path = dir.join("controller.toml");
    std::fs::write(
        &path,
        format!(
            r#"
            [postgres]
            url = "postgres://nobody@127.0.0.1:1/nothing"
            ca_certificate = "{}"
            [replication]
            slot = "s"
            publication = "p"
            [[mappings]]
            table = "things"
            resource_type = "thing"
            [[providers]]
            kind = "mock"
            "#,
            dir.join("absent.pem").display()
        ),
    )
    .unwrap();
    let output = binary()
        .args(["--config", path.to_str().unwrap(), "check"])
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(1));
    let text = String::from_utf8_lossy(&output.stderr);
    assert!(text.contains("configuration"), "{text}");
    let _ = std::fs::remove_dir_all(&dir);
}

#[cfg(unix)]
#[test]
fn run_runs_until_it_is_told_to_stop() {
    let Ok(url) = std::env::var("MPC_TEST_DATABASE_URL") else {
        eprintln!("skipped: MPC_TEST_DATABASE_URL is not set");
        return;
    };
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let client = runtime.block_on(async {
        let (client, connection) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
            .await
            .unwrap();
        tokio::spawn(connection);
        client
    });
    let pid = std::process::id();
    let table = format!("cli_run_{pid}");
    let name = format!("mpc_cli_run_{pid}");
    runtime
        .block_on(client.batch_execute(&format!(
            "CREATE TABLE {table} (id text PRIMARY KEY, name text, cidr text)"
        )))
        .unwrap();
    let dir = scratch("run");
    let path = dir.join("controller.toml");
    std::fs::write(
        &path,
        format!(
            r#"
            [postgres]
            url = "{url}"
            [replication]
            slot = "{name}"
            publication = "{name}"
            create_publication = true
            [checkpoint]
            mode = "memory"
            [[mappings]]
            table = "{table}"
            resource_type = "network"
            [[providers]]
            kind = "mock"
            [logging]
            format = "json"
            "#
        ),
    )
    .unwrap();
    // No command: `run` is what the program does by default.
    let mut child = binary()
        .args(["--config", path.to_str().unwrap()])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let lines: Arc<Mutex<Vec<String>>> = Arc::default();
    let reader = std::thread::spawn({
        let lines = Arc::clone(&lines);
        let stdout = child.stdout.take().unwrap();
        move || {
            for line in BufReader::new(stdout).lines().map_while(Result::ok) {
                lines.lock().unwrap().push(line);
            }
        }
    });
    let deadline = Instant::now() + PATIENCE;
    loop {
        if lines
            .lock()
            .unwrap()
            .iter()
            .any(|line| line.contains("the controller is running"))
        {
            break;
        }
        if let Some(status) = child.try_wait().unwrap() {
            panic!(
                "the program ended with {status} before it was running:\n{}",
                lines.lock().unwrap().join("\n")
            );
        }
        assert!(
            Instant::now() < deadline,
            "the program never said it was running:\n{}",
            lines.lock().unwrap().join("\n")
        );
        std::thread::sleep(Duration::from_millis(50));
    }
    let status = Command::new("kill")
        .args(["-TERM", &child.id().to_string()])
        .status()
        .unwrap();
    assert!(status.success());
    let output = child.wait_with_output().unwrap();
    reader.join().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let lines = lines.lock().unwrap();
    assert!(
        lines.iter().all(|line| line.starts_with('{')),
        "every line of the log is JSON:\n{}",
        lines.join("\n")
    );
    let text = lines.join("\n");
    assert!(text.contains("\"signal\":\"SIGTERM\""), "{text}");
    assert!(text.contains("told to stop"), "{text}");
    assert!(text.contains("\"message\":\"stopped\""), "{text}");
    runtime.block_on(drop_slot(&client, &name));
    runtime
        .block_on(client.batch_execute(&format!("DROP PUBLICATION {name}; DROP TABLE {table}")))
        .unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn run_stops_and_fails_when_a_part_of_the_controller_cannot_carry_on() {
    let Ok(url) = std::env::var("MPC_TEST_DATABASE_URL") else {
        eprintln!("skipped: MPC_TEST_DATABASE_URL is not set");
        return;
    };
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let client = runtime.block_on(async {
        let (client, connection) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
            .await
            .unwrap();
        tokio::spawn(connection);
        client
    });
    let pid = std::process::id();
    let table = format!("cli_fail_{pid}");
    let name = format!("mpc_cli_fail_{pid}");
    runtime
        .block_on(client.batch_execute(&format!(
            "CREATE TABLE {table} (id text PRIMARY KEY, name text, cidr text)"
        )))
        .unwrap();
    let dir = scratch("run-fail");
    let path = dir.join("controller.toml");
    std::fs::write(
        &path,
        format!(
            r#"
            [postgres]
            url = "{url}"
            [replication]
            slot = "{name}"
            publication = "{name}"
            create_publication = true
            [checkpoint]
            mode = "memory"
            [[mappings]]
            table = "{table}"
            resource_type = "network"
            [[providers]]
            kind = "mock"
            "#,
            url = socket_first(&url)
        ),
    )
    .unwrap();
    let output = binary()
        .args(["--config", path.to_str().unwrap(), "run"])
        .output()
        .unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_eq!(output.status.code(), Some(1), "{stdout}\n{stderr}");
    assert!(
        stdout.contains("the controller cannot carry on"),
        "{stdout}"
    );
    assert!(stdout.contains("stopped"), "{stdout}");
    assert!(stderr.contains("TCP address"), "{stderr}");
    runtime.block_on(drop_slot(&client, &name));
    runtime
        .block_on(client.batch_execute(&format!("DROP PUBLICATION {name}; DROP TABLE {table}")))
        .unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}
