//! The controller's ordinary connection: slots, publications and lag, against
//! a real server named by `MPC_TEST_DATABASE_URL`, skipped without one.

use morph_postgres_controller::Error;
use morph_postgres_controller::postgres::database::Database;

#[test]
fn a_url_is_checked_before_any_connection_is_made() {
    let error = Database::new("not a url", None).unwrap_err();
    assert!(matches!(error, Error::Config(_)), "{error}");
    let database = Database::new(
        "postgres://user:secret@db.example.test:5433/morph?sslmode=disable",
        None,
    )
    .unwrap();
    assert_eq!(
        database.location(),
        "postgres://user@db.example.test:5433/morph"
    );
    assert!(
        !database.location().contains("secret"),
        "the password is never shown"
    );
    assert!(format!("{database:?}").contains("Database"));
    let missing_ca = Database::new(
        "postgres://user@localhost/morph",
        Some(std::path::Path::new("/nonexistent.pem")),
    );
    assert!(matches!(missing_ca.unwrap_err(), Error::Config(_)));
    let socket = Database::new("postgres:///morph?host=/var/run/postgresql&user=u", None).unwrap();
    assert_eq!(socket.location(), "postgres://u@/var/run/postgresql/morph");
}

#[tokio::test]
async fn a_server_that_cannot_be_reached_is_a_connection_error() {
    let database =
        Database::new("postgres://user@127.0.0.1:1/morph?connect_timeout=1", None).unwrap();
    let error = database.client().await.unwrap_err();
    assert!(matches!(error, Error::Connection(_)), "{error}");
}

#[tokio::test]
async fn slots_publications_and_lag_are_read_and_made() {
    let Ok(url) = std::env::var("MPC_TEST_DATABASE_URL") else {
        eprintln!("skipped: MPC_TEST_DATABASE_URL is not set");
        return;
    };
    // Everything is logged, so that the connection tasks say why they
    // ended.
    let _log = tracing::subscriber::set_default(
        tracing_subscriber::fmt()
            .with_max_level(tracing::Level::TRACE)
            .with_test_writer()
            .finish(),
    );
    let database = Database::new(&url, None).unwrap();
    let unique = format!("dbtest_{}", std::process::id());
    let in_the_clear = Database::new(&format!("{url}?sslmode=disable"), None).unwrap();
    in_the_clear
        .client()
        .await
        .expect("a connection without TLS");
    // Under `require`, a certificate that cannot be verified is a failure,
    // never a connection in the clear.
    let required = Database::new(&format!("{url}?sslmode=require"), None).unwrap();
    let refused = required.client().await.unwrap_err();
    assert!(matches!(refused, Error::Connection(_)), "{refused}");
    let no_tables = database
        .create_publication("p_unused", &[])
        .await
        .unwrap_err();
    assert!(matches!(no_tables, Error::Config(_)), "{no_tables}");
    let table = format!("t_{unique}");
    let client = database.client().await.unwrap();
    client
        .batch_execute(&format!("CREATE TABLE {table} (id text PRIMARY KEY)"))
        .await
        .unwrap();
    let again = database.client().await.unwrap();
    assert!(
        std::sync::Arc::ptr_eq(&client, &again),
        "the connection is kept"
    );

    let publication = format!("p_{unique}");
    assert!(!database.publication_exists(&publication).await.unwrap());
    database
        .create_publication(&publication, &[("public".into(), table.clone())])
        .await
        .unwrap();
    assert!(database.publication_exists(&publication).await.unwrap());

    let slot = format!("s_{unique}");
    assert!(!database.slot_exists(&slot).await.unwrap());
    let missing = database.slot_lag(&slot).await;
    assert!(missing.is_err(), "no lag without a slot");
    let start = database.create_slot(&slot).await.unwrap();
    assert!(database.slot_exists(&slot).await.unwrap());
    let lag = database.slot_lag(&slot).await.unwrap();
    assert!(lag.confirmed_flush_lsn <= lag.current_lsn);
    assert!(start <= lag.current_lsn);
    assert!(!lag.active, "nobody reads it");
    let _ = lag.bytes();

    // A connection the server ends is noticed, and the next call makes a
    // new one.
    let held = database.client().await.unwrap();
    let pid: i32 = held
        .query_one("SELECT pg_backend_pid()", &[])
        .await
        .unwrap()
        .get(0);
    let (killer, connection) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
        .await
        .unwrap();
    tokio::spawn(connection);
    let terminated: bool = killer
        .query_one("SELECT pg_terminate_backend($1)", &[&pid])
        .await
        .unwrap()
        .get(0);
    assert!(terminated);
    for _ in 0..200 {
        if held.is_closed() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    assert!(
        held.is_closed(),
        "the client notices that its connection ended"
    );
    let renewed = database.client().await.unwrap();
    assert!(
        !std::sync::Arc::ptr_eq(&held, &renewed),
        "a new connection is made"
    );
    let one: i32 = renewed.query_one("SELECT 1", &[]).await.unwrap().get(0);
    assert_eq!(one, 1);
    let client = renewed;

    // A statement the server refuses is a query error, not a connection one.
    let refused = database
        .create_publication(&publication, &[("public".into(), table.clone())])
        .await
        .unwrap_err();
    assert!(matches!(refused, Error::Query(_)), "{refused}");

    client
        .batch_execute(&format!(
            "SELECT pg_drop_replication_slot('{slot}'); DROP PUBLICATION {publication}; DROP TABLE {table}"
        ))
        .await
        .unwrap();
}
