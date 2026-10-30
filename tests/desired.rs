//! Rows that are odd, read through the row reader against a real server
//! named by `MPC_TEST_DATABASE_URL`, skipped without one.

use std::sync::Arc;

use morph::ResourceId;
use morph_postgres_controller::Error;
use morph_postgres_controller::config::Mapping;
use morph_postgres_controller::postgres::database::Database;
use morph_postgres_controller::postgres::rows::RowReader;

fn id(text: &str) -> ResourceId {
    text.parse().unwrap()
}

fn mapping(text: &str) -> Mapping {
    toml::from_str(text).unwrap()
}

#[tokio::test]
async fn odd_rows_are_passed_over_or_refused_as_the_call_demands() {
    let Ok(url) = std::env::var("MPC_TEST_DATABASE_URL") else {
        eprintln!("skipped: MPC_TEST_DATABASE_URL is not set");
        return;
    };
    let database = Arc::new(Database::new(&url, None).unwrap());
    let table = format!("t_desired_{}", std::process::id());
    let client = database.client().await.unwrap();
    client
        .batch_execute(&format!(
            "CREATE TABLE {table} (id text, resource_type text, name text, env text); \
             INSERT INTO {table} VALUES ('ok', 'network', 'a', 'prod'); \
             INSERT INTO {table} VALUES ('bad id', 'network', 'b', 'prod'); \
             INSERT INTO {table} VALUES ('nokind', NULL, 'c', 'prod'); \
             INSERT INTO {table} VALUES (NULL, 'network', 'd', 'staging'); \
             INSERT INTO {table} VALUES ('fine', 'network', 'e', NULL)"
        ))
        .await
        .unwrap();

    // By a type column: rows that name no valid resource are passed over
    // when listing, and a row is read by its id and kind.
    let by_column = mapping(&format!(
        "table = \"{table}\"\ntype_column = \"resource_type\"\nexclude_columns = [\"env\"]"
    ));
    let source = RowReader::new(Arc::clone(&database), vec![by_column]);
    assert_eq!(
        source.all_resources().await.unwrap(),
        [id("network.fine"), id("network.ok")]
    );
    let row = source
        .row(&id("network.ok"))
        .await
        .unwrap()
        .expect("the row");
    assert_eq!(row.resource, id("network.ok"));
    assert_eq!(row.json["name"], "a");
    assert!(source.row(&id("network.nope")).await.unwrap().is_none());
    assert!(
        source.row(&id("server.ok")).await.unwrap().is_none(),
        "a kind the mapping cannot hold"
    );
    assert_eq!(
        source.load(&id("network.ok")).await.unwrap().iter().count(),
        1
    );
    assert!(source.load(&id("network.nope")).await.unwrap().is_empty());

    // Grouped by a column: every row of a group has to name a resource, or
    // the group cannot be written. A row with no stack is passed over.
    let grouped = mapping(&format!(
        "table = \"{table}\"\ntype_column = \"resource_type\"\nstack_column = \"env\""
    ));
    let source = RowReader::new(Arc::clone(&database), vec![grouped]);
    assert_eq!(
        source.all_resources().await.unwrap(),
        [id("stack.prod"), id("stack.staging")]
    );
    let error = source.group_rows("prod").await.unwrap_err();
    assert!(
        matches!(error, Error::Mapping(ref m) if m.contains("\"bad id\"")),
        "{error}"
    );
    let error = source.group_rows("staging").await.unwrap_err();
    assert!(
        matches!(error, Error::Mapping(ref m) if m.contains("has no id")),
        "{error}"
    );
    assert!(source.group_rows("nothing").await.unwrap().is_empty());
    client
        .batch_execute(&format!("DELETE FROM {table} WHERE id = 'bad id'"))
        .await
        .unwrap();
    let error = source.group_rows("prod").await.unwrap_err();
    assert!(
        matches!(error, Error::Mapping(ref m) if m.contains("has no resource_type")),
        "{error}"
    );
    client
        .batch_execute(&format!(
            "INSERT INTO {table} VALUES ('odd', 'network', 'f', 'bad env')"
        ))
        .await
        .unwrap();
    assert_eq!(
        source.all_resources().await.unwrap(),
        [id("stack.prod"), id("stack.staging")],
        "a value that is no stack name is passed over"
    );
    client
        .batch_execute(&format!(
            "DELETE FROM {table} WHERE id IS NULL OR id = 'odd' OR resource_type IS NULL"
        ))
        .await
        .unwrap();
    let rows = source.group_rows("prod").await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].resource, id("network.ok"));

    // A fixed stack takes every row of the table, sorted by resource.
    let fixed = mapping(&format!(
        "table = \"{table}\"\nresource_type = \"network\"\nstack = \"fleet\""
    ));
    let source = RowReader::new(Arc::clone(&database), vec![fixed]);
    assert_eq!(source.all_resources().await.unwrap(), [id("stack.fleet")]);
    let rows = source.group_rows("fleet").await.unwrap();
    assert_eq!(
        rows.iter()
            .map(|row| row.resource.to_string())
            .collect::<Vec<_>>(),
        ["network.fine", "network.ok"]
    );

    // A table of one fixed kind, not grouped: listed and read by kind.
    let plain = mapping(&format!(
        "table = \"{table}\"\nresource_type = \"network\"\nexclude_columns = [\"env\", \"resource_type\"]"
    ));
    let source = RowReader::new(Arc::clone(&database), vec![plain]);
    assert_eq!(
        source.all_resources().await.unwrap(),
        [id("network.fine"), id("network.ok")]
    );
    assert!(source.row(&id("network.ok")).await.unwrap().is_some());
    assert!(source.row(&id("server.ok")).await.unwrap().is_none());

    // A mapping that names no kind at all cannot write a group.
    let nameless = mapping(&format!("table = \"{table}\"\nstack = \"fleet\""));
    let source = RowReader::new(Arc::clone(&database), vec![nameless]);
    let error = source.group_rows("fleet").await.unwrap_err();
    assert!(
        matches!(error, Error::Mapping(ref m) if m.contains("names no kind")),
        "{error}"
    );

    // An id that is not text is read as its text.
    let numbers = format!("{table}_n");
    client
        .batch_execute(&format!(
            "CREATE TABLE {numbers} (id int PRIMARY KEY, size int); \
             INSERT INTO {numbers} VALUES (7, 2), (3, 1)"
        ))
        .await
        .unwrap();
    let numeric = mapping(&format!(
        "table = \"{numbers}\"\nresource_type = \"disk\"\nstack = \"disks\""
    ));
    let source = RowReader::new(Arc::clone(&database), vec![numeric]);
    let rows = source.group_rows("disks").await.unwrap();
    assert_eq!(
        rows.iter()
            .map(|row| row.resource.to_string())
            .collect::<Vec<_>>(),
        ["disk.3", "disk.7"]
    );

    client
        .batch_execute(&format!("DROP TABLE {table}; DROP TABLE {numbers}"))
        .await
        .unwrap();
}
