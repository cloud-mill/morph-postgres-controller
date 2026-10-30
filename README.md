# morph-postgres-controller

A standalone service that reconciles external resources whenever PostgreSQL says that their desired state changed,
through [Morph](https://github.com/cloud-mill/morph) or through [Pulumi](https://www.pulumi.com).

PostgreSQL says *when* and *which*. The engine decides *what*. A row that changes names a resource to look at; the
engine then takes the resource's desired state from the row, reads the actual state through its provider, works out
the difference and applies it. Nothing in this service turns a row change into an action on a resource. If a row says
`cpu: 2 -> 4`, the controller does not set the CPU to 4. It asks the engine to reconcile the resource, and the engine
works out that the CPU needs changing, or that it already was.

Two engines are offered, chosen in the configuration. **Morph** runs as a library in this process, with the providers
written for Terraform and OpenTofu and a record per resource in PostgreSQL. **Pulumi** runs as the `pulumi` command,
with a stack per resource in any Pulumi backend, so that everything Pulumi has, from its console and history to its
policy packs and secrets, applies to resources that came from rows.

```mermaid
flowchart LR
  pg[("PostgreSQL")] -- " logical replication " --> reader["reader"]
  reader -- " committed transactions " --> intake["intake"]
  intake -- " marks " --> scheduler["scheduler"]
  intake --> dirty[("dirty table")]
  periodic["periodic loop"] -- " marks " --> scheduler
  scheduler -- " turns " --> workers["workers"]
  workers --> engine["engine: Morph or Pulumi"] --> providers["providers"]
```

## Try it

A PostgreSQL with logical replication, a table of files, and the `local` provider that was written for Terraform:

```sh
docker run -d --rm -p 54329:5432 -e POSTGRES_USER=morph -e POSTGRES_PASSWORD=morph postgres:16-alpine -c wal_level=logical
psql postgres://morph:morph@localhost:54329/morph \
  -c "CREATE TABLE files (id text PRIMARY KEY, filename text NOT NULL, content text NOT NULL); ALTER TABLE files REPLICA IDENTITY FULL"
```

Save this as `local-files.toml`:

```toml
[postgres]
url = "postgres://morph:morph@localhost:54329/morph"

[replication]
slot = "morph_controller_demo"
publication = "morph_demo"
create_publication = true

[[mappings]]
table = "files"
resource_type = "local_file"

[[providers]]
kind = "terraform"
source = "hashicorp/local"
version = "2.5.2"

[morph]
state = { backend = "postgres" }
```

Then check it and start it:

```sh
cargo run -- --config local-files.toml check
cargo run -- --config local-files.toml
```

In another terminal:

```sh
psql postgres://morph:morph@localhost:54329/morph -c "INSERT INTO files VALUES ('hello', '/tmp/hello.txt', 'hello from a row')"
psql postgres://morph:morph@localhost:54329/morph -c "UPDATE files SET content = 'the row changed' WHERE id = 'hello'"
psql postgres://morph:morph@localhost:54329/morph -c "DELETE FROM files WHERE id = 'hello'"
```

The file appears, changes, and goes. The log says what Morph did each time. Ctrl-C or SIGTERM stops the controller once
what is in flight has finished.

Any provider in the OpenTofu registry can be used the same way: a table of queues and the AWS provider make SQS queues,
a table of instances and the Google provider make virtual machines.

## How it works

**The reader** holds a logical replication connection on a persistent slot and reads `pgoutput`. The wire protocol is
spoken by the `pgwire-replication` crate; the `pgoutput` messages are decoded here. `RELATION` messages are kept for the
session, so that `INSERT`, `UPDATE` and `DELETE` tuples can be read by column name. Changes are gathered by transaction
and nothing leaves the reader until the `COMMIT`. Within a transaction, a resource that changed three times is a
resource that changed. A change to a table that is not mapped, or that the filters exclude, is dropped before it is
mapped.

**The mapper** turns a row change into a Morph resource id, `kind.name`, from the mapping of its table: a column that
names the resource, and either a column that says its kind or a kind fixed for the whole table. It does not look at what
changed.

**The intake** records each committed transaction's resources and marks them for reconciliation. In the durable
checkpoint mode it writes them to a table first, and the transaction's position is confirmed to the server as soon as
that write is done, because a restart reads the table. In the memory mode the position is confirmed once every resource
of the transaction, and of every transaction before it, has been reconciled or given up on.

**The scheduler** is a bounded set of dirty resources. A resource is in it once however many times it was marked. Two
turns at the same resource never overlap: a mark that arrives while the resource is being reconciled makes it run again
afterwards. Different resources are reconciled in parallel, up to `reconcile.workers` at once. When the set is full the
intake waits, and the reader waits behind it, which is how a slow provider slows the reading of the log rather than
filling memory. The replication client keeps answering the server's keepalives meanwhile, so the connection is not
dropped for it.

**The workers** call the engine. For each turn, the controller reads the resource's row again, as it is now, and hands
it to the engine, or tells the engine that the row is gone. With Morph, the row becomes a desired state of one
resource, and Morph is given that and the resource's own record, and does the rest: refresh, plan, apply. With Pulumi,
the row becomes a program of one resource, and `pulumi up` is run on the resource's stack, with a refresh first; a row
that is gone is `pulumi destroy`. A failed reconciliation is tried again after a growing wait, and after
`reconcile.retry.max_attempts` the resource is reported unhealthy and left to the next full reconciliation. One broken
resource stops nothing else.

**The periodic loop** marks every resource that has a row or a record, every `reconcile.full_interval`, and once at
start. This is the safety path: it catches what the stream could not say, such as something changed at the provider, a
delete the stream could not name, or a bug anywhere in between.

**The lag watcher** reads the slot's position every so often and warns when the slot holds back more log than
`replication.lag_warn_bytes`. A slot whose consumer has stopped keeps log for ever, and the warning is there so that
nobody finds out from a full disk.

**Shutdown** on SIGTERM or Ctrl-C stops taking in changes, closes the scheduler, lets reconciliations in flight finish
within a grace period, hands the last confirmed position to the replication client, and stops. Whatever the server was
not told of is sent again at the next start and reconciled again, which is harmless.

## The boundary with the engine

| This service decides                       | The engine decides                                    |
|--------------------------------------------|-------------------------------------------------------|
| When a resource is looked at               | What the resource's actual state is                   |
| Which resource a row describes             | Whether anything differs, and what it means           |
| What the desired state of that resource is | The plan, its order, and whether to replace or update |
| How often, how many at once, how to retry  | How to carry the plan out, and what to record         |

Morph is used as a library, unchanged: `Engine::converge` over a `DesiredState`, with a `StateStore` per record and
the providers of the configuration. By default each resource has a record of its own, so that reconciling one never
plans for another, and two can run at once without contending for one record.

Pulumi is used as it is installed, through its command line: one stack per resource in the backend of your choice,
each holding a program of one resource that the controller writes from the row before every update. Stacks are
ordinary Pulumi stacks, and Pulumi locks each for itself, so reconciliations of different resources run side by side.

A mapping can group its rows into one stack instead, with `stack = "fleet"` or `stack_column = "env"`. The stack is
then one record for either engine, reconciled as a whole whenever any of its rows changes: one Morph state whose rows
are one plan, or one Pulumi stack whose program holds every row. Rows of a stack that refer to each other are the
engine's own references, ordered and carried within that one plan.

## Choosing the engine

| | Morph | Pulumi |
|---|---|---|
| Runs as | A library in this process | The `pulumi` command, one process per reconciliation |
| Providers | Terraform and OpenTofu providers from the OpenTofu registry | Everything in Pulumi's registry, including its native providers |
| Records | One Morph state per resource, or per stack of rows, in PostgreSQL, a file or memory | One Pulumi stack per resource, or per stack of rows, in Pulumi Cloud, a bucket or a directory |
| Property names | As the Terraform provider documents them: `machine_type` | As Pulumi documents them: `machineType`; columns are converted |
| Cost per reconciliation | Milliseconds plus the provider's calls | A second or two plus the provider's calls |
| What you get | A small dependency and no other service | Pulumi Cloud's console and history, policy packs, encrypted secrets, and the `pulumi` tools on every stack |

Pick Morph for the lightest footprint. Pick Pulumi if you already run Pulumi and want its backend, console and policy
packs to cover what comes from rows. The rows, the mappings, the checkpointing and the scheduling are the same either
way; only the `[engine]` table and the property names in the rows differ.

## Configuration

A TOML file, given with `--config`. `MPC_DATABASE_URL` overrides `postgres.url` and `MPC_LOG` overrides
`logging.level`. Durations are written as `30s`, `5m`, `1h`.

```toml
[postgres]
url = "postgres://morph:secret@db.example.test/morph?sslmode=require"
ca_certificate = "/etc/ssl/rds.pem"        # optional, besides the public authorities

[replication]
slot = "morph_controller"                  # kept across restarts, never dropped by the controller
publication = "morph_resources"
create_slot = true                         # make it if missing; an existing one is always reused
create_publication = false                 # make it for the mapped tables if missing
status_interval = "10s"                    # how often the server is told how far the controller got
lag_check_interval = "30s"
lag_warn_bytes = 1073741824
[replication.reconnect]
initial = "1s"
max = "1m"
multiplier = 2.0

[filters]                                  # all optional; tables as "schema.table"
include_schemas = ["public"]
include_tables = []
exclude_schemas = []
exclude_tables = ["public.audit"]

[[mappings]]
schema = "public"                          # default
table = "resources"
id_column = "id"                           # default
type_column = "resource_type"              # or resource_type = "vm" for the whole table
exclude_columns = ["created_at"]           # never attributes; the id and type columns never are
# attribute_columns = ["name", "cpu"]      # only these, if given
# attributes_column = "spec"               # a JSON column whose object is merged over the rest
# stack = "fleet"                          # group every row into one record, a Morph state or a Pulumi stack
# stack_column = "env"                     # or group rows by the value of a column
exports = "id"                             # what rows that refer to these resources receive
foreign_keys = true                        # a foreign key into a mapped table is a reference
# import_column = "object"                 # the id of an object that exists already, for the engine to take charge of
# [[mappings.references]]                  # a reference without a constraint, or taking another attribute
# column = "network"
# table = "networks"
# attribute = "self_link"
# sets = "network_id"                      # the attribute that receives the value; the column itself by default
[mappings.lifecycle]
prevent_destroy = false
retain = false                             # when the row goes, stop tracking rather than delete
create_before_destroy = false

[[mappings]]
schema = "infra"
table = "virtual_machines"
id_column = "uuid"
resource_type = "vm"

[checkpoint]
mode = "durable"                           # or "memory"
table = "morph_controller_dirty"

[references]
table = "morph_controller_references"      # which resource refers to which

[reconcile]
workers = 16                               # reconciliations at once
queue_size = 1000                          # dirty resources before the reader waits
full_interval = "5m"
timeout = "10m"                            # the longest one reconciliation may take
[reconcile.retry]
max_attempts = 5
initial = "1s"
max = "1m"
multiplier = 2.0

[morph]
state = { backend = "postgres" }           # or { backend = "file", directory = "/var/lib/morph" } or { backend = "memory" }
state_prefix = "controller/"
# operation_timeout = "15m"               # unlimited unless set; providers written for Terraform wait for themselves

[[providers]]
kind = "terraform"
source = "hashicorp/aws"
version = "6.66.0"                         # the newest, if left out
cache = "/var/cache/morph-providers"
[providers.config]                         # what would be written in the provider block
region = "eu-west-1"

[[providers]]
kind = "mock"                              # Morph's in-memory cloud, for trying things out

[logging]
level = "info"
format = "text"                            # or "json"
metrics_interval = "30s"
```

With Pulumi as the engine, `[morph]` and `[[providers]]` give way to `[engine]`:

```toml
[engine]
kind = "pulumi"
backend = "https://api.pulumi.com"         # or file:///var/lib/controller/pulumi, s3://..., gs://..., azblob://...
project = "resources-from-postgres"        # the Pulumi project every stack belongs to
work_dir = "/var/lib/controller/pulumi-work"
# home = "/var/lib/controller/pulumi-home" # PULUMI_HOME: plugins and credentials
# passphrase_file = "/etc/controller/pulumi-passphrase"   # for backends other than Pulumi Cloud
stack_prefix = ""                          # stacks are named <prefix><kind>.<name>
refresh = true                             # read the actual state before each update
camel_case_columns = true                  # machine_type -> machineType
# policy_packs = ["/etc/controller/policy"]
[engine.types]                             # the Pulumi type of each kind the mappings name
aws_sqs_queue = "aws:sqs:Queue"
[engine.config]                            # every stack's configuration, as pulumi config would set it
"aws:region" = "eu-west-1"
```

Credentials are never in the file. Pulumi reads them from the controller's environment: `PULUMI_ACCESS_TOKEN` for
Pulumi Cloud, the cloud's own variables for its providers.

### What a row becomes

Every column of the row is an attribute of the resource, by name, except the id column, the type column, the excluded
columns, and those outside `attribute_columns` when that is given. A null is not an attribute. Values are read as JSON,
so numbers, booleans, text, arrays and JSON objects all pass through as what they are. The resource is
`kind.name`, where both parts may only hold letters, digits, `_` and `-`.

For a `DELETE`, logical replication sends only the row's replica identity. A table whose kind comes from a column needs
`REPLICA IDENTITY FULL` for its deletes to name their kind; otherwise the delete is logged and left to the periodic
loop, which notices that a record has no row. A table with a fixed kind needs only its id column in the identity.

### Rows that refer to other rows

A foreign key from one mapped table into another makes its column a reference. A subscription row whose `topic`
column is `REFERENCES topics(id)` is handed the topic's identifier at the provider in place of the row id, waits for
the topic if it has not been reconciled yet, is reconciled again when the topic's identifier changes, and is removed
before the topic is. A mapping may declare such a column by hand with `[[mappings.references]]` where there is no
constraint, and `exports` names the attribute the referring rows receive, `id` by default. Two rows of one stack that
refer to each other are left to the engine: a Morph reference in one plan, or a Pulumi expression in one program.

### Checkpoint modes

| Mode      | The position is confirmed to the server when             | A restart                                    | A slow or failing resource                |
|-----------|----------------------------------------------------------|----------------------------------------------|-------------------------------------------|
| `durable` | The transaction's resources are written to the table     | Reads the table and reconciles what it holds | Holds back nothing                        |
| `memory`  | Every resource of it and of earlier transactions is done | Is sent the unconfirmed transactions again   | Holds back confirmation of all later ones |

Both are at-least-once. A transaction may be delivered twice after a restart, and a resource may be reconciled twice.
Morph makes that harmless: the second time it finds the resource as it should be and does nothing.

## Observability

Structured `tracing` output, in text or JSON. Every reconciliation is logged with the resource, why it ran, the attempt,
whether anything changed, how long it took and Morph's summary. Every `logging.metrics_interval` one line carries the
counters: connected, reconnects, received and confirmed positions, lag bytes, transactions, row changes, mapping errors,
resources marked and coalesced, reconciliations started, succeeded and failed, turns that waited for other resources,
marks propagated from a changed export, time spent reconciling, full runs, and how many resources are waiting, running,
parked and unhealthy. Row contents are never logged.

The dirty table is itself observable: `SELECT * FROM morph_controller_dirty` lists what is waiting, with the attempts
and the last error of anything that is failing. A resource marked only by the periodic loop has no row there, so its
failures show in the log alone.

A process that embeds the controller as a library gets the same as events: `Controller::events()` yields connection,
transaction, reconciliation, waiting and full-run events, which is how the integration tests watch it.

## Failure model

| What happens                             | What the controller does                                                                     |
|------------------------------------------|----------------------------------------------------------------------------------------------|
| The controller restarts                  | Reuses the slot, takes up the dirty table, and is sent what was not confirmed                |
| PostgreSQL restarts or the network drops | Connects again after a wait, rebuilds its table descriptions, carries on from the slot       |
| A reconciliation fails                   | Tries again after a growing wait, then reports the resource unhealthy; others are unaffected |
| A provider call fails                    | Is Morph's to report; the reconciliation fails as above                                      |
| A change is delivered twice              | The resource is reconciled again, and Morph finds nothing to do                              |
| The database is briefly unavailable      | Dirty-table writes are retried; reconciliations fail and are retried; the stream reconnects  |
| A row names no resource                  | Is logged and counted; the periodic loop catches up                                          |
| A row refers to a resource not yet there | Waits for it, without a failed attempt, and is reconciled as soon as it is                   |
| `wal_level` is not `logical`             | The controller refuses to start, and says so                                                 |

## Testing

```sh
cargo test --workspace --lib                       # no database needed: both crates' unit tests
cargo test --workspace --doc                       # and their documentation examples
docker run -d --rm -p 54329:5432 -e POSTGRES_USER=morph -e POSTGRES_PASSWORD=morph postgres:16-alpine -c wal_level=logical
export MPC_TEST_DATABASE_URL=postgres://morph:morph@localhost:54329/morph
cargo test -p morph-controller-core                # no database needed: the core over an in-memory source
cargo test --test controller --test references --test database --test desired --test cli --test providers \
           --test stores --test source
cargo test --test pulumi                           # needs pulumi on the path; fetches the random plugin once
cargo test --test adopt                            # needs hashicorp/random in ../morph/target/providers
cargo llvm-cov --workspace --all-targets           # line coverage, with cargo-llvm-cov installed
```

Every suite that needs the database says that it was skipped when `MPC_TEST_DATABASE_URL` is not set, and the
Pulumi suite also when `pulumi` is not on the path, so `cargo test` passes anywhere and proves more where more is
available. Each test makes a table, publication, slot and dirty table of its own and removes them.

**The core's own suites** need no database. They run the controller over a second `Source`, one that keeps its rows
in memory and sends the transactions the test commits, with a dirty store and a store of references that fail on
demand, and read the log the controller writes. That is where the paths a real database rarely takes are proved: a
dirty store that cannot be written holding the transaction back and never confirming it, one that cannot be updated
leaving a turn's outcome standing, a store of references that cannot be read leaving propagation to the full pass, a
source whose stream ends, fails or panics being reported by `failed` and by `stop`, a source that cannot list its
rows making the full pass try again, the relay falling behind the source's events, a stack waiting for a resource
outside it, a reference held as a number, a row that refers to itself, and the scheduler's races: a mark that waited
for room finding its resource waiting or parked meanwhile, a resource given up on while a mark was kept for it, and
a wait that reaches a resource by two paths.

**Unit tests** cover the decoder, including every prefix of every message, the transaction buffer, the mapper, the
row-to-properties rules, the scheduler's locking, bounds, retries and close, the ledger, the configuration's defaults
and every refusal, the file and memory record stores, the metrics, the error conversions, TLS roots, and the Pulumi
engine over a stand-in `pulumi` script that records every call and answers from files, so that every way an update or
a destroy can succeed or fail is exercised without Pulumi, a backend or a network, down to a dropped reconciliation
killing its process.

**Integration suites**, against the real database:

- `controller`: insert, update and delete through Morph; many changes in one transaction reconciling once; a restart
  taking up what happened meanwhile; a cut connection made again; a change delivered twice applying once; a failing
  resource not holding up the others; the periodic loop catching drift; the dirty table taken up at start; `retain`
  and `prevent_destroy`; the memory checkpoint mode with the file record store, the lag watcher and the metrics line;
  and a reconciliation that takes too long being abandoned and retried.
- `references`: rows that refer to other rows, through Morph and the mock cloud: a server handed its network's id,
  one inserted before its network has a record waiting and being woken, a replaced network's id reaching the servers
  and a DNS record following a server's new address, a network going only after the servers that refer to it, the
  same across a restart, a reference to a row that does not exist or to an attribute that is not there failing with
  the reason, two rows that wait for each other being a failure rather than a deadlock, and a stack of rows being
  one Morph plan that outsiders read their values from.
- `pulumi`: a row becoming a stack, a replacement, a change that changes nothing, a kind with no Pulumi type, the row
  going with its stack; rows grouped into one stack by a column, moving between groups and the last row taking the
  stack with it; single and grouped tables in one project with the full pass removing an emptied group; a group with
  a row it cannot write failing whole until the row is fixed; a delete that cannot name its stack being left to the
  full pass; a row taking another stack's output and following its change; rows of one stack linked in the
  program, with an outsider reading the stack's output; and a row with an import id taking charge of a random id
  that Pulumi made before, with a row that describes it otherwise failing twice, once at the import and once at the
  replacement `prevent_destroy` refuses, and the object left as it was.
- `adopt`: taking charge of what exists through a real provider written for Terraform, `hashicorp/random` from
  Morph's cache: a `random_id` row whose import id carries the prefix reconciling with no change and a record that
  holds the hex Terraform held, an identifier that names nothing failing and making nothing, and one without the
  prefix being imported, found to differ, and kept from replacement by `prevent_destroy`.
- `source`: the PostgreSQL source on its own: a cancelled one reading nothing, the stream ending when nobody takes
  its transactions, a server that cannot be reached being tried again while the slot's lag cannot be read, and the
  lag watcher noting a small lag and warning of a large one.
- `stores`: the controller's own tables when they are not what it made: a table of another shape under the dirty
  table's name or the references table's name failing every use with the server's own message, a domain under the
  dirty table's name being in the way of making it, a row of the references table that names no resource being
  passed over, and Morph's records in the database refusing a certificate file that cannot be read.
- `database`, `desired`, `providers`, `cli`: the connection helpers, odd rows through the row reader, the providers
  from the configuration including one written for Terraform started from Morph's cache, and the binary: `check`
  for both engines, against a publication, a slot and a foreign key that exist, and with a certificate file that is
  not there; `run` until `SIGTERM`, with JSON logging; and `run` stopping with exit code 1 when a part of the
  controller cannot carry on.

## Limitations

- A row refers to another row by a column that holds its id, and takes one attribute of the referenced resource. A
  replacement that changes that attribute reaches the referring rows as a second reconciliation, not as one plan.
- The foreign keys are read when the controller starts; a constraint added later counts from the next start.
- With Pulumi, every reconciliation is a `pulumi` process, and the provider's plugin is started with it, so a
  reconciliation costs a second or two before the provider is even asked. A stack per resource also means as many
  stacks as rows: an update costs the same however many there are, but the full pass lists them all, which on a file
  backend takes seconds for thousands of stacks and minutes for tens of thousands.
- `TRUNCATE` is logged and left to the periodic loop; the stream does not say which rows went.
- The stream is read with `pgoutput` protocol version 1, over TCP, from one database.
- Replication slot lag is watched and warned about, not acted on. A controller that is stopped for long holds log on the
  server until it runs again, or the slot is dropped by hand.

## Layout

The repository is a Cargo workspace of two crates. `morph-controller-core` is everything that does not depend on
where the rows come from: the scheduler, the workers, the engines, the references, the stacks, and the controller
that wires them together. It knows a *source* only through a trait, `Source`, which says which resources changed and
reads a row back when one is reconciled, and keeps the controller's own records behind two more traits, `DirtyStore`
and `ReferenceStore`. `morph-postgres-controller`, the root crate, is the PostgreSQL front-end: the source over
logical replication, the two tables, Morph's records in the database, the configuration file and the program. A
second source, another database with a change log of its own, would be a second front-end over the same core.

```mermaid
flowchart TB
    subgraph core ["crates/morph-controller-core/src/"]
        direction TB
        capp["app.rs<br/>the controller: wiring, intake, workers, periodic loop, shutdown; Parts, Checkpoint"]
        cconfig["config.rs<br/>the shared configuration types and their checks"]
        cerror["error.rs<br/>what can go wrong, by layer"]
        cproviders["providers.rs<br/>providers from the configuration"]
        cstores["stores.rs<br/>a Morph record per resource: file, memory, or a RecordStores"]
        cobserve["observe.rs<br/>counters"]
        subgraph csource ["source/"]
            direction TB
            sourcemod["mod.rs<br/>the Source trait, Position, Confirmed, Transaction"]
            transaction["transaction.rs<br/>transaction boundaries and coalescing"]
        end
        subgraph cengine ["engine/"]
            direction TB
            enginemod["mod.rs<br/>the engine the configuration chose; Morph"]
            pulumi["pulumi.rs<br/>Pulumi: a stack per resource, through the command line"]
        end
        subgraph cmapping ["mapping/"]
            desired["desired.rs<br/>row to desired state"]
        end
        subgraph ccheckpoint ["checkpoint/"]
            direction TB
            checkpointmod["mod.rs<br/>the DirtyStore trait"]
            ledger["ledger.rs<br/>which position is safe to confirm"]
        end
        subgraph creferences ["references/"]
            direction TB
            refmod["mod.rs<br/>references between tables, from foreign keys and mappings"]
            resolve["resolve.rs<br/>replacing row ids with what the referenced resource exports"]
            refstore["store.rs<br/>the ReferenceStore trait"]
        end
        subgraph cscheduler ["scheduler/"]
            dirty["dirty_set.rs<br/>the bounded set, keyed locking, retry, waiting for others"]
        end
        subgraph cworker ["worker/"]
            reconcile["reconcile.rs<br/>the one place the engine is called"]
        end
    end
    subgraph pg ["src/ (morph-postgres-controller)"]
        direction TB
        main["main.rs<br/>the program: run, check, signals"]
        app["app.rs<br/>the parts from the configuration; the Controller wrapper"]
        config["config.rs<br/>the configuration file: the PostgreSQL tables, the rest re-exported"]
        source["source.rs<br/>PostgresSource: the Source over logical replication, with the lag watcher"]
        checkpoint["checkpoint.rs<br/>the dirty table"]
        references["references.rs<br/>foreign keys from the catalogue; the references table"]
        stores["stores.rs<br/>Morph's records in the database"]
        subgraph postgres ["postgres/"]
            direction TB
            replication["replication.rs<br/>the reader, on the replication client"]
            pgoutput["pgoutput.rs<br/>decoding the plugin's messages"]
            relation["relation.rs<br/>table descriptions, row changes"]
            rows["rows.rs<br/>reading rows back as desired state"]
            database["database.rs<br/>the ordinary connection: slots, publications, lag"]
            lsn["lsn.rs<br/>positions in the log"]
            tls["tls.rs<br/>encryption of connections"]
        end
        subgraph mapping ["mapping/"]
            mapper["resource_mapper.rs<br/>row change to resource id"]
        end
    end
    pg -- " implements Source, DirtyStore, ReferenceStore, RecordStores " --> core
```
