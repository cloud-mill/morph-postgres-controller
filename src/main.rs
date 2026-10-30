//! The controller as a program.
//!
//! ```text
//! morph-postgres-controller --config controller.toml          run until told to stop
//! morph-postgres-controller --config controller.toml check    see whether it could run
//! ```
//!
//! Everything of substance is in the library. This reads the configuration,
//! sets up logging, and then either runs a [`Controller`] until a signal or
//! a failure ends it, or prints what a controller would find.
//!
//! The exit code is `0` when the controller was told to stop or the check
//! went through, `1` when the controller or the check failed, and `2` when
//! the configuration could not be loaded.

use std::path::PathBuf;
use std::time::Duration;

use clap::{Parser, Subcommand};
use morph_postgres_controller::config::{Config, EngineConfig, LogFormat};
use morph_postgres_controller::engine::PulumiEngine;
use morph_postgres_controller::postgres::database::Database;
use morph_postgres_controller::postgres::failed;
use morph_postgres_controller::providers::Providers;
use morph_postgres_controller::references::discover;
use morph_postgres_controller::{Controller, Result};

#[derive(Parser)]
#[command(
    version,
    about = "Reconciles resources through Morph or Pulumi whenever PostgreSQL says their desired state changed"
)]
struct Arguments {
    /// The configuration file.
    #[arg(short, long, default_value = "controller.toml")]
    config: PathBuf,
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand)]
enum Command {
    /// Run until told to stop.
    Run,
    /// Check the configuration, the database, and the providers, and stop.
    Check,
}

/// Sends `tracing` output to standard output, filtered by `logging.level`
/// and written as `logging.format` says.
///
/// A level that is not a valid filter is replaced by `info`, and nothing
/// says so.
fn logging(config: &Config) {
    let filter = tracing_subscriber::EnvFilter::try_new(&config.logging.level)
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    let builder = tracing_subscriber::fmt().with_env_filter(filter);
    match config.logging.format {
        LogFormat::Text => builder.init(),
        LogFormat::Json => builder.json().init(),
    }
}

/// Prints what a controller would find, without starting one.
///
/// Connects to the database and prints its `wal_level`, whether the
/// publication and the slot exist and whether they would be made, the
/// slot's lag if it exists, the mappings, and the references between
/// their tables as the foreign keys and the mappings give them. Then, with
/// the Morph engine, starts the providers, which fetches any that the
/// cache lacks, and prints how many kinds of resource they offer; with the
/// Pulumi engine, runs `pulumi version` and lists the stacks of the
/// project.
///
/// Nothing is made in the database. The `wal_level` is printed and not
/// judged: only a start refuses one that is not `logical`. The check stops
/// at the first step that fails, with that step's error.
async fn check(config: Config) -> Result<()> {
    let database = Database::new(
        &config.postgres.url,
        config.postgres.ca_certificate.as_deref(),
    )?;
    let client = database.client().await?;
    let level: String = client
        .query_one("SHOW wal_level", &[])
        .await
        .map_err(failed)?
        .get(0);
    println!(
        "database      {} (wal_level = {level})",
        database.location()
    );
    let publication = &config.replication.publication;
    let exists = database.publication_exists(publication).await?;
    println!(
        "publication   {publication} {}",
        match (exists, config.replication.create_publication) {
            (true, _) => "exists",
            (false, true) => "does not exist, and would be made",
            (false, false) => "does not exist, and would not be made",
        }
    );
    let slot = &config.replication.slot;
    let exists = database.slot_exists(slot).await?;
    println!(
        "slot          {slot} {}",
        match (exists, config.replication.create_slot) {
            (true, _) => "exists",
            (false, true) => "does not exist, and would be made",
            (false, false) => "does not exist, and would not be made",
        }
    );
    if exists {
        let lag = database.slot_lag(slot).await?;
        println!(
            "              confirmed {} of {}, {} bytes behind, {}",
            lag.confirmed_flush_lsn,
            lag.current_lsn,
            lag.bytes(),
            if lag.active { "active" } else { "not active" }
        );
    }
    for mapping in &config.mappings {
        let kind = match (&mapping.resource_type, &config.engine) {
            (Some(kind), EngineConfig::Pulumi(pulumi)) => {
                format!(
                    "{kind} ({})",
                    pulumi
                        .types
                        .get(kind)
                        .map_or("no Pulumi type", String::as_str)
                )
            }
            (Some(kind), EngineConfig::Morph) => kind.clone(),
            (None, _) => format!(
                "from column {}",
                mapping.type_column.clone().unwrap_or_default()
            ),
        };
        let grouping = match (&mapping.stack, &mapping.stack_column) {
            (Some(stack), _) => format!(", all in the stack {stack}"),
            (None, Some(column)) => format!(", in the stack named by column {column}"),
            (None, None) => String::new(),
        };
        println!(
            "mapping       {} -> {kind}, named by {}{grouping}",
            mapping.qualified_table(),
            mapping.id_column
        );
    }
    let references = discover(&database, &config.mappings).await?;
    for reference in references.iter() {
        println!("reference     {reference}");
    }
    println!(
        "references    {} between rows, kept in {}",
        references.len(),
        config.references.table
    );
    println!("checkpoint    {:?} mode", config.checkpoint.mode);
    match &config.engine {
        EngineConfig::Morph => {
            let providers = Providers::start(&config.providers, &config.morph).await?;
            println!("engine        morph");
            println!(
                "providers     {} kinds of resource",
                providers.registry.schemas().kinds().count()
            );
            println!("morph state   {:?}", config.morph.state);
        }
        EngineConfig::Pulumi(settings) => {
            let pulumi = PulumiEngine::new(settings)?;
            let version = pulumi.version().await?;
            println!(
                "engine        pulumi {version}, project {}, backend {}",
                pulumi.project(),
                pulumi.backend()
            );
            let stacks = pulumi.stacks().await?;
            let ours = stacks
                .iter()
                .filter(|stack| pulumi.resource_of(stack).is_some())
                .count();
            println!(
                "stacks        {} in the project, {ours} of them this controller's",
                stacks.len()
            );
        }
    }
    Ok(())
}

/// Runs a controller until a signal or a failure ends it.
///
/// On Ctrl-C, or SIGTERM on Unix, the controller is stopped with a minute
/// for the reconciliations in flight to finish, and the outcome is `Ok`. If
/// a part of the controller fails first, the rest is stopped with ten
/// seconds of grace and the failure is returned.
async fn run(config: Config) -> Result<()> {
    let mut controller = Controller::start(config).await?;
    let stopped = async {
        let interrupt = tokio::signal::ctrl_c();
        #[cfg(unix)]
        {
            let mut terminate =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                    .expect("SIGTERM can be listened for");
            tokio::select! {
                _ = interrupt => "interrupt",
                _ = terminate.recv() => "SIGTERM",
            }
        }
        #[cfg(not(unix))]
        {
            let _ = interrupt.await;
            "interrupt"
        }
    };
    tokio::select! {
        signal = stopped => {
            tracing::info!(signal, "told to stop");
            controller.stop(Duration::from_secs(60)).await;
            Ok(())
        }
        error = controller.failed() => {
            tracing::error!(%error, "the controller cannot carry on");
            controller.stop(Duration::from_secs(10)).await;
            Err(error)
        }
    }
}

/// Loads the configuration, sets up logging, and runs the command, which is
/// `run` when none is given.
#[tokio::main]
async fn main() {
    let arguments = Arguments::parse();
    let config = match Config::load(&arguments.config) {
        Ok(config) => config,
        Err(error) => {
            eprintln!("{error}");
            std::process::exit(2);
        }
    };
    logging(&config);
    let outcome = match arguments.command.unwrap_or(Command::Run) {
        Command::Run => run(config).await,
        Command::Check => check(config).await,
    };
    if let Err(error) = outcome {
        eprintln!("{error}");
        std::process::exit(1);
    }
}
