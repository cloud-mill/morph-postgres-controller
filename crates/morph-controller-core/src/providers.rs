//! The providers through which Morph reaches resources.
//!
//! [`Providers::start`] turns the `[[providers]]` of the configuration into
//! a [`ProviderRegistry`] for Morph, and chooses the [`ExecutorOptions`]
//! that suit what was started. The workers hand both to Morph with every
//! reconciliation. Once started, the providers are called by Morph alone.

use std::path::PathBuf;
use std::sync::Arc;

use morph::{ExecutorOptions, ProviderRegistry, Value};
use morph_bridge::{Bridged, Registry, Source};
use morph_mock::MockCloud;

use crate::config::{MorphConfig, ProviderConfig};
use crate::error::{Error, Result};

/// The providers, registered, and the settings that suit them.
pub struct Providers {
    /// The registry Morph is given: every kind of resource, and the
    /// provider that handles it.
    pub registry: ProviderRegistry,
    /// How Morph is to run operations against these providers.
    pub executor: ExecutorOptions,
    /// The in-memory cloud, if there is one among the providers, so that
    /// what it holds can be looked at.
    pub mock: Option<MockCloud>,
    /// The providers written for Terraform. A provider's process is killed
    /// when the last holder lets go of it, and this is one holder. The
    /// registry is another, for every provider that offers a kind.
    _bridged: Vec<Arc<Bridged>>,
}

impl std::fmt::Debug for Providers {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Providers")
            .field("registry", &self.registry)
            .finish_non_exhaustive()
    }
}

impl Providers {
    /// Starts the providers the configuration names, in the order given.
    ///
    /// * `mock` makes a new in-memory cloud.
    /// * `terraform` asks the registry for the newest version if none is
    ///   given, fetches the provider into the cache unless it is there
    ///   already, starts it as a process of its own, and gives it its
    ///   configuration.
    ///
    /// # Executor options
    ///
    /// One set of options serves every provider. If any provider written
    /// for Terraform was started, the options are those of
    /// [`morph_bridge::executor_options`]: no limit on how long a provider
    /// call may take, and no second attempt. Such a provider waits and
    /// retries for itself, and does not recognise a creation that is asked
    /// for twice. Otherwise Morph's defaults apply. In either case
    /// `morph.operation_timeout`, if set, then replaces the time limit.
    ///
    /// # Errors
    ///
    /// [`Error::Provider`], with the cause as text, if a `source` cannot be
    /// read, the registry cannot say which version is newest, the provider
    /// cannot be fetched, started or configured, or Morph's registry
    /// refuses it, as it does a provider that offers a kind of resource
    /// which one before it already offers.
    pub async fn start(configs: &[ProviderConfig], morph: &MorphConfig) -> Result<Self> {
        let mut registry = ProviderRegistry::new();
        let mut bridged = Vec::new();
        let mut mock = None;
        for config in configs {
            match config {
                ProviderConfig::Mock => {
                    let cloud = MockCloud::new();
                    registry = registry.with(Arc::new(cloud.clone())).map_err(provider)?;
                    mock = Some(cloud);
                }
                ProviderConfig::Terraform {
                    source,
                    version,
                    cache,
                    config,
                } => {
                    let source: Source = source.parse().map_err(provider)?;
                    let cache = cache
                        .clone()
                        .unwrap_or_else(|| PathBuf::from(".morph-providers"));
                    let fetched = Registry::opentofu(cache);
                    let version = match version {
                        Some(version) => version.clone(),
                        None => fetched.newest(&source).await.map_err(provider)?,
                    };
                    tracing::info!(%source, %version, "fetching the provider, unless it is here already");
                    let program = fetched.fetch(&source, &version).await.map_err(provider)?;
                    let launched =
                        Bridged::launch(&program, from_toml(toml::Value::Table(config.clone())))
                            .await
                            .map_err(provider)?;
                    tracing::info!(%source, %version, kinds = launched.kinds().count(), "the provider is running");
                    let launched = Arc::new(launched);
                    registry = registry.with(launched.clone()).map_err(provider)?;
                    bridged.push(launched);
                }
            }
        }
        // Providers written for Terraform wait for themselves, and are not
        // to be given up on or asked again. Morph has one set of options
        // for all providers, so one such provider decides for the rest.
        let mut executor = match bridged.is_empty() {
            true => ExecutorOptions::default(),
            false => morph_bridge::executor_options(),
        };
        if let Some(timeout) = morph.operation_timeout {
            executor.operation_timeout = Some(timeout);
        }
        Ok(Self {
            registry,
            executor,
            mock,
            _bridged: bridged,
        })
    }

    /// Wraps providers that were made elsewhere, as tests do.
    ///
    /// Nothing is started, and `executor` is taken as it is given.
    pub fn ready(
        registry: ProviderRegistry,
        executor: ExecutorOptions,
        mock: Option<MockCloud>,
    ) -> Self {
        Self {
            registry,
            executor,
            mock,
            _bridged: Vec::new(),
        }
    }
}

fn provider(error: impl std::fmt::Display) -> Error {
    Error::Provider(error.to_string())
}

/// Converts a TOML value into the Morph value that a provider is
/// configured with.
///
/// A table becomes a map and an array a list. A date or a time becomes its
/// TOML text, because Morph has no value of that kind.
///
/// # Examples
///
/// ```
/// use morph::Value;
/// use morph_controller_core::providers::from_toml;
///
/// let table: toml::Table = toml::from_str(
///     r#"
///     region = "eu-west-1"
///     max_retries = 3
///     endpoints = [{ sqs = "http://127.0.0.1:5000" }]
///     "#,
/// )
/// .unwrap();
///
/// let Value::Map(config) = from_toml(toml::Value::Table(table)) else {
///     unreachable!("a table becomes a map");
/// };
/// assert_eq!(config["region"], Value::from("eu-west-1"));
/// assert_eq!(config["max_retries"], Value::from(3));
/// assert!(matches!(&config["endpoints"], Value::List(blocks) if blocks.len() == 1));
/// ```
pub fn from_toml(value: toml::Value) -> Value {
    match value {
        toml::Value::String(text) => Value::from(text),
        toml::Value::Integer(number) => Value::from(number),
        toml::Value::Float(number) => Value::from(number),
        toml::Value::Boolean(flag) => Value::from(flag),
        toml::Value::Datetime(when) => Value::from(when.to_string()),
        toml::Value::Array(items) => Value::List(items.into_iter().map(from_toml).collect()),
        toml::Value::Table(table) => Value::Map(
            table
                .into_iter()
                .map(|(name, value)| (name, from_toml(value)))
                .collect(),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn provider_configuration_becomes_a_morph_value() {
        let table: toml::Table = toml::from_str(
            "region = \"eu-west-1\"\nskip = true\nretries = 3\n[[endpoints]]\nsqs = \"http://localhost:5000\"",
        )
        .unwrap();
        let value = from_toml(toml::Value::Table(table));
        let Value::Map(map) = value else { panic!() };
        assert_eq!(map["region"], Value::from("eu-west-1"));
        assert_eq!(map["skip"], Value::from(true));
        assert_eq!(map["retries"], Value::from(3));
        assert!(matches!(&map["endpoints"], Value::List(items) if items.len() == 1));
    }
}
