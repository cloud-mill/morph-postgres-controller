//! Starting providers from the configuration: Morph's mock cloud, and a
//! provider written for Terraform when one is in the cache.
//!
//! The Terraform case needs `hashicorp/local` fetched into
//! `../morph/target/providers`, as Morph's own examples leave it, and says
//! that it was skipped otherwise. Nothing here reaches the network.

use std::path::PathBuf;
use std::time::Duration;

use morph::ExecutorOptions;
use morph_postgres_controller::config::{MorphConfig, ProviderConfig};
use morph_postgres_controller::providers::Providers;

fn provider(text: &str) -> ProviderConfig {
    toml::from_str(text).unwrap()
}

#[tokio::test]
async fn the_mock_cloud_is_started_and_offers_its_kinds() {
    let providers = Providers::start(&[provider("kind = \"mock\"")], &MorphConfig::default())
        .await
        .unwrap();
    assert!(providers.mock.is_some());
    assert!(providers.registry.schemas().kinds().count() >= 3);
    assert_eq!(
        providers.executor.operation_timeout,
        ExecutorOptions::default().operation_timeout,
        "Morph's own options without a provider written for Terraform"
    );
    assert!(format!("{providers:?}").contains("Providers"));
}

#[tokio::test]
async fn two_providers_offering_the_same_kinds_are_refused() {
    let error = Providers::start(
        &[provider("kind = \"mock\""), provider("kind = \"mock\"")],
        &MorphConfig::default(),
    )
    .await
    .unwrap_err();
    assert!(
        matches!(error, morph_postgres_controller::Error::Provider(_)),
        "{error}"
    );
}

#[tokio::test]
async fn a_source_that_is_not_an_address_is_refused_before_anything_is_fetched() {
    let error = Providers::start(
        &[provider(
            "kind = \"terraform\"\nsource = \"not an address at all\"\nversion = \"1.0.0\"",
        )],
        &MorphConfig::default(),
    )
    .await
    .unwrap_err();
    assert!(
        matches!(error, morph_postgres_controller::Error::Provider(_)),
        "{error}"
    );
}

/// The cache that Morph's examples fill, and the newest version of the
/// `local` provider in it.
fn cached_local() -> Option<(PathBuf, String)> {
    let cache = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../morph/target/providers");
    let versions = cache.join("registry.opentofu.org/hashicorp/local");
    let mut found: Vec<String> = std::fs::read_dir(&versions)
        .ok()?
        .flatten()
        .filter(|entry| entry.path().is_dir())
        .map(|entry| entry.file_name().to_string_lossy().into_owned())
        .collect();
    found.sort();
    Some((cache, found.pop()?))
}

#[tokio::test]
async fn a_provider_written_for_terraform_starts_from_the_cache_and_sets_the_executor() {
    let Some((cache, version)) = cached_local() else {
        eprintln!("skipped: hashicorp/local is not in ../morph/target/providers");
        return;
    };
    let text = format!(
        "kind = \"terraform\"\nsource = \"hashicorp/local\"\nversion = \"{version}\"\ncache = \"{}\"",
        cache.display()
    );
    let providers = Providers::start(&[provider(&text)], &MorphConfig::default())
        .await
        .expect("the cached provider starts");
    let kinds: Vec<String> = providers
        .registry
        .schemas()
        .kinds()
        .map(|kind| kind.to_string())
        .collect();
    assert!(kinds.iter().any(|kind| kind == "local_file"), "{kinds:?}");
    assert!(providers.mock.is_none());
    assert_eq!(
        providers.executor.operation_timeout, None,
        "a provider written for Terraform waits for itself"
    );

    let limited = MorphConfig {
        operation_timeout: Some(Duration::from_secs(900)),
        ..MorphConfig::default()
    };
    let providers = Providers::start(&[provider(&text)], &limited)
        .await
        .expect("the cached provider starts again");
    assert_eq!(
        providers.executor.operation_timeout,
        Some(Duration::from_secs(900)),
        "a configured limit replaces the provider's"
    );
}
