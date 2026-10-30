//! Encryption of connections to the database.
//!
//! The configuration made here is the one the ordinary connection uses, in
//! [`super::database`]. The replication client sets up its own TLS from the
//! same settings, in [`super::replication`].

use std::path::Path;
use std::sync::Arc;

use rustls_pki_types::CertificateDer;
use rustls_pki_types::pem::PemObject;

use crate::error::{Error, Result};

/// A TLS configuration that trusts the public authorities and, if given,
/// those in a PEM file.
///
/// The public authorities are the ones compiled in with `webpki-roots`, not
/// those of the operating system. No client certificate is offered.
///
/// # Errors
///
/// [`Error::Config`], naming the file, if `extra_roots` cannot be opened or
/// read as PEM, or holds a certificate that cannot serve as a trusted
/// authority.
pub fn client_config(extra_roots: Option<&Path>) -> Result<Arc<rustls::ClientConfig>> {
    let mut roots =
        rustls::RootCertStore::from_iter(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    if let Some(path) = extra_roots {
        let failed = |what: String| Error::Config(format!("{}: {what}", path.display()));
        let certificates =
            CertificateDer::pem_file_iter(path).map_err(|error| failed(error.to_string()))?;
        for certificate in certificates {
            let certificate = certificate.map_err(|error| failed(error.to_string()))?;
            roots
                .add(certificate)
                .map_err(|error| failed(error.to_string()))?;
        }
    }
    // The provider is named here, so that nothing depends on a default
    // having been installed for the whole process.
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let config = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|error| Error::Config(error.to_string()))?
        .with_root_certificates(roots)
        .with_no_client_auth();
    Ok(Arc::new(config))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_public_roots_alone_are_always_available() {
        client_config(None).expect("a client configuration over the public roots");
    }

    #[test]
    fn a_missing_or_unreadable_authority_file_is_named() {
        let dir = std::env::temp_dir().join(format!("mpc-tls-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let missing = dir.join("absent.pem");
        let error = client_config(Some(&missing)).unwrap_err();
        assert!(
            matches!(error, Error::Config(ref m) if m.contains("absent.pem")),
            "{error}"
        );
        let garbage = dir.join("garbage.pem");
        std::fs::write(
            &garbage,
            "-----BEGIN CERTIFICATE-----\nnot base64 at all!!\n-----END CERTIFICATE-----\n",
        )
        .unwrap();
        let error = client_config(Some(&garbage)).unwrap_err();
        assert!(matches!(error, Error::Config(_)), "{error}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_self_signed_authority_is_accepted_when_openssl_can_make_one() {
        let dir = std::env::temp_dir().join(format!("mpc-tls-ca-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let made = std::process::Command::new("openssl")
            .args([
                "req",
                "-x509",
                "-newkey",
                "rsa:2048",
                "-nodes",
                "-days",
                "1",
                "-subj",
                "/CN=morph-test-ca",
                "-keyout",
            ])
            .arg(dir.join("ca.key"))
            .arg("-out")
            .arg(dir.join("ca.pem"))
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status();
        match made {
            Ok(status) if status.success() => {
                client_config(Some(&dir.join("ca.pem")))
                    .expect("the authority is added to the roots");
            }
            _ => eprintln!("skipped: openssl is not available"),
        }
        let _ = std::fs::remove_dir_all(&dir);
    }
}
