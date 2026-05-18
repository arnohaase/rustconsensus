//! Shared demo-only identity helper for the cluster examples.
//!
//! Examples mint nothing themselves; instead they all use the same
//! hard-coded self-signed Ed25519 cert and key. Every demo node thus shares
//! one identity and trusts itself, so they can authenticate each other on a
//! single host without any operator setup.
//!
//! THIS IS NOT FOR PRODUCTION. In production each node has its own cert and
//! the operator distributes SPKI hashes.

use rcgen::{CertificateParams, DistinguishedName, KeyPair};
use rustc_hash::FxHashSet;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};

use cluster::messaging::quic::spki_verifier::{SpkiHash, spki_hash_of_cert};

const DEMO_KEY_PEM: &str = "\
-----BEGIN PRIVATE KEY-----
MFECAQEwBQYDK2VwBCIEIBKkZyKIP7pC06/OBiRwdsns69angO1XWYt94rXBwvci
gSEABr4eMMSVMxcEunNapASYqY3ySfLkKoCJEPrWXbmnfl4=
-----END PRIVATE KEY-----
";

/// Build the shared demo identity. Returns (cert, key, single-element trust set).
pub fn demo_identity() -> (
    CertificateDer<'static>,
    PrivateKeyDer<'static>,
    FxHashSet<SpkiHash>,
) {
    let key_pair = KeyPair::from_pem(DEMO_KEY_PEM).expect("demo key parses");
    let mut params = CertificateParams::new(vec!["demo.cluster".to_string()]).unwrap();
    params.distinguished_name = DistinguishedName::new();
    let cert = params
        .self_signed(&key_pair)
        .expect("demo cert self-signs");
    let cert_der = CertificateDer::from(cert.der().to_vec());
    let key_der = PrivateKeyDer::try_from(
        KeyPair::from_pem(DEMO_KEY_PEM)
            .unwrap()
            .serialize_der(),
    )
    .expect("demo key DER");
    let spki = spki_hash_of_cert(&cert_der).expect("demo spki hash");
    let mut trusted = FxHashSet::default();
    trusted.insert(spki);
    (cert_der, key_der, trusted)
}
