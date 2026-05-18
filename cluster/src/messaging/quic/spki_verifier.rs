//! SPKI-pinning certificate verifiers for QUIC mutual TLS.
//!
//! Each cluster node has a long-lived self-signed cert. The shared cluster
//! secret is the set of trusted SPKI SHA-256 hashes. Both sides verify the
//! peer's leaf cert by computing its SPKI hash and checking membership in
//! the trusted set. CA, hostname, chain, and validity are all ignored.
//! Handshake-transcript signatures still prove possession of the private key.

use std::sync::Arc;

use anyhow::anyhow;
use rustc_hash::FxHashSet;
use rustls::DigitallySignedStruct;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::server::danger::{ClientCertVerified, ClientCertVerifier};
use rustls::{DistinguishedName, SignatureScheme};
use sha2::{Digest, Sha256};

pub type SpkiHash = [u8; 32];

#[derive(Debug, Clone)]
pub struct TrustedSpki(pub Arc<FxHashSet<SpkiHash>>);

impl TrustedSpki {
    pub fn new<I: IntoIterator<Item = SpkiHash>>(hashes: I) -> Self {
        Self(Arc::new(hashes.into_iter().collect()))
    }

    fn contains(&self, h: &SpkiHash) -> bool {
        self.0.contains(h)
    }
}

pub fn spki_hash_of_cert(cert: &CertificateDer<'_>) -> anyhow::Result<SpkiHash> {
    let ee = webpki::EndEntityCert::try_from(cert)
        .map_err(|e| anyhow!("parse leaf cert: {e:?}"))?;
    let spki = ee.subject_public_key_info();
    let mut h = Sha256::new();
    h.update(spki.as_ref());
    Ok(h.finalize().into())
}

fn hex(b: &[u8]) -> String {
    let mut s = String::with_capacity(b.len() * 2);
    for x in b {
        s.push_str(&format!("{:02x}", x));
    }
    s
}

// --- server-side: verify the client's cert by SPKI pinning ---

#[derive(Debug)]
pub struct SpkiClientVerifier {
    trusted: TrustedSpki,
    schemes: Vec<SignatureScheme>,
}

impl SpkiClientVerifier {
    pub fn new(trusted: TrustedSpki, provider: &Arc<rustls::crypto::CryptoProvider>) -> Self {
        Self {
            trusted,
            schemes: provider.signature_verification_algorithms.supported_schemes(),
        }
    }
}

impl ClientCertVerifier for SpkiClientVerifier {
    fn root_hint_subjects(&self) -> &[DistinguishedName] {
        &[]
    }

    fn verify_client_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _now: UnixTime,
    ) -> Result<ClientCertVerified, rustls::Error> {
        let h = spki_hash_of_cert(end_entity)
            .map_err(|e| rustls::Error::General(format!("spki: {e}")))?;
        if self.trusted.contains(&h) {
            Ok(ClientCertVerified::assertion())
        } else {
            Err(rustls::Error::General(format!(
                "client SPKI not in trusted set: {}",
                hex(&h)
            )))
        }
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Err(rustls::Error::PeerIncompatible(
            rustls::PeerIncompatible::Tls12NotOffered,
        ))
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &rustls::crypto::ring::default_provider().signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.schemes.clone()
    }
}

// --- client-side: verify the server's cert by SPKI pinning ---

#[derive(Debug)]
pub struct SpkiServerVerifier {
    trusted: TrustedSpki,
    schemes: Vec<SignatureScheme>,
}

impl SpkiServerVerifier {
    pub fn new(trusted: TrustedSpki, provider: &Arc<rustls::crypto::CryptoProvider>) -> Self {
        Self {
            trusted,
            schemes: provider.signature_verification_algorithms.supported_schemes(),
        }
    }
}

impl ServerCertVerifier for SpkiServerVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        let h = spki_hash_of_cert(end_entity)
            .map_err(|e| rustls::Error::General(format!("spki: {e}")))?;
        if self.trusted.contains(&h) {
            Ok(ServerCertVerified::assertion())
        } else {
            Err(rustls::Error::General(format!(
                "server SPKI not in trusted set: {}",
                hex(&h)
            )))
        }
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Err(rustls::Error::PeerIncompatible(
            rustls::PeerIncompatible::Tls12NotOffered,
        ))
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &rustls::crypto::ring::default_provider().signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.schemes.clone()
    }
}
