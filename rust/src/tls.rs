use log::info;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::ring::default_provider;
use rustls::crypto::{verify_tls12_signature, verify_tls13_signature, WebPkiSupportedAlgorithms};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime};
use rustls::{ClientConfig, DigitallySignedStruct, Error as RustlsError, RootCertStore, ServerConfig, SignatureScheme};
use sha2::{Digest, Sha256};
use std::fs::File;
use std::io::BufReader;
use std::sync::Arc;
use tokio_rustls::{TlsAcceptor, TlsConnector};

pub fn cert_fingerprint_hex(cert: &CertificateDer<'_>) -> String {
    let mut hasher = Sha256::new();
    hasher.update(cert.as_ref());
    let digest = hasher.finalize();
    digest.iter().map(|b| format!("{:02x}", b)).collect()
}

pub fn build_server_acceptor(
    cert_path: Option<&str>,
    key_path: Option<&str>,
) -> Result<(TlsAcceptor, String), Box<dyn std::error::Error>> {
    let _ = default_provider().install_default();

    let (certs, key): (Vec<CertificateDer<'static>>, PrivateKeyDer<'static>) =
        match (cert_path, key_path) {
            (Some(cp), Some(kp)) => {
                let mut cert_reader = BufReader::new(File::open(cp)?);
                let certs: Vec<CertificateDer<'static>> =
                    rustls_pemfile::certs(&mut cert_reader).collect::<Result<Vec<_>, _>>()?;
                if certs.is_empty() {
                    return Err("No certificates found in TLS cert file".into());
                }

                let mut key_reader = BufReader::new(File::open(kp)?);
                let key = rustls_pemfile::private_key(&mut key_reader)?
                    .ok_or("No private key found in TLS key file")?;
                (certs, key)
            }
            (None, None) => {
                info!("No TLS cert/key provided; generating ephemeral self-signed certificate...");
                let cert = rcgen::generate_simple_self_signed(vec![
                    "tunner".to_string(),
                    "localhost".to_string(),
                    "127.0.0.1".to_string(),
                ])?;
                let cert_der = CertificateDer::from(cert.cert.der().to_vec());
                let key_der = PrivateKeyDer::try_from(cert.key_pair.serialize_der())
                    .map_err(|e| format!("Failed to serialize generated key: {}", e))?;
                (vec![cert_der], key_der)
            }
            _ => {
                return Err("Both --tls-cert and --tls-key must be provided together".into());
            }
        };

    let fingerprint = cert_fingerprint_hex(&certs[0]);
    let config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)?;

    Ok((TlsAcceptor::from(Arc::new(config)), fingerprint))
}

pub fn build_client_connector(
    ca_path: Option<&str>,
    fingerprint: Option<&str>,
    insecure: bool,
) -> Result<TlsConnector, Box<dyn std::error::Error>> {
    let _ = default_provider().install_default();

    let config = if let Some(ca) = ca_path {
        let mut root_store = RootCertStore::empty();
        let mut reader = BufReader::new(File::open(ca)?);
        let certs: Vec<CertificateDer<'static>> =
            rustls_pemfile::certs(&mut reader).collect::<Result<Vec<_>, _>>()?;
        for cert in certs {
            root_store.add(cert)?;
        }
        ClientConfig::builder()
            .with_root_certificates(root_store)
            .with_no_client_auth()
    } else if let Some(fp) = fingerprint {
        let expected = fp.trim().to_lowercase().replace(':', "");
        let verifier = Arc::new(FingerprintVerifier {
            expected_hex: expected,
            algs: default_provider().signature_verification_algorithms,
        });
        ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(verifier)
            .with_no_client_auth()
    } else if insecure {
        let verifier = Arc::new(InsecureVerifier {
            algs: default_provider().signature_verification_algorithms,
        });
        ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(verifier)
            .with_no_client_auth()
    } else {
        let root_store = load_system_root_store()?;
        ClientConfig::builder()
            .with_root_certificates(root_store)
            .with_no_client_auth()
    };

    Ok(TlsConnector::from(Arc::new(config)))
}

fn load_system_root_store() -> Result<RootCertStore, Box<dyn std::error::Error>> {
    const SYSTEM_CA_PATHS: &[&str] = &[
        "/etc/ssl/cert.pem",
        "/etc/ssl/certs/ca-certificates.crt",
        "/etc/pki/tls/certs/ca-bundle.crt",
        "/etc/ssl/ca-bundle.pem",
        "/system/etc/security/cacerts",
    ];

    let mut root_store = RootCertStore::empty();
    for path in SYSTEM_CA_PATHS {
        let p = std::path::Path::new(path);
        if p.is_file() {
            if let Ok(file) = File::open(p) {
                let mut reader = BufReader::new(file);
                for cert in rustls_pemfile::certs(&mut reader).flatten() {
                    let _ = root_store.add(cert);
                }
            }
            if !root_store.is_empty() {
                info!("Loaded {} system root CA certificates from {}", root_store.len(), path);
                return Ok(root_store);
            }
        }
    }

    Err(
        "No system root CA bundle found; please specify --tls-ca <PEM>, --tls-fingerprint <SHA256>, or --tls-insecure"
            .into(),
    )
}

pub fn make_server_name(host: &str) -> ServerName<'static> {
    ServerName::try_from(host.to_string())
        .unwrap_or_else(|_| ServerName::try_from("tunner".to_string()).unwrap())
}

#[derive(Debug)]
struct FingerprintVerifier {
    expected_hex: String,
    algs: WebPkiSupportedAlgorithms,
}

impl ServerCertVerifier for FingerprintVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, RustlsError> {
        let actual = cert_fingerprint_hex(end_entity);
        if actual == self.expected_hex {
            Ok(ServerCertVerified::assertion())
        } else {
            Err(RustlsError::General(format!(
                "Server certificate fingerprint mismatch: expected {}, got {}",
                self.expected_hex, actual
            )))
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, RustlsError> {
        verify_tls12_signature(message, cert, dss, &self.algs)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, RustlsError> {
        verify_tls13_signature(message, cert, dss, &self.algs)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.algs.supported_schemes()
    }
}

#[derive(Debug)]
struct InsecureVerifier {
    algs: WebPkiSupportedAlgorithms,
}

impl ServerCertVerifier for InsecureVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, RustlsError> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, RustlsError> {
        verify_tls12_signature(message, cert, dss, &self.algs)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, RustlsError> {
        verify_tls13_signature(message, cert, dss, &self.algs)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.algs.supported_schemes()
    }
}
