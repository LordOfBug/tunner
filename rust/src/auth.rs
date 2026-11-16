use log::{debug, warn};
use rand::RngCore;
use ssh_key::{AuthorizedKeys, HashAlg, LineEnding, PrivateKey, PublicKey, SshSig};
use std::path::{Path, PathBuf};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub const AUTH_NAMESPACE: &str = "tunner-auth";
pub const AUTH_NOT_REQUIRED: u8 = 0x00;
pub const AUTH_REQUIRED: u8 = 0x01;

const STATUS_REJECT: u8 = 0x00;
const STATUS_OK: u8 = 0x01;
const STATUS_TRY_NEXT: u8 = 0x02;
const MAX_AUTH_ATTEMPTS: usize = 8;

pub fn load_authorized_keys(path: &Path) -> Result<Vec<PublicKey>, Box<dyn std::error::Error>> {
    let entries = AuthorizedKeys::read_file(path)?;
    let keys: Vec<PublicKey> = entries.into_iter().map(|e| e.public_key().clone()).collect();
    if keys.is_empty() {
        return Err(format!("No valid SSH public keys found in {}", path.display()).into());
    }
    Ok(keys)
}

pub fn load_private_key(
    path: &Path,
    passphrase: Option<&str>,
) -> Result<PrivateKey, Box<dyn std::error::Error>> {
    let key = PrivateKey::read_openssh_file(path)?;
    if key.is_encrypted() {
        let pass = match passphrase {
            Some(p) => p.to_string(),
            None => std::env::var("TUNNER_KEY_PASSPHRASE").map_err(|_| {
                format!(
                    "SSH private key {} is encrypted; pass --passphrase or set TUNNER_KEY_PASSPHRASE",
                    path.display()
                )
            })?,
        };
        Ok(key.decrypt(pass)?)
    } else {
        Ok(key)
    }
}

/// Discover default private keys from ~/.ssh/ (like OpenSSH `ssh` does).
pub fn discover_default_keys(passphrase: Option<&str>) -> Vec<(PathBuf, PrivateKey)> {
    let mut found = Vec::new();
    let home = std::env::var_os("HOME").or_else(|| std::env::var_os("USERPROFILE"));
    let Some(home_dir) = home else {
        return found;
    };

    let ssh_dir = PathBuf::from(home_dir).join(".ssh");
    for name in ["id_ed25519", "id_ecdsa", "id_rsa", "id_dsa"] {
        let path = ssh_dir.join(name);
        if path.is_file() {
            match load_private_key(&path, passphrase) {
                Ok(key) => {
                    debug!("Discovered SSH private key: {}", path.display());
                    found.push((path, key));
                }
                Err(e) => {
                    warn!("Skipping {}: {}", path.display(), e);
                }
            }
        }
    }
    found
}

pub async fn server_send_no_auth<S: AsyncWrite + Unpin>(
    stream: &mut S,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    stream.write_all(&[AUTH_NOT_REQUIRED]).await?;
    stream.flush().await?;
    Ok(())
}

pub async fn server_authenticate<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
    authorized_keys: &[PublicKey],
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let mut challenge = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut challenge);

    stream.write_all(&[AUTH_REQUIRED]).await?;
    stream.write_all(&challenge).await?;
    stream.flush().await?;

    for attempt in 0..MAX_AUTH_ATTEMPTS {
        let mut len_buf = [0u8; 2];
        stream.read_exact(&mut len_buf).await?;
        let sig_len = u16::from_be_bytes(len_buf) as usize;

        if sig_len == 0 || sig_len > 16384 {
            let _ = stream.write_all(&[STATUS_REJECT]).await;
            let _ = stream.flush().await;
            return Err("No valid SSH key signature provided".into());
        }

        let mut sig_bytes = vec![0u8; sig_len];
        stream.read_exact(&mut sig_bytes).await?;

        let sig = match SshSig::from_pem(&sig_bytes) {
            Ok(s) => s,
            Err(e) => {
                let _ = stream.write_all(&[STATUS_REJECT]).await;
                let _ = stream.flush().await;
                return Err(format!("Failed to parse SSH signature: {}", e).into());
            }
        };

        let key_data = sig.public_key();
        let pub_key = PublicKey::new(key_data.clone(), "");
        let is_authorized = authorized_keys
            .iter()
            .any(|allowed| allowed.key_data() == key_data);

        if is_authorized && pub_key.verify(AUTH_NAMESPACE, &challenge, &sig).is_ok() {
            stream.write_all(&[STATUS_OK]).await?;
            stream.flush().await?;
            return Ok(pub_key.fingerprint(HashAlg::Sha256).to_string());
        }

        let has_more = attempt + 1 < MAX_AUTH_ATTEMPTS;
        let reply = if has_more { STATUS_TRY_NEXT } else { STATUS_REJECT };
        stream.write_all(&[reply]).await?;
        stream.flush().await?;

        if !has_more {
            return Err("Exceeded max SSH key authentication attempts".into());
        }
    }

    Err("SSH key authentication failed".into())
}

pub async fn client_authenticate<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
    candidate_keys: &[(PathBuf, PrivateKey)],
) -> Result<Option<PathBuf>, Box<dyn std::error::Error + Send + Sync>> {
    let mut mode = [0u8; 1];
    stream.read_exact(&mut mode).await?;

    if mode[0] == AUTH_NOT_REQUIRED {
        return Ok(None);
    }
    if mode[0] != AUTH_REQUIRED {
        return Err(format!("Unexpected auth mode byte from server: {}", mode[0]).into());
    }

    let mut challenge = [0u8; 32];
    stream.read_exact(&mut challenge).await?;

    if candidate_keys.is_empty() {
        let _ = stream.write_all(&0u16.to_be_bytes()).await;
        let _ = stream.flush().await;
        return Err(
            "Server requires SSH key authentication, but no usable private keys were found (pass -k <KEY> or add keys to ~/.ssh/)"
                .into(),
        );
    }

    for (path, private_key) in candidate_keys {
        let sig = private_key.sign(AUTH_NAMESPACE, HashAlg::Sha512, &challenge)?;
        let pem = sig.to_pem(LineEnding::LF)?;
        let pem_bytes = pem.as_bytes();

        if pem_bytes.len() > u16::MAX as usize {
            continue;
        }

        stream
            .write_all(&(pem_bytes.len() as u16).to_be_bytes())
            .await?;
        stream.write_all(pem_bytes).await?;
        stream.flush().await?;

        let mut status = [0u8; 1];
        stream.read_exact(&mut status).await?;
        match status[0] {
            STATUS_OK => return Ok(Some(path.clone())),
            STATUS_TRY_NEXT => {
                debug!("Server did not accept key {}, trying next...", path.display());
                continue;
            }
            _ => {
                return Err("Server rejected SSH key authentication".into());
            }
        }
    }

    Err("None of the candidate SSH private keys matched server's authorized_keys".into())
}
