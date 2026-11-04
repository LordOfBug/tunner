use bytes::Bytes;
use log::{error, info, warn};
use rand::RngCore;
use std::fs::File;
use std::io::Read;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};

pub struct NoiseSource {
    data: Vec<u8>,
    position: AtomicUsize,
}

impl NoiseSource {
    pub fn new(file_path: Option<&str>) -> Self {
        let mut loaded = Vec::new();
        if let Some(path_str) = file_path {
            if !path_str.trim().is_empty() {
                let path = Path::new(path_str);
                if path.is_file() {
                    match File::open(path) {
                        Ok(mut file) => {
                            // Read up to 16MB for memory caching
                            let mut buf = vec![0u8; 16 * 1024 * 1024];
                            match file.read(&mut buf) {
                                Ok(n) if n > 0 => {
                                    buf.truncate(n);
                                    info!("Loaded {} bytes from noise file: {}", n, path_str);
                                    loaded = buf;
                                }
                                Ok(_) => {
                                    warn!("Noise file {} is empty", path_str);
                                }
                                Err(e) => {
                                    error!("Failed reading noise file {}: {}", path_str, e);
                                }
                            }
                        }
                        Err(e) => {
                            error!("Failed opening noise file {}: {}", path_str, e);
                        }
                    }
                } else {
                    warn!("Noise file {} not found or is not a file", path_str);
                }
            }
        }

        if loaded.is_empty() {
            let mut pattern = vec![0u8; 65536];
            rand::thread_rng().fill_bytes(&mut pattern);
            info!("Initialized 64KB random noise pattern.");
            loaded = pattern;
        }

        Self {
            data: loaded,
            position: AtomicUsize::new(0),
        }
    }

    pub fn get_bytes(&self, count: usize) -> Bytes {
        let len = self.data.len();
        let mut out = vec![0u8; count];
        let mut written = 0;

        let mut pos = self.position.load(Ordering::Relaxed);
        while written < count {
            let available = len - (pos % len);
            let to_copy = (count - written).min(available);
            let start = pos % len;
            out[written..written + to_copy].copy_from_slice(&self.data[start..start + to_copy]);
            pos += to_copy;
            written += to_copy;
        }
        self.position.store(pos % len, Ordering::Relaxed);

        Bytes::from(out)
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
}

pub fn parse_rate(rate_str: &str) -> u64 {
    let s = rate_str.trim().to_lowercase();
    if s.is_empty() || s == "0" {
        return 0;
    }

    let (num_str, multiplier) = if s.ends_with("gb") {
        (&s[..s.len() - 2], 1024 * 1024 * 1024)
    } else if s.ends_with('g') {
        (&s[..s.len() - 1], 1024 * 1024 * 1024)
    } else if s.ends_with("mb") {
        (&s[..s.len() - 2], 1024 * 1024)
    } else if s.ends_with('m') {
        (&s[..s.len() - 1], 1024 * 1024)
    } else if s.ends_with("kb") {
        (&s[..s.len() - 2], 1024)
    } else if s.ends_with('k') {
        (&s[..s.len() - 1], 1024)
    } else {
        (s.as_str(), 1)
    };

    num_str
        .trim()
        .parse::<f64>()
        .map(|val| (val * multiplier as f64) as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_rate() {
        assert_eq!(parse_rate("0"), 0);
        assert_eq!(parse_rate(""), 0);
        assert_eq!(parse_rate("1k"), 1024);
        assert_eq!(parse_rate("10k"), 10 * 1024);
        assert_eq!(parse_rate("10kb"), 10 * 1024);
        assert_eq!(parse_rate("1m"), 1024 * 1024);
        assert_eq!(parse_rate("500m"), 500 * 1024 * 1024);
        assert_eq!(parse_rate("2g"), 2 * 1024 * 1024 * 1024);
    }

    #[test]
    fn test_noise_source_cyclic() {
        let src = NoiseSource {
            data: b"0123456789".to_vec(),
            position: AtomicUsize::new(0),
        };
        let b = src.get_bytes(25);
        assert_eq!(&b[..], b"0123456789012345678901234");
    }
}

