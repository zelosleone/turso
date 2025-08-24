#![allow(unused_variables, dead_code)]
use crate::{LimboError, Result};
use aes_gcm::{
    aead::{Aead, AeadCore, KeyInit, OsRng},
    Aes256Gcm, Key, Nonce,
};
use std::ops::Deref;

pub const ENCRYPTION_METADATA_SIZE: usize = 28;
pub const ENCRYPTED_PAGE_SIZE: usize = 4096;
pub const ENCRYPTION_NONCE_SIZE: usize = 12;
pub const ENCRYPTION_TAG_SIZE: usize = 16;

#[repr(transparent)]
#[derive(Clone)]
pub struct EncryptionKey([u8; 32]);

impl EncryptionKey {
    pub fn new(key: [u8; 32]) -> Self {
        Self(key)
    }

    pub fn from_string(s: &str) -> Self {
        let mut key = [0u8; 32];
        let bytes = s.as_bytes();
        let len = bytes.len().min(32);
        key[..len].copy_from_slice(&bytes[..len]);
        Self(key)
    }

    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.0
    }
}

impl Deref for EncryptionKey {
    type Target = [u8; 32];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl AsRef<[u8; 32]> for EncryptionKey {
    fn as_ref(&self) -> &[u8; 32] {
        &self.0
    }
}

impl std::fmt::Debug for EncryptionKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EncryptionKey")
            .field("key", &"<encryption key redacted>")
            .finish()
    }
}

impl Drop for EncryptionKey {
    fn drop(&mut self) {
        // securely zero out the key bytes before dropping
        for byte in self.0.iter_mut() {
            unsafe {
                std::ptr::write_volatile(byte, 0);
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CipherMode {
    Aes256Gcm,
}

impl CipherMode {
    /// Every cipher requires a specific key size. For 256-bit algorithms, this is 32 bytes.
    /// For 128-bit algorithms, it would be 16 bytes, etc.
    pub fn required_key_size(&self) -> usize {
        match self {
            CipherMode::Aes256Gcm => 32,
        }
    }

    /// Returns the nonce size for this cipher mode. Though most AEAD ciphers use 12-byte nonces.
    pub fn nonce_size(&self) -> usize {
        match self {
            CipherMode::Aes256Gcm => ENCRYPTION_NONCE_SIZE,
        }
    }

    /// Returns the authentication tag size for this cipher mode. All common AEAD ciphers use 16-byte tags.
    pub fn tag_size(&self) -> usize {
        match self {
            CipherMode::Aes256Gcm => ENCRYPTION_TAG_SIZE,
        }
    }
}

#[derive(Clone)]
pub enum Cipher {
    Aes256Gcm(Box<Aes256Gcm>),
}

impl std::fmt::Debug for Cipher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Cipher::Aes256Gcm(_) => write!(f, "Cipher::Aes256Gcm"),
        }
    }
}

#[derive(Clone)]
pub struct PerConnEncryptionContext {
    cipher_mode: CipherMode,
    cipher: Cipher,
}

impl PerConnEncryptionContext {
    pub fn new(key: &EncryptionKey) -> Result<Self> {
        let cipher_mode = CipherMode::Aes256Gcm;
        let required_size = cipher_mode.required_key_size();
        if key.as_slice().len() != required_size {
            return Err(crate::LimboError::InvalidArgument(format!(
                "Invalid key size for {:?}: expected {} bytes, got {}",
                cipher_mode,
                required_size,
                key.as_slice().len()
            )));
        }

        let cipher = match cipher_mode {
            CipherMode::Aes256Gcm => {
                let cipher_key: &Key<Aes256Gcm> = key.as_ref().into();
                Cipher::Aes256Gcm(Box::new(Aes256Gcm::new(cipher_key)))
            }
        };
        Ok(Self {
            cipher_mode,
            cipher,
        })
    }

    pub fn cipher_mode(&self) -> CipherMode {
        self.cipher_mode
    }

    #[cfg(feature = "encryption")]
    pub fn encrypt_page(&self, page: &[u8], page_id: usize) -> Result<Vec<u8>> {
        if page_id == 1 {
            tracing::debug!("skipping encryption for page 1 (database header)");
            return Ok(page.to_vec());
        }
        tracing::debug!("encrypting page {}", page_id);
        assert_eq!(
            page.len(),
            ENCRYPTED_PAGE_SIZE,
            "Page data must be exactly {ENCRYPTED_PAGE_SIZE} bytes"
        );
        let reserved_bytes = &page[ENCRYPTED_PAGE_SIZE - ENCRYPTION_METADATA_SIZE..];
        let reserved_bytes_zeroed = reserved_bytes.iter().all(|&b| b == 0);
        assert!(
            reserved_bytes_zeroed,
            "last reserved bytes must be empty/zero, but found non-zero bytes"
        );
        let payload = &page[..ENCRYPTED_PAGE_SIZE - ENCRYPTION_METADATA_SIZE];
        let (encrypted, nonce) = self.encrypt_raw(payload)?;

        assert_eq!(
            encrypted.len(),
            ENCRYPTED_PAGE_SIZE - nonce.len(),
            "Encrypted page must be exactly {} bytes",
            ENCRYPTED_PAGE_SIZE - nonce.len()
        );
        let mut result = Vec::with_capacity(ENCRYPTED_PAGE_SIZE);
        result.extend_from_slice(&encrypted);
        result.extend_from_slice(&nonce);
        assert_eq!(
            result.len(),
            ENCRYPTED_PAGE_SIZE,
            "Encrypted page must be exactly {ENCRYPTED_PAGE_SIZE} bytes"
        );
        Ok(result)
    }

    #[cfg(feature = "encryption")]
    pub fn decrypt_page(&self, encrypted_page: &[u8], page_id: usize) -> Result<Vec<u8>> {
        if page_id == 1 {
            tracing::debug!("skipping decryption for page 1 (database header)");
            return Ok(encrypted_page.to_vec());
        }
        tracing::debug!("decrypting page {}", page_id);
        assert_eq!(
            encrypted_page.len(),
            ENCRYPTED_PAGE_SIZE,
            "Encrypted page data must be exactly {ENCRYPTED_PAGE_SIZE} bytes"
        );

        let nonce_start = encrypted_page.len() - ENCRYPTION_NONCE_SIZE;
        let payload = &encrypted_page[..nonce_start];
        let nonce = &encrypted_page[nonce_start..];

        let decrypted_data = self.decrypt_raw(payload, nonce)?;

        assert_eq!(
            decrypted_data.len(),
            ENCRYPTED_PAGE_SIZE - ENCRYPTION_METADATA_SIZE,
            "Decrypted page data must be exactly {} bytes",
            ENCRYPTED_PAGE_SIZE - ENCRYPTION_METADATA_SIZE
        );
        let mut result = Vec::with_capacity(ENCRYPTED_PAGE_SIZE);
        result.extend_from_slice(&decrypted_data);
        result.resize(ENCRYPTED_PAGE_SIZE, 0);
        assert_eq!(
            result.len(),
            ENCRYPTED_PAGE_SIZE,
            "Decrypted page data must be exactly {ENCRYPTED_PAGE_SIZE} bytes"
        );
        Ok(result)
    }

    /// encrypts raw data using the configured cipher, returns ciphertext and nonce
    fn encrypt_raw(&self, plaintext: &[u8]) -> Result<(Vec<u8>, Vec<u8>)> {
        match &self.cipher {
            Cipher::Aes256Gcm(cipher) => {
                let nonce = Aes256Gcm::generate_nonce(&mut OsRng);
                let ciphertext = cipher
                    .encrypt(&nonce, plaintext)
                    .map_err(|e| LimboError::InternalError(format!("Encryption failed: {e:?}")))?;
                Ok((ciphertext, nonce.to_vec()))
            }
        }
    }

    fn decrypt_raw(&self, ciphertext: &[u8], nonce: &[u8]) -> Result<Vec<u8>> {
        match &self.cipher {
            Cipher::Aes256Gcm(cipher) => {
                let nonce = Nonce::from_slice(nonce);
                let plaintext = cipher.decrypt(nonce, ciphertext).map_err(|e| {
                    crate::LimboError::InternalError(format!("Decryption failed: {e:?}"))
                })?;
                Ok(plaintext)
            }
        }
    }

    #[cfg(not(feature = "encryption"))]
    pub fn encrypt_page(&self, _page: &[u8], _page_id: usize) -> Result<Vec<u8>> {
        Err(LimboError::InvalidArgument(
            "encryption is not enabled, cannot encrypt page. enable via passing `--features encryption`".into(),
        ))
    }

    #[cfg(not(feature = "encryption"))]
    pub fn decrypt_page(&self, _encrypted_page: &[u8], _page_id: usize) -> Result<Vec<u8>> {
        Err(LimboError::InvalidArgument(
            "encryption is not enabled, cannot decrypt page. enable via passing `--features encryption`".into(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::Rng;

    #[test]
    #[cfg(feature = "encryption")]
    fn test_encrypt_decrypt_round_trip() {
        let mut rng = rand::thread_rng();
        let data_size = ENCRYPTED_PAGE_SIZE - ENCRYPTION_METADATA_SIZE;

        let page_data = {
            let mut page = vec![0u8; ENCRYPTED_PAGE_SIZE];
            page.iter_mut()
                .take(data_size)
                .for_each(|byte| *byte = rng.gen());
            page
        };

        let key = EncryptionKey::from_string("alice and bob use encryption on database");
        let ctx = PerConnEncryptionContext::new(&key).unwrap();

        let page_id = 42;
        let encrypted = ctx.encrypt_page(&page_data, page_id).unwrap();
        assert_eq!(encrypted.len(), ENCRYPTED_PAGE_SIZE);
        assert_ne!(&encrypted[..data_size], &page_data[..data_size]);
        assert_ne!(&encrypted[..], &page_data[..]);

        let decrypted = ctx.decrypt_page(&encrypted, page_id).unwrap();
        assert_eq!(decrypted.len(), ENCRYPTED_PAGE_SIZE);
        assert_eq!(decrypted, page_data);
    }
}
