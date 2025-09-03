//! TLS configuration and certificate management
//! 
//! Provides async TLS 1.3 configuration using rustls with Tokio integration.
//! Supports certificate loading, validation, and HTTPS server binding.

use crate::{Result, ServerError};
use rustls::{Certificate, PrivateKey, ServerConfig as RustlsServerConfig};
use rustls_pemfile::{certs, pkcs8_private_keys};
use std::{
    io::BufReader,
    path::{Path, PathBuf},
    sync::{atomic::{AtomicBool, Ordering}, Arc},
};
use tokio::fs;
use tracing::{info, warn};

/// TLS configuration for the server
#[derive(Debug, Clone)]
pub struct TlsConfig {
    /// Path to the certificate file (PEM format)
    pub cert_path: PathBuf,
    /// Path to the private key file (PEM format)
    pub key_path: PathBuf,
    /// Whether to enable TLS (atomic for runtime updates)
    pub enabled: Arc<AtomicBool>,
    /// Whether to require client certificates
    pub require_client_cert: bool,
    /// Path to CA certificates for client verification (optional)
    pub ca_cert_path: Option<PathBuf>,
}

impl Default for TlsConfig {
    fn default() -> Self {
        Self {
            cert_path: PathBuf::from("./certs/server.crt"),
            key_path: PathBuf::from("./certs/server.key"),
            enabled: Arc::new(AtomicBool::new(false)),
            require_client_cert: false,
            ca_cert_path: None,
        }
    }
}

impl TlsConfig {
    /// Create TLS configuration from environment variables
    pub fn from_env() -> Result<Self> {
        let cert_path = std::env::var("VOLEBARN_TLS_CERT_PATH")
            .unwrap_or_else(|_| "./certs/server.crt".to_string())
            .into();
            
        let key_path = std::env::var("VOLEBARN_TLS_KEY_PATH")
            .unwrap_or_else(|_| "./certs/server.key".to_string())
            .into();
            
        let enabled = std::env::var("VOLEBARN_TLS_ENABLED")
            .unwrap_or_else(|_| "false".to_string())
            .parse::<bool>()
            .map_err(|e| ServerError::TlsConfig {
                error: format!("Invalid TLS enabled value: {}", e),
            })?;
            
        let require_client_cert = std::env::var("VOLEBARN_TLS_REQUIRE_CLIENT_CERT")
            .unwrap_or_else(|_| "false".to_string())
            .parse::<bool>()
            .map_err(|e| ServerError::TlsConfig {
                error: format!("Invalid require client cert value: {}", e),
            })?;
            
        let ca_cert_path = std::env::var("VOLEBARN_TLS_CA_CERT_PATH")
            .ok()
            .map(PathBuf::from);

        Ok(Self {
            cert_path,
            key_path,
            enabled: Arc::new(AtomicBool::new(enabled)),
            require_client_cert,
            ca_cert_path,
        })
    }
    
    /// Check if TLS is enabled
    pub fn is_enabled(&self) -> bool {
        self.enabled.load(Ordering::Relaxed)
    }
    
    /// Enable or disable TLS at runtime
    pub fn set_enabled(&self, enabled: bool) {
        self.enabled.store(enabled, Ordering::Relaxed);
        if enabled {
            info!("TLS enabled");
        } else {
            info!("TLS disabled");
        }
    }
    
    /// Validate that certificate and key files exist and are readable
    pub async fn validate(&self) -> Result<()> {
        if !self.is_enabled() {
            return Ok(());
        }
        
        // Check certificate file
        if !self.cert_path.exists() {
            return Err(ServerError::TlsConfig {
                error: format!("Certificate file not found: {}", self.cert_path.display()),
            });
        }
        
        // Check private key file
        if !self.key_path.exists() {
            return Err(ServerError::TlsConfig {
                error: format!("Private key file not found: {}", self.key_path.display()),
            });
        }
        
        // Check CA certificate file if specified
        if let Some(ca_path) = &self.ca_cert_path {
            if !ca_path.exists() {
                return Err(ServerError::TlsConfig {
                    error: format!("CA certificate file not found: {}", ca_path.display()),
                });
            }
        }
        
        // Try to load and parse certificates to validate format
        self.load_certificates().await?;
        self.load_private_key().await?;
        
        info!("TLS configuration validated successfully");
        Ok(())
    }
}

/// TLS certificate manager for loading and validating certificates
pub struct TlsCertificateManager {
    config: TlsConfig,
}

impl TlsCertificateManager {
    /// Create a new certificate manager with the given configuration
    pub fn new(config: TlsConfig) -> Self {
        Self { config }
    }
    
    /// Load server certificates from PEM file
    pub async fn load_certificates(&self) -> Result<Vec<Certificate>> {
        let cert_file = fs::File::open(&self.config.cert_path).await
            .map_err(|e| ServerError::TlsConfig {
                error: format!("Failed to open certificate file {}: {}", 
                    self.config.cert_path.display(), e),
            })?;
            
        let cert_file = cert_file.into_std().await;
        let mut cert_reader = BufReader::new(cert_file);
        
        let cert_chain = certs(&mut cert_reader)
            .map_err(|e| ServerError::TlsConfig {
                error: format!("Failed to parse certificate file {}: {}", 
                    self.config.cert_path.display(), e),
            })?
            .into_iter()
            .map(Certificate)
            .collect::<Vec<_>>();
            
        if cert_chain.is_empty() {
            return Err(ServerError::TlsConfig {
                error: format!("No certificates found in file: {}", 
                    self.config.cert_path.display()),
            });
        }
        
        info!("Loaded {} certificate(s) from {}", 
            cert_chain.len(), self.config.cert_path.display());
        
        Ok(cert_chain)
    }
    
    /// Load private key from PEM file
    pub async fn load_private_key(&self) -> Result<PrivateKey> {
        let key_file = fs::File::open(&self.config.key_path).await
            .map_err(|e| ServerError::TlsConfig {
                error: format!("Failed to open private key file {}: {}", 
                    self.config.key_path.display(), e),
            })?;
            
        let key_file = key_file.into_std().await;
        let mut key_reader = BufReader::new(key_file);
        
        let mut keys = pkcs8_private_keys(&mut key_reader)
            .map_err(|e| ServerError::TlsConfig {
                error: format!("Failed to parse private key file {}: {}", 
                    self.config.key_path.display(), e),
            })?;
            
        if keys.is_empty() {
            return Err(ServerError::TlsConfig {
                error: format!("No private keys found in file: {}", 
                    self.config.key_path.display()),
            });
        }
        
        if keys.len() > 1 {
            warn!("Multiple private keys found, using the first one");
        }
        
        let private_key = PrivateKey(keys.remove(0));
        
        info!("Loaded private key from {}", self.config.key_path.display());
        
        Ok(private_key)
    }
    
    /// Load CA certificates for client verification (if configured)
    pub async fn load_ca_certificates(&self) -> Result<Option<Vec<Certificate>>> {
        let ca_path = match &self.config.ca_cert_path {
            Some(path) => path,
            None => return Ok(None),
        };
        
        let ca_file = fs::File::open(ca_path).await
            .map_err(|e| ServerError::TlsConfig {
                error: format!("Failed to open CA certificate file {}: {}", 
                    ca_path.display(), e),
            })?;
            
        let ca_file = ca_file.into_std().await;
        let mut ca_reader = BufReader::new(ca_file);
        
        let ca_certs = certs(&mut ca_reader)
            .map_err(|e| ServerError::TlsConfig {
                error: format!("Failed to parse CA certificate file {}: {}", 
                    ca_path.display(), e),
            })?
            .into_iter()
            .map(Certificate)
            .collect::<Vec<_>>();
            
        if ca_certs.is_empty() {
            return Err(ServerError::TlsConfig {
                error: format!("No CA certificates found in file: {}", 
                    ca_path.display()),
            });
        }
        
        info!("Loaded {} CA certificate(s) from {}", 
            ca_certs.len(), ca_path.display());
        
        Ok(Some(ca_certs))
    }
    
    /// Create rustls ServerConfig with TLS 1.3
    pub async fn create_server_config(&self) -> Result<Arc<RustlsServerConfig>> {
        let cert_chain = self.load_certificates().await?;
        let private_key = self.load_private_key().await?;
        
        let config = RustlsServerConfig::builder()
            .with_safe_default_cipher_suites()
            .with_safe_default_kx_groups()
            .with_protocol_versions(&[&rustls::version::TLS13])
            .map_err(|e| ServerError::TlsConfig {
                error: format!("Failed to create TLS config builder: {}", e),
            })?;
            
        // Configure client certificate verification if required
        let config = if self.config.require_client_cert {
            let ca_certs = self.load_ca_certificates().await?
                .ok_or_else(|| ServerError::TlsConfig {
                    error: "Client certificate verification required but no CA certificates configured".to_string(),
                })?;
                
            let mut root_store = rustls::RootCertStore::empty();
            for ca_cert in ca_certs {
                root_store.add(&ca_cert)
                    .map_err(|e| ServerError::TlsConfig {
                        error: format!("Failed to add CA certificate to root store: {}", e),
                    })?;
            }
            
            let client_cert_verifier = Arc::new(rustls::server::AllowAnyAuthenticatedClient::new(root_store));
                
            config.with_client_cert_verifier(client_cert_verifier)
        } else {
            config.with_no_client_auth()
        };
        
        let server_config = config
            .with_single_cert(cert_chain, private_key)
            .map_err(|e| ServerError::TlsConfig {
                error: format!("Failed to configure TLS with certificate and key: {}", e),
            })?;
            
        info!("TLS 1.3 server configuration created successfully");
        
        Ok(Arc::new(server_config))
    }
}

/// Helper functions for TLS configuration
impl TlsConfig {
    /// Load certificates using the certificate manager
    pub async fn load_certificates(&self) -> Result<Vec<Certificate>> {
        let manager = TlsCertificateManager::new(self.clone());
        manager.load_certificates().await
    }
    
    /// Load private key using the certificate manager
    pub async fn load_private_key(&self) -> Result<PrivateKey> {
        let manager = TlsCertificateManager::new(self.clone());
        manager.load_private_key().await
    }
    
    /// Create rustls server configuration
    pub async fn create_server_config(&self) -> Result<Arc<RustlsServerConfig>> {
        let manager = TlsCertificateManager::new(self.clone());
        manager.create_server_config().await
    }
}

/// Generate self-signed certificate for testing purposes
#[cfg(feature = "self-signed-certs")]
pub async fn generate_self_signed_cert(cert_path: &Path, key_path: &Path) -> Result<()> {
    use rcgen::{Certificate as RcgenCertificate, CertificateParams, DistinguishedName};
    
    let mut params = CertificateParams::new(vec!["localhost".to_string()]);
    params.distinguished_name = DistinguishedName::new();
    params.distinguished_name.push(rcgen::DnType::CommonName, "Volebarn Server");
    params.distinguished_name.push(rcgen::DnType::OrganizationName, "Volebarn");
    
    let cert = RcgenCertificate::from_params(params)
        .map_err(|e| ServerError::TlsConfig {
            error: format!("Failed to generate self-signed certificate: {}", e),
        })?;
    
    // Create certificate directory if it doesn't exist
    if let Some(parent) = cert_path.parent() {
        fs::create_dir_all(parent).await
            .map_err(|e| ServerError::TlsConfig {
                error: format!("Failed to create certificate directory {}: {}", 
                    parent.display(), e),
            })?;
    }
    
    // Write certificate file
    fs::write(cert_path, cert.serialize_pem().map_err(|e| ServerError::TlsConfig {
        error: format!("Failed to serialize certificate: {}", e),
    })?)
    .await
    .map_err(|e| ServerError::TlsConfig {
        error: format!("Failed to write certificate file {}: {}", 
            cert_path.display(), e),
    })?;
    
    // Write private key file
    fs::write(key_path, cert.serialize_private_key_pem())
    .await
    .map_err(|e| ServerError::TlsConfig {
        error: format!("Failed to write private key file {}: {}", 
            key_path.display(), e),
    })?;
    
    info!("Generated self-signed certificate: {}", cert_path.display());
    info!("Generated private key: {}", key_path.display());
    
    Ok(())
}

/// Generate self-signed certificate for testing purposes (stub when feature disabled)
#[cfg(not(feature = "self-signed-certs"))]
pub async fn generate_self_signed_cert(_cert_path: &Path, _key_path: &Path) -> Result<()> {
    Err(ServerError::TlsConfig {
        error: "Self-signed certificate generation not available. Enable 'self-signed-certs' feature.".to_string(),
    })
}