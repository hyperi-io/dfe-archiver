// Project:   dfe-archiver
// File:      src/storage/backend.rs
// Purpose:   Storage backend trait and implementations
// Language:  Rust
//
// License:      FSL-1.1-ALv2
// Copyright:    (c) 2026 HyperI Pty Ltd

use crate::config::{ArchiveConfig, MinioConfig, S3Config};
use crate::{Error, Result};
use async_trait::async_trait;
use bytes::Bytes;
use object_store::aws::AmazonS3Builder;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::fs::{File, OpenOptions};
use tokio::io::AsyncWriteExt;
use tracing::{debug, info, warn};

/// Storage backend trait
#[async_trait]
pub trait StorageBackend: Send + Sync {
    /// Create a new file/object
    async fn create(&self, path: &str) -> Result<()>;

    /// Append data to existing file/object
    async fn append(&self, path: &str, data: &[u8]) -> Result<()>;

    /// Close file/object (finalise upload)
    async fn close(&self, path: &str) -> Result<()>;

    /// Check if path exists
    async fn exists(&self, path: &str) -> Result<bool>;

    /// Delete file/object
    async fn delete(&self, path: &str) -> Result<()>;

    /// Get backend name
    fn name(&self) -> &'static str;
}

/// Local filesystem backend
pub struct FileBackend {
    base_path: PathBuf,
}

impl FileBackend {
    /// Create new file backend
    pub fn new(base_path: impl Into<PathBuf>) -> Self {
        Self {
            base_path: base_path.into(),
        }
    }

    fn full_path(&self, path: &str) -> PathBuf {
        self.base_path.join(path)
    }
}

#[async_trait]
impl StorageBackend for FileBackend {
    async fn create(&self, path: &str) -> Result<()> {
        let full_path = self.full_path(path);

        // Create parent directories
        if let Some(parent) = full_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        // Create file
        File::create(&full_path).await?;
        debug!(path = %full_path.display(), "Created file");

        Ok(())
    }

    async fn append(&self, path: &str, data: &[u8]) -> Result<()> {
        let full_path = self.full_path(path);

        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&full_path)
            .await?;

        file.write_all(data).await?;
        file.flush().await?;

        Ok(())
    }

    async fn close(&self, path: &str) -> Result<()> {
        // For local files, nothing special needed
        debug!(path = %path, "File closed");
        Ok(())
    }

    async fn exists(&self, path: &str) -> Result<bool> {
        let full_path = self.full_path(path);
        Ok(full_path.exists())
    }

    async fn delete(&self, path: &str) -> Result<()> {
        let full_path = self.full_path(path);
        tokio::fs::remove_file(&full_path).await?;
        Ok(())
    }

    fn name(&self) -> &'static str {
        "file"
    }
}

/// S3/MinIO backend using object_store
///
/// Uses buffered writes since S3 doesn't support true append operations.
/// Data is accumulated in memory and uploaded on close().
pub struct S3Backend {
    store: Arc<dyn ObjectStore>,
    prefix: String,
    /// In-memory buffers for objects being written (path -> data)
    buffers: Mutex<HashMap<String, Vec<u8>>>,
}

impl S3Backend {
    /// Create new S3 backend with AWS S3
    pub fn new_s3(config: &S3Config, prefix: String) -> Result<Self> {
        let mut builder = AmazonS3Builder::new()
            .with_bucket_name(&config.bucket)
            .with_allow_http(true); // Allow HTTP for testing

        if let Some(ref region) = config.region {
            builder = builder.with_region(region);
        }

        if let Some(ref endpoint) = config.endpoint {
            builder = builder.with_endpoint(endpoint);
        }

        if let Some(ref key_id) = config.access_key_id {
            builder = builder.with_access_key_id(key_id);
        }

        if let Some(ref secret) = config.secret_access_key {
            builder = builder.with_secret_access_key(secret);
        }

        let store = builder
            .build()
            .map_err(|e| Error::Storage(format!("failed to create S3 client: {e}")))?;

        info!(bucket = %config.bucket, prefix = %prefix, "S3 backend initialized");

        Ok(Self {
            store: Arc::new(store),
            prefix,
            buffers: Mutex::new(HashMap::new()),
        })
    }

    /// Create new S3 backend for MinIO
    pub fn new_minio(config: &MinioConfig, prefix: String) -> Result<Self> {
        let scheme = if config.use_ssl { "https" } else { "http" };
        let endpoint = if config.endpoint.starts_with("http") {
            config.endpoint.clone()
        } else {
            format!("{scheme}://{}", config.endpoint)
        };

        let builder = AmazonS3Builder::new()
            .with_bucket_name(&config.bucket)
            .with_endpoint(&endpoint)
            .with_access_key_id(&config.access_key)
            .with_secret_access_key(&config.secret_key)
            .with_region("us-east-1") // MinIO doesn't care but object_store requires it
            .with_allow_http(!config.use_ssl);

        let store = builder
            .build()
            .map_err(|e| Error::Storage(format!("failed to create MinIO client: {e}")))?;

        info!(
            endpoint = %endpoint,
            bucket = %config.bucket,
            prefix = %prefix,
            "MinIO backend initialized"
        );

        Ok(Self {
            store: Arc::new(store),
            prefix,
            buffers: Mutex::new(HashMap::new()),
        })
    }

    fn object_path(&self, path: &str) -> ObjectPath {
        if self.prefix.is_empty() {
            ObjectPath::from(path)
        } else {
            ObjectPath::from(format!("{}/{}", self.prefix, path))
        }
    }
}

#[async_trait]
impl StorageBackend for S3Backend {
    async fn create(&self, path: &str) -> Result<()> {
        // Initialize empty buffer for this path
        let mut buffers = self.buffers.lock();
        buffers.insert(path.to_string(), Vec::new());
        debug!(path = %path, "S3: created buffer for new object");
        Ok(())
    }

    async fn append(&self, path: &str, data: &[u8]) -> Result<()> {
        let mut buffers = self.buffers.lock();

        let buffer = buffers.entry(path.to_string()).or_insert_with(Vec::new);

        buffer.extend_from_slice(data);

        debug!(
            path = %path,
            bytes = data.len(),
            total = buffer.len(),
            "S3: appended to buffer"
        );

        Ok(())
    }

    async fn close(&self, path: &str) -> Result<()> {
        // Take buffer out and upload
        let data = {
            let mut buffers = self.buffers.lock();
            buffers.remove(path)
        };

        let Some(data) = data else {
            warn!(path = %path, "S3: close called but no buffer found");
            return Ok(());
        };

        if data.is_empty() {
            debug!(path = %path, "S3: skipping upload of empty object");
            return Ok(());
        }

        let object_path = self.object_path(path);
        let bytes = Bytes::from(data);
        let size = bytes.len();

        self.store
            .put(&object_path, PutPayload::from_bytes(bytes))
            .await
            .map_err(|e| Error::Storage(format!("S3 upload failed: {e}")))?;

        info!(
            path = %object_path,
            bytes = size,
            "S3: uploaded object"
        );

        Ok(())
    }

    async fn exists(&self, path: &str) -> Result<bool> {
        let object_path = self.object_path(path);

        match self.store.head(&object_path).await {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(e) => Err(Error::Storage(format!("S3 head failed: {e}"))),
        }
    }

    async fn delete(&self, path: &str) -> Result<()> {
        let object_path = self.object_path(path);

        self.store
            .delete(&object_path)
            .await
            .map_err(|e| Error::Storage(format!("S3 delete failed: {e}")))?;

        debug!(path = %object_path, "S3: deleted object");
        Ok(())
    }

    fn name(&self) -> &'static str {
        "s3"
    }
}

/// Create storage backend from destination URL
pub fn create_backend(config: &ArchiveConfig) -> Result<Box<dyn StorageBackend + Send + Sync>> {
    let dest = &config.destination;

    if dest.starts_with("file://") {
        let path = dest.strip_prefix("file://").unwrap_or(dest);
        Ok(Box::new(FileBackend::new(path)))
    } else if dest.starts_with("s3://") {
        // Parse s3://bucket/prefix
        let rest = dest.strip_prefix("s3://").unwrap_or("");
        let (bucket, prefix) = parse_bucket_prefix(rest);

        // Use provided S3 config or build from URL
        let s3_config = config.s3.clone().unwrap_or_else(|| S3Config {
            bucket: bucket.to_string(),
            ..Default::default()
        });

        Ok(Box::new(S3Backend::new_s3(&s3_config, prefix.to_string())?))
    } else if dest.starts_with("minio://") {
        // Parse minio://bucket/prefix
        let rest = dest.strip_prefix("minio://").unwrap_or("");
        let (bucket, prefix) = parse_bucket_prefix(rest);

        // MinIO config is required
        let minio_config = config.minio.clone().ok_or_else(|| {
            Error::Config("minio:// destination requires minio config section".to_string())
        })?;

        // Override bucket from URL if different
        let mut minio_config = minio_config;
        if !bucket.is_empty() && minio_config.bucket.is_empty() {
            minio_config.bucket = bucket.to_string();
        }

        Ok(Box::new(S3Backend::new_minio(
            &minio_config,
            prefix.to_string(),
        )?))
    } else {
        // Default to file backend
        Ok(Box::new(FileBackend::new(dest)))
    }
}

/// Parse bucket and prefix from URL path
fn parse_bucket_prefix(path: &str) -> (&str, &str) {
    match path.find('/') {
        Some(idx) => (&path[..idx], &path[idx + 1..]),
        None => (path, ""),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_file_backend_roundtrip() {
        let temp_dir = TempDir::new().expect("create temp dir");
        let backend = FileBackend::new(temp_dir.path());

        // Create and write
        backend.create("test/file.txt").await.expect("create");
        backend
            .append("test/file.txt", b"hello ")
            .await
            .expect("append 1");
        backend
            .append("test/file.txt", b"world")
            .await
            .expect("append 2");
        backend.close("test/file.txt").await.expect("close");

        // Verify
        assert!(backend.exists("test/file.txt").await.expect("exists"));

        let content = std::fs::read(temp_dir.path().join("test/file.txt")).expect("read");
        assert_eq!(content, b"hello world");

        // Delete
        backend.delete("test/file.txt").await.expect("delete");
        assert!(!backend.exists("test/file.txt").await.expect("not exists"));
    }

    #[tokio::test]
    async fn test_parse_bucket_prefix() {
        assert_eq!(parse_bucket_prefix("bucket"), ("bucket", ""));
        assert_eq!(parse_bucket_prefix("bucket/prefix"), ("bucket", "prefix"));
        assert_eq!(
            parse_bucket_prefix("bucket/path/to/prefix"),
            ("bucket", "path/to/prefix")
        );
        assert_eq!(parse_bucket_prefix(""), ("", ""));
    }
}
