// Project:   dfe-archiver
// File:      crates/io/src/storage.rs
// Purpose:   Storage backend implementations (file, S3, MinIO, GCS, Azure)
// Language:  Rust
//
// License:      BUSL-1.1
// Copyright:    (c) 2026 HyperI Pty Ltd

use async_trait::async_trait;
use dfe_archiver_core::config::{ArchiveConfig, AzureConfig, GcsConfig, MinioConfig, S3Config};
use dfe_archiver_core::storage::StorageBackend;
use dfe_archiver_core::{Error, Result};
use futures::{StreamExt, TryStreamExt};
use object_store::aws::AmazonS3Builder;
use object_store::azure::MicrosoftAzureBuilder;
use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectStoreExt, WriteMultipart};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;
use tracing::{debug, info, trace, warn};

/// Default multipart chunk size: 8MB
const DEFAULT_MULTIPART_CHUNK_SIZE: usize = 8 * 1024 * 1024;

/// Default max concurrent part uploads per file
const DEFAULT_MAX_CONCURRENCY: usize = 8;

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

        if let Some(parent) = full_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&full_path)
            .await
            .map_err(|e| match e.kind() {
                std::io::ErrorKind::AlreadyExists => Error::AlreadyExists {
                    path: path.to_string(),
                },
                _ => Error::Io(e),
            })?;
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

        trace!(path = %full_path.display(), bytes = data.len(), "Appended to file");

        Ok(())
    }

    async fn close(&self, path: &str) -> Result<()> {
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

    async fn list_prefix(&self, prefix: &str, limit: Option<usize>) -> Result<Vec<String>> {
        let search_dir = self.full_path(prefix);
        let base = &self.base_path;
        let mut results = Vec::new();

        let walk_dir = if search_dir.is_dir() {
            search_dir.clone()
        } else {
            search_dir.parent().unwrap_or(&search_dir).to_path_buf()
        };

        if walk_dir.exists() {
            let mut stack = vec![walk_dir];
            'walk: while let Some(dir) = stack.pop() {
                let mut entries = tokio::fs::read_dir(&dir).await?;
                while let Some(entry) = entries.next_entry().await? {
                    let path = entry.path();
                    if path.is_dir() {
                        stack.push(path);
                    } else if let Ok(rel) = path.strip_prefix(base) {
                        let rel_str = rel.to_string_lossy().to_string();
                        if rel_str.starts_with(prefix) {
                            results.push(rel_str);
                            if let Some(cap) = limit
                                && results.len() >= cap
                            {
                                break 'walk;
                            }
                        }
                    }
                }
            }
        }

        Ok(results)
    }

    fn name(&self) -> &'static str {
        "file"
    }
}

/// Cloud object store backend using multipart uploads
///
/// Supports S3, `MinIO`, GCS, and Azure Blob via the `object_store` crate.
/// Uses `WriteMultipart` for streaming uploads — data is uploaded in
/// configurable chunk sizes (default 8MB), keeping memory usage bounded
/// to ~`chunk_size` per active file rather than buffering the entire file.
pub struct ObjectStoreBackend {
    store: Arc<dyn ObjectStore>,
    prefix: String,
    backend_name: &'static str,
    chunk_size: usize,
    max_concurrency: usize,
    /// Active multipart uploads (path -> `WriteMultipart`)
    uploads: Mutex<HashMap<String, WriteMultipart>>,
}

impl ObjectStoreBackend {
    /// Create backend for AWS S3
    pub fn new_s3(config: &S3Config, prefix: String, chunk_size: usize) -> Result<Self> {
        // `allow_http` defaults to false (HTTPS required). Only enable for local/dev endpoints.
        let mut builder = AmazonS3Builder::from_env()
            .with_bucket_name(&config.bucket)
            .with_allow_http(config.allow_http);

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
            builder = builder.with_secret_access_key(secret.expose());
        }

        let store = builder
            .build()
            .map_err(|e| Error::storage_with("failed to create S3 client", e))?;

        info!(bucket = %config.bucket, prefix = %prefix, chunk_size, "S3 backend initialized");

        Ok(Self {
            store: Arc::new(store),
            prefix,
            backend_name: "s3",
            chunk_size,
            max_concurrency: DEFAULT_MAX_CONCURRENCY,
            uploads: Mutex::new(HashMap::new()),
        })
    }

    /// Create backend for `MinIO` (S3-compatible)
    pub fn new_minio(config: &MinioConfig, prefix: String, chunk_size: usize) -> Result<Self> {
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
            .with_secret_access_key(config.secret_key.expose())
            .with_region("us-east-1") // MinIO doesn't care but object_store requires it
            .with_allow_http(!config.use_ssl);

        let store = builder
            .build()
            .map_err(|e| Error::storage_with("failed to create MinIO client", e))?;

        info!(
            endpoint = %endpoint,
            bucket = %config.bucket,
            prefix = %prefix,
            "MinIO backend initialized"
        );

        Ok(Self {
            store: Arc::new(store),
            prefix,
            backend_name: "minio",
            chunk_size,
            max_concurrency: DEFAULT_MAX_CONCURRENCY,
            uploads: Mutex::new(HashMap::new()),
        })
    }

    /// Create backend for Google Cloud Storage
    ///
    /// GCS credentials are resolved via (in order):
    /// 1. `service_account_key` (inline JSON key)
    /// 2. `credentials_path` (path to service account JSON file)
    /// 3. `GOOGLE_APPLICATION_CREDENTIALS` env var (Application Default Credentials)
    /// 4. Instance metadata (when running on GCP)
    pub fn new_gcs(config: &GcsConfig, prefix: String, chunk_size: usize) -> Result<Self> {
        let mut builder = GoogleCloudStorageBuilder::from_env().with_bucket_name(&config.bucket);

        if let Some(ref key) = config.service_account_key {
            builder = builder.with_service_account_key(key.expose());
        }
        if let Some(ref path) = config.credentials_path {
            builder = builder.with_service_account_path(path);
        }

        let store = builder
            .build()
            .map_err(|e| Error::storage_with("failed to create GCS client", e))?;

        info!(bucket = %config.bucket, prefix = %prefix, "GCS backend initialized");

        Ok(Self {
            store: Arc::new(store),
            prefix,
            backend_name: "gcs",
            chunk_size,
            max_concurrency: DEFAULT_MAX_CONCURRENCY,
            uploads: Mutex::new(HashMap::new()),
        })
    }

    /// Create backend for Azure Blob Storage
    pub fn new_azure(config: &AzureConfig, prefix: String, chunk_size: usize) -> Result<Self> {
        let mut builder = MicrosoftAzureBuilder::from_env()
            .with_account(&config.account_name)
            .with_container_name(&config.container);

        if let Some(ref key) = config.account_key {
            builder = builder.with_access_key(key.expose());
        }
        if let Some(ref sas) = config.sas_token {
            builder = builder.with_sas_authorization(parse_sas_pairs(sas.expose()));
        }
        // `use_emulator` and `endpoint` cannot both apply. object_store's
        // emulator branch takes its URL from the `AZURITE_BLOB_STORAGE_URL` env
        // var (default `http://127.0.0.1:10000`) and never reads
        // `with_endpoint`, so setting both used to drop the endpoint in silence
        // and address whatever answered on the local default port -- or, with
        // nothing there, `<account>.blob.core.windows.net` out on the internet,
        // which resolves for `devstoreaccount1` and returns 403.
        //
        // So an explicit endpoint wins. An emulator behind one needs its own
        // credential, because there is no managed identity to fall back on and
        // the emulator's key is not ours to assume.
        if let Some(ref endpoint) = config.endpoint {
            if config.use_emulator && config.account_key.is_none() && config.sas_token.is_none() {
                return Err(Error::Config(
                    "azure.use_emulator together with azure.endpoint needs \
                     azure.account_key or azure.sas_token (for Azurite, its published \
                     devstoreaccount1 key); or drop azure.endpoint and let \
                     AZURITE_BLOB_STORAGE_URL address the emulator"
                        .to_string(),
                ));
            }
            builder = builder
                .with_endpoint(endpoint.clone())
                .with_allow_http(true);
        } else if config.use_emulator {
            builder = builder.with_use_emulator(true);
        }

        let store = builder
            .build()
            .map_err(|e| Error::storage_with("failed to create Azure client", e))?;

        info!(
            account = %config.account_name,
            container = %config.container,
            prefix = %prefix,
            "Azure Blob backend initialized"
        );

        Ok(Self {
            store: Arc::new(store),
            prefix,
            backend_name: "azure",
            chunk_size,
            max_concurrency: DEFAULT_MAX_CONCURRENCY,
            uploads: Mutex::new(HashMap::new()),
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

/// Parse SAS token query string into key-value pairs.
///
/// Values are percent-decoded so that Azure SDK receives the raw signature
/// bytes (SAS tokens often contain `%3D`, `%20`, etc.).
fn parse_sas_pairs(sas: &str) -> Vec<(String, String)> {
    let sas = sas.strip_prefix('?').unwrap_or(sas);
    sas.split('&')
        .filter_map(|pair| {
            let mut parts = pair.splitn(2, '=');
            let key = parts.next()?;
            let value = parts.next().unwrap_or("");
            let decoded_value = percent_encoding::percent_decode_str(value)
                .decode_utf8_lossy()
                .into_owned();
            Some((key.to_string(), decoded_value))
        })
        .collect()
}

#[async_trait]
impl StorageBackend for ObjectStoreBackend {
    async fn create(&self, path: &str) -> Result<()> {
        let object_path = self.object_path(path);

        if self.exists(path).await? {
            return Err(Error::AlreadyExists {
                path: path.to_string(),
            });
        }

        let upload = self.store.put_multipart(&object_path).await.map_err(|e| {
            Error::storage_with(
                format!("{}: multipart init failed for {path}", self.backend_name),
                e,
            )
        })?;

        let write = WriteMultipart::new_with_chunk_size(upload, self.chunk_size);

        let mut uploads = self.uploads.lock().await;
        uploads.insert(path.to_string(), write);

        debug!(path = %path, backend = self.backend_name, "Started multipart upload");
        Ok(())
    }

    async fn append(&self, path: &str, data: &[u8]) -> Result<()> {
        let mut uploads = self.uploads.lock().await;

        let write = uploads.get_mut(path).ok_or_else(|| {
            Error::storage(format!(
                "{}: no active upload for path: {path}",
                self.backend_name
            ))
        })?;

        write.write(data);

        write
            .wait_for_capacity(self.max_concurrency)
            .await
            .map_err(|e| {
                Error::storage_with(
                    format!("{}: part upload failed for {path}", self.backend_name),
                    e,
                )
            })?;

        debug!(
            path = %path,
            bytes = data.len(),
            backend = self.backend_name,
            "Appended to multipart upload"
        );

        Ok(())
    }

    async fn close(&self, path: &str) -> Result<()> {
        let write = {
            let mut uploads = self.uploads.lock().await;
            uploads.remove(path)
        };

        let Some(write) = write else {
            warn!(
                path = %path,
                backend = self.backend_name,
                "close called but no active upload"
            );
            return Ok(());
        };

        debug!(path = %path, backend = self.backend_name, "Completing multipart upload");
        let start = std::time::Instant::now();
        write.finish().await.map_err(|e| {
            Error::storage_with(
                format!(
                    "{}: multipart complete failed for {path}",
                    self.backend_name
                ),
                e,
            )
        })?;

        let duration = start.elapsed();
        info!(
            path = %path,
            backend = self.backend_name,
            duration_ms = duration.as_millis(),
            "Completed multipart upload"
        );
        Ok(())
    }

    async fn exists(&self, path: &str) -> Result<bool> {
        let object_path = self.object_path(path);
        match self.store.head(&object_path).await {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(e) => Err(Error::storage_with(
                format!("{}: head failed for {path}", self.backend_name),
                e,
            )),
        }
    }

    async fn delete(&self, path: &str) -> Result<()> {
        let object_path = self.object_path(path);
        self.store.delete(&object_path).await.map_err(|e| {
            Error::storage_with(
                format!("{}: delete failed for {path}", self.backend_name),
                e,
            )
        })?;
        debug!(path = %object_path, backend = self.backend_name, "Deleted object");
        Ok(())
    }

    async fn list_prefix(&self, prefix: &str, limit: Option<usize>) -> Result<Vec<String>> {
        let object_prefix = self.object_path(prefix);
        let list_prefix = Some(&object_prefix);

        // Stream the list and stop early at `limit` rather than collecting
        // everything first. For S3/GCS/Azure this lets the caller cap the
        // number of API pages fetched (object_store internally paginates).
        let stream = self.store.list(list_prefix);
        let stream = if let Some(cap) = limit {
            futures::StreamExt::take(stream, cap).boxed()
        } else {
            stream
        };

        let objects: Vec<_> = stream.try_collect().await.map_err(|e| {
            Error::storage_with(
                format!("{}: list failed for prefix {prefix}", self.backend_name),
                e,
            )
        })?;

        Ok(objects
            .into_iter()
            .map(|meta| meta.location.to_string())
            .collect())
    }

    fn name(&self) -> &'static str {
        self.backend_name
    }
}

/// Create storage backend from destination URL
pub fn create_backend(config: &ArchiveConfig) -> Result<Box<dyn StorageBackend + Send + Sync>> {
    let dest = &config.destination;
    let chunk_size = if config.multipart_chunk_size > 0 {
        config.multipart_chunk_size
    } else {
        DEFAULT_MULTIPART_CHUNK_SIZE
    };

    if dest.starts_with("file://") {
        let path = dest.strip_prefix("file://").unwrap_or(dest);
        Ok(Box::new(FileBackend::new(path)))
    } else if dest.starts_with("s3://") {
        let rest = dest.strip_prefix("s3://").unwrap_or("");
        let (bucket, prefix) = parse_bucket_prefix(rest);

        let s3_config = config.s3.clone().unwrap_or_else(|| S3Config {
            bucket: bucket.to_string(),
            ..Default::default()
        });

        Ok(Box::new(ObjectStoreBackend::new_s3(
            &s3_config,
            prefix.to_string(),
            chunk_size,
        )?))
    } else if dest.starts_with("minio://") {
        let rest = dest.strip_prefix("minio://").unwrap_or("");
        let (bucket, prefix) = parse_bucket_prefix(rest);

        let mut minio_config = config.minio.clone().ok_or_else(|| {
            Error::Config("minio:// destination requires minio config section".to_string())
        })?;

        if !bucket.is_empty() && minio_config.bucket.is_empty() {
            minio_config.bucket = bucket.to_string();
        }

        Ok(Box::new(ObjectStoreBackend::new_minio(
            &minio_config,
            prefix.to_string(),
            chunk_size,
        )?))
    } else if dest.starts_with("gs://") || dest.starts_with("gcs://") {
        // Both spellings, because `ArchiveConfig::backend_name()` labels either
        // as "gcs" and a destination must mean the same thing to both.
        let rest = dest
            .strip_prefix("gs://")
            .or_else(|| dest.strip_prefix("gcs://"))
            .unwrap_or("");
        let (bucket, prefix) = parse_bucket_prefix(rest);

        let gcs_config = config.gcs.clone().unwrap_or_else(|| GcsConfig {
            bucket: bucket.to_string(),
            ..Default::default()
        });

        Ok(Box::new(ObjectStoreBackend::new_gcs(
            &gcs_config,
            prefix.to_string(),
            chunk_size,
        )?))
    } else if dest.starts_with("az://") || dest.starts_with("azure://") {
        let rest = if let Some(r) = dest.strip_prefix("az://") {
            r
        } else {
            dest.strip_prefix("azure://").unwrap_or("")
        };
        let (container, prefix) = parse_bucket_prefix(rest);

        let mut azure_config = config.azure.clone().unwrap_or_else(|| AzureConfig {
            container: container.to_string(),
            ..Default::default()
        });

        if !container.is_empty() && azure_config.container.is_empty() {
            azure_config.container = container.to_string();
        }

        Ok(Box::new(ObjectStoreBackend::new_azure(
            &azure_config,
            prefix.to_string(),
            chunk_size,
        )?))
    } else if dest.contains("://") {
        // URL-shaped but not a scheme this implements. The local-path branch
        // below would create a directory named after the URL and archive into
        // it, so a typo in `archive.destination` writes to ephemeral pod disk
        // instead of the bucket with nothing downstream able to tell.
        Err(Error::Config(format!(
            "unsupported archive.destination scheme in '{dest}' \
             (supported: file://, s3://, minio://, gs://, gcs://, az://, azure://, \
             or a bare local path)"
        )))
    } else {
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
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_file_backend_roundtrip() {
        let temp_dir = TempDir::new().expect("create temp dir");
        let backend = FileBackend::new(temp_dir.path());

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

        assert!(backend.exists("test/file.txt").await.expect("exists"));

        let content = std::fs::read(temp_dir.path().join("test/file.txt")).expect("read");
        assert_eq!(content, b"hello world");

        backend.delete("test/file.txt").await.expect("delete");
        assert!(!backend.exists("test/file.txt").await.expect("not exists"));
    }

    #[tokio::test]
    async fn test_file_backend_create_refuses_to_clobber() {
        let temp_dir = TempDir::new().expect("create temp dir");
        let backend = FileBackend::new(temp_dir.path());

        backend.create("hour/06-0001.jsonl").await.expect("create");
        backend
            .append("hour/06-0001.jsonl", b"before_restart")
            .await
            .expect("append");
        backend.close("hour/06-0001.jsonl").await.expect("close");

        let err = backend
            .create("hour/06-0001.jsonl")
            .await
            .expect_err("second create must not clobber");
        assert!(
            matches!(err, Error::AlreadyExists { .. }),
            "expected AlreadyExists, got {err:?}"
        );

        let content = std::fs::read(temp_dir.path().join("hour/06-0001.jsonl")).expect("read");
        assert_eq!(content, b"before_restart", "existing file was truncated");
    }

    #[tokio::test]
    async fn test_file_backend_list_prefix_honours_limit() {
        let temp_dir = TempDir::new().expect("create temp dir");
        let backend = FileBackend::new(temp_dir.path());

        for i in 0..10 {
            let path = format!("data/{i:02}.jsonl");
            backend.create(&path).await.expect("create");
            backend
                .append(&path, format!("row{i}\n").as_bytes())
                .await
                .expect("append");
            backend.close(&path).await.expect("close");
        }

        // Unbounded — gets all entries.
        let all = backend.list_prefix("data/", None).await.expect("list all");
        assert_eq!(all.len(), 10, "expected all 10 entries with limit=None");

        // Capped — stops walking as soon as cap is met.
        let capped = backend
            .list_prefix("data/", Some(3))
            .await
            .expect("list capped");
        assert_eq!(capped.len(), 3, "expected exactly 3 entries with limit=3");

        // Limit larger than result set returns everything available.
        let big_cap = backend
            .list_prefix("data/", Some(100))
            .await
            .expect("list big cap");
        assert_eq!(big_cap.len(), 10, "limit > available returns all");
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

    #[test]
    fn test_parse_sas_pairs() {
        let pairs = parse_sas_pairs("?sv=2021-08-06&ss=b&srt=sco&sp=rwdlacup");
        assert_eq!(pairs.len(), 4);
        assert_eq!(pairs[0], ("sv".to_string(), "2021-08-06".to_string()));
        assert_eq!(pairs[1], ("ss".to_string(), "b".to_string()));

        let pairs = parse_sas_pairs("sv=2021-08-06&ss=b");
        assert_eq!(pairs.len(), 2);
    }

    #[test]
    fn test_parse_sas_pairs_url_decodes_values() {
        // Real SAS tokens contain percent-encoded signature/timestamp values
        let pairs = parse_sas_pairs("?sig=abc%2Bdef%2Fghi%3D&se=2026-04-16T12%3A00%3A00Z&sp=rwdl");
        assert_eq!(pairs.len(), 3);
        assert_eq!(pairs[0], ("sig".to_string(), "abc+def/ghi=".to_string()));
        assert_eq!(
            pairs[1],
            ("se".to_string(), "2026-04-16T12:00:00Z".to_string())
        );
        assert_eq!(pairs[2], ("sp".to_string(), "rwdl".to_string()));
    }

    #[test]
    fn test_parse_sas_pairs_empty_value() {
        let pairs = parse_sas_pairs("sv=&key=value");
        assert_eq!(pairs.len(), 2);
        assert_eq!(pairs[0], ("sv".to_string(), String::new()));
        assert_eq!(pairs[1], ("key".to_string(), "value".to_string()));
    }

    #[test]
    fn test_create_backend_file_url() {
        let config = ArchiveConfig {
            destination: "file:///tmp/test-archive".to_string(),
            ..Default::default()
        };
        let backend = create_backend(&config).expect("create file backend");
        assert_eq!(backend.name(), "file");
    }

    #[test]
    fn test_create_backend_bare_path_falls_back_to_file() {
        let config = ArchiveConfig {
            destination: "/tmp/bare-path".to_string(),
            ..Default::default()
        };
        let backend = create_backend(&config).expect("bare path -> file backend");
        assert_eq!(backend.name(), "file");
    }

    #[test]
    fn test_create_backend_minio_requires_config() {
        let config = ArchiveConfig {
            destination: "minio://bucket/prefix".to_string(),
            minio: None,
            ..Default::default()
        };
        let result = create_backend(&config);
        assert!(result.is_err(), "minio:// without config should fail");
    }

    /// `ArchiveConfig::backend_name()` accepts `gcs://` as a GCS destination and
    /// labels every sink metric `backend="gcs"`, so `create_backend` has to
    /// agree. Matching only `gs://` sends `gcs://bucket/prefix` to the catch-all
    /// and archives into a local directory of that name -- data bound for a
    /// cloud bucket on ephemeral pod disk, with the metrics claiming GCS.
    #[test]
    fn test_create_backend_agrees_with_backend_name_on_gcs_alias() {
        let config = ArchiveConfig {
            destination: "gcs://some-bucket/prefix".to_string(),
            ..Default::default()
        };
        assert_eq!(config.backend_name(), "gcs", "backend_name claims gcs");

        match create_backend(&config) {
            // No GCS credentials here, so a client build error is acceptable.
            Err(_) => {}
            Ok(backend) => assert_eq!(
                backend.name(),
                "gcs",
                "gcs:// must not be silently downgraded to a local directory"
            ),
        }
    }

    /// `use_emulator` plus an `endpoint` used to archive somewhere else entirely.
    ///
    /// `object_store`'s emulator branch reads `AZURITE_BLOB_STORAGE_URL` and never
    /// looks at `with_endpoint`, so the endpoint was dropped in silence and the
    /// client addressed `http://127.0.0.1:10000` -- or, failing that,
    /// `devstoreaccount1.blob.core.windows.net`, which really resolves. Rather
    /// than guess which the operator meant, say so.
    #[test]
    fn test_azure_emulator_with_endpoint_needs_a_credential() {
        let config = AzureConfig {
            account_name: "devstoreaccount1".to_string(),
            account_key: None,
            sas_token: None,
            container: "archive-test".to_string(),
            use_emulator: true,
            endpoint: Some("http://127.0.0.1:32769/devstoreaccount1".to_string()),
        };
        let err = ObjectStoreBackend::new_azure(&config, String::new(), 8 * 1024 * 1024)
            .err()
            .expect("use_emulator with an endpoint and no credential must be rejected");
        let message = err.to_string();
        assert!(
            message.contains("azure.account_key"),
            "the error must name the setting that fixes it, got: {message}"
        );
    }

    /// The same pair WITH a credential is the Azurite-on-a-random-port case the
    /// e2e tests use, and it must build.
    #[test]
    fn test_azure_emulator_with_endpoint_and_key_builds() {
        let config = AzureConfig {
            account_name: "devstoreaccount1".to_string(),
            account_key: Some(dfe_archiver_core::config::sensitive::SensitiveString::from(
                "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==".to_string(),
            )),
            sas_token: None,
            container: "archive-test".to_string(),
            use_emulator: true,
            endpoint: Some("http://127.0.0.1:32769/devstoreaccount1".to_string()),
        };
        let backend = ObjectStoreBackend::new_azure(&config, String::new(), 8 * 1024 * 1024)
            .expect("emulator behind an explicit endpoint with a key must build");
        assert_eq!(backend.name(), "azure");
    }

    /// A URL-shaped destination with an unsupported scheme must be rejected.
    /// Accepting it as a relative local path archives a mistyped
    /// `archive.destination` to pod-local disk, and neither startup, validation
    /// nor a metric says the cloud bucket went unused. Bare paths (no `://`)
    /// stay valid.
    #[test]
    fn test_create_backend_rejects_unknown_url_scheme() {
        for dest in ["S3://upper-bucket", "blob://bucket/x", "http://host/path"] {
            let config = ArchiveConfig {
                destination: dest.to_string(),
                ..Default::default()
            };
            assert!(
                create_backend(&config).is_err(),
                "unsupported scheme '{dest}' must be rejected, not written to local disk"
            );
        }
    }
}
