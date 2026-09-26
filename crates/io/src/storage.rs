// Project:   dfe-archiver
// File:      crates/io/src/storage.rs
// Purpose:   Storage backend implementations (file, S3, MinIO, GCS, Azure)
// Language:  Rust
//
// License:      BUSL-1.1
// Copyright:    (c) 2026 HyperI Pty Ltd

use async_trait::async_trait;
use dfe_archiver_core::config::{ArchiveConfig, AzureConfig, GcsConfig, MinioConfig, S3Config};
use dfe_archiver_core::storage::{Closed, PendingUpload, RecoveredFile, StorageBackend, confine};
use dfe_archiver_core::{Error, Result};
use futures::{StreamExt, TryStreamExt};
use object_store::aws::AmazonS3Builder;
use object_store::azure::MicrosoftAzureBuilder;
use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::path::Path as ObjectPath;
use object_store::{MultipartUpload, ObjectStore, ObjectStoreExt, PutPayload};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use tokio::fs::OpenOptions;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::Mutex;
use tracing::{debug, info, trace, warn};

/// Default multipart chunk size: 8MB
const DEFAULT_MULTIPART_CHUNK_SIZE: usize = 8 * 1024 * 1024;

/// Parts of one upload in flight at once, which bounds its memory to this
/// many chunks.
const UPLOAD_PART_CONCURRENCY: usize = 4;

/// The longest object key S3, GCS and Azure accept, in bytes.
const MAX_OBJECT_KEY_BYTES: usize = 1024;

/// The directory under staging where files that are not uploaded are moved.
const QUARANTINE_DIR: &str = "quarantine";

/// Bytes the quarantine directory may hold. With the 8 GiB staging cap it
/// stays under the spool volume's 10 GiB limit, past which kubelet evicts the
/// pod and the volume with it.
const QUARANTINE_BYTES_CAP: u64 = 1024 * 1024 * 1024;

/// The extension of a staged file.
const STAGED_EXTENSION: &str = "part";

/// Added to a staged file's name for its manifest, written once it is complete.
const MANIFEST_SUFFIX: &str = ".json";

/// Added to a manifest's name while it is written, so a manifest is whole or
/// absent.
const MANIFEST_TEMP_SUFFIX: &str = ".tmp";

/// What a complete staged file needs to be uploaded by another process.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct StagedManifest {
    /// The file's path under the destination.
    path: String,
    /// The length of each block appended, in order.
    blocks: Vec<u64>,
}

/// `local` with `suffix` added to its file name.
fn with_suffix(local: &Path, suffix: &str) -> PathBuf {
    let mut name = local.as_os_str().to_os_string();
    name.push(suffix);
    PathBuf::from(name)
}

/// Write `manifest` beside the staged file `local`, whole or not at all.
async fn write_manifest(local: &Path, manifest: &StagedManifest) -> std::io::Result<()> {
    let body = serde_json::to_vec(manifest).map_err(std::io::Error::other)?;
    let path = with_suffix(local, MANIFEST_SUFFIX);
    let temp = with_suffix(&path, MANIFEST_TEMP_SUFFIX);
    tokio::fs::write(&temp, body).await?;
    tokio::fs::rename(&temp, &path).await
}

/// The manifest of the staged file `local`: `None` when it has none, which is
/// a file its process never completed.
fn read_manifest(local: &Path) -> Option<std::io::Result<StagedManifest>> {
    match std::fs::read(with_suffix(local, MANIFEST_SUFFIX)) {
        Ok(body) => Some(serde_json::from_slice(&body).map_err(std::io::Error::other)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
        Err(e) => Some(Err(e)),
    }
}

/// What opening staging found that a previous process left.
#[derive(Debug, Default)]
pub struct Recovery {
    /// Complete files to upload.
    pub complete: Vec<RecoveredFile>,
    /// Files removed because the source delivers their records again.
    pub replayed: u64,
    /// Files never completed, moved to quarantine.
    pub incomplete: u64,
    /// Files whose manifest could not be read, moved to quarantine.
    pub corrupt: u64,
    /// Files removed because the quarantine directory was at its cap.
    pub quarantine_full: u64,
}

/// Where [`Staging::quarantine`] put a file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Quarantined {
    /// Moved into the quarantine directory.
    Moved,
    /// Removed, because the quarantine directory was at its cap.
    Full,
    /// There was no file to move.
    Gone,
}

/// Local disk where object-store files are written before they upload.
///
/// A file stays here until the store confirms it, so an outage of any length
/// costs disk rather than records. A completed file carries a manifest, so a
/// restart can upload what a stopped process left.
#[derive(Clone, Debug)]
pub struct Staging {
    dir: PathBuf,
    bytes: Arc<AtomicU64>,
}

impl Staging {
    /// Stage under `dir`. What a previous process left there stays until
    /// [`recover`](Self::recover) decides its fate.
    ///
    /// # Errors
    /// Returns an error when the directory cannot be created.
    pub fn open(dir: impl Into<PathBuf>) -> Result<Self> {
        let dir = dir.into();
        std::fs::create_dir_all(&dir)?;
        Ok(Self {
            dir,
            bytes: Arc::new(AtomicU64::new(0)),
        })
    }

    /// Settle what a previous process left in staging, before anything new is
    /// staged.
    ///
    /// With `replayed`, the source delivers those files' records again because
    /// their offsets were never committed, so the files are removed. Otherwise
    /// nothing delivers them again: a complete file is returned to upload, and
    /// one never completed, or whose manifest cannot be read, is quarantined.
    ///
    /// # Errors
    /// Returns an error when the directory cannot be read.
    pub fn recover(&self, replayed: bool) -> Result<Recovery> {
        let mut recovery = Recovery::default();
        let mut staged = Vec::new();
        for entry in std::fs::read_dir(&self.dir)? {
            let path = entry?.path();
            if !path.is_file() {
                continue;
            }
            if path.extension().is_some_and(|ext| ext == STAGED_EXTENSION) {
                staged.push(path);
            } else if path.to_string_lossy().ends_with(MANIFEST_TEMP_SUFFIX) {
                // A manifest never finished: its file is incomplete, and handled as one.
                remove_quietly(&path);
            }
        }
        for local in staged {
            let manifest = read_manifest(&local);
            if replayed {
                remove_quietly(&local);
                remove_quietly(&with_suffix(&local, MANIFEST_SUFFIX));
                recovery.replayed += 1;
                continue;
            }
            let size = std::fs::metadata(&local).map_or(0, |meta| meta.len());
            match manifest {
                Some(Ok(manifest)) if manifest.blocks.iter().sum::<u64>() == size => {
                    self.bytes.fetch_add(size, Ordering::Relaxed);
                    recovery.complete.push(RecoveredFile {
                        local,
                        path: manifest.path,
                        size,
                        blocks: manifest.blocks,
                    });
                }
                Some(_) => {
                    recovery.corrupt += 1;
                    if self.quarantine(&local) == Quarantined::Full {
                        recovery.quarantine_full += 1;
                    }
                }
                None => {
                    recovery.incomplete += 1;
                    if self.quarantine(&local) == Quarantined::Full {
                        recovery.quarantine_full += 1;
                    }
                }
            }
        }
        Ok(recovery)
    }

    /// Move a staged file and its manifest into the quarantine directory for
    /// an operator, or remove them when it is at its 1 GiB cap.
    #[must_use]
    pub fn quarantine(&self, local: &Path) -> Quarantined {
        let manifest = with_suffix(local, MANIFEST_SUFFIX);
        let Ok(size) = std::fs::metadata(local).map(|meta| meta.len()) else {
            remove_quietly(&manifest);
            return Quarantined::Gone;
        };
        let dir = self.dir.join(QUARANTINE_DIR);
        let held = dir_bytes(&dir);
        let moved = held.saturating_add(size) <= QUARANTINE_BYTES_CAP
            && std::fs::create_dir_all(&dir).is_ok()
            && local
                .file_name()
                .is_some_and(|name| std::fs::rename(local, dir.join(name)).is_ok());
        if !moved {
            remove_quietly(local);
            remove_quietly(&manifest);
            warn!(
                local = %local.display(),
                quarantine_bytes = held,
                cap = QUARANTINE_BYTES_CAP,
                "The quarantine directory is full or cannot take a staged file, so it is removed"
            );
            return Quarantined::Full;
        }
        if let Some(name) = manifest.file_name()
            && manifest.exists()
            && let Err(e) = std::fs::rename(&manifest, dir.join(name))
        {
            warn!(manifest = %manifest.display(), error = %e, "Could not quarantine a staged file's manifest");
        }
        Quarantined::Moved
    }

    /// Bytes staged and not yet uploaded or discarded.
    #[must_use]
    pub fn bytes(&self) -> Arc<AtomicU64> {
        Arc::clone(&self.bytes)
    }

    /// Where files are staged.
    #[must_use]
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// A local file name no other staged file shares, this process's or a
    /// previous one's whose files are still here.
    fn next_file(&self) -> PathBuf {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        static PROCESS: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
        // RandomState is seeded from the OS, so a restarted PID 1 still names its files apart.
        let process = PROCESS.get_or_init(|| {
            std::hash::BuildHasher::hash_one(&std::hash::RandomState::new(), std::process::id())
        });
        let n = NEXT.fetch_add(1, Ordering::Relaxed);
        self.dir
            .join(format!("{process:016x}-{n}.{STAGED_EXTENSION}"))
    }
}

/// Remove `path`, which may already be gone.
fn remove_quietly(path: &Path) {
    if let Err(e) = std::fs::remove_file(path)
        && e.kind() != std::io::ErrorKind::NotFound
    {
        warn!(path = %path.display(), error = %e, "Could not remove a staged file");
    }
}

/// The bytes of the files directly in `dir`, or 0 when it does not exist.
fn dir_bytes(dir: &Path) -> u64 {
    std::fs::read_dir(dir).map_or(0, |entries| {
        entries
            .filter_map(|entry| entry.ok()?.metadata().ok())
            .filter(std::fs::Metadata::is_file)
            .map(|meta| meta.len())
            .sum()
    })
}

/// Classify an object store error: refused for good when the same object can
/// never be accepted, a storage error to retry otherwise.
fn store_error(backend: &str, what: &str, path: &str, e: object_store::Error) -> Error {
    let message = format!("{backend}: {what} failed for {path}");
    if refused_for_good(&e) {
        Error::refused_with(message, e)
    } else {
        Error::storage_with(message, e)
    }
}

/// Whether the store rejects this object whatever is retried.
///
/// Deliberately narrow: an unrecognised refusal is retried, which holds the
/// commit rather than dropping records. `object_store` does not export its
/// request error, so the store's own error codes are recognised by their text.
fn refused_for_good(e: &object_store::Error) -> bool {
    const PERMANENT_CODES: [&str; 3] = ["KeyTooLongError", "EntityTooLarge", "InvalidObjectName"];
    if matches!(e, object_store::Error::InvalidPath { .. }) {
        return true;
    }
    let mut cause: Option<&dyn std::error::Error> = Some(e);
    while let Some(err) = cause {
        let text = err.to_string();
        if PERMANENT_CODES.iter().any(|code| text.contains(code)) {
            return true;
        }
        cause = err.source();
    }
    false
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

    /// `path` under the base, refused when it could name anything outside it.
    fn full_path(&self, path: &str) -> Result<PathBuf> {
        confine(path)?;
        Ok(self.base_path.join(path))
    }
}

/// Flush a directory's entries to disk. An empty path is the working
/// directory, which a bare relative destination writes under.
async fn sync_dir(dir: &Path) -> std::io::Result<()> {
    let dir = if dir.as_os_str().is_empty() {
        Path::new(".")
    } else {
        dir
    };
    tokio::fs::File::open(dir).await?.sync_all().await
}

#[async_trait]
impl StorageBackend for FileBackend {
    async fn create(&self, path: &str) -> Result<()> {
        let full_path = self.full_path(path)?;
        let refuse_name = |e: std::io::Error| {
            if e.kind() == std::io::ErrorKind::InvalidFilename {
                // Too long a name is the same refusal on every retry.
                Error::refused_with(format!("{path} cannot be named on this filesystem"), e)
            } else {
                Error::Io(e)
            }
        };

        if let Some(parent) = full_path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(refuse_name)?;
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
                _ => refuse_name(e),
            })?;
        debug!(path = %full_path.display(), "Created file");

        Ok(())
    }

    async fn append(&self, path: &str, data: &[u8]) -> Result<()> {
        let full_path = self.full_path(path)?;

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

    async fn truncate(&self, path: &str, len: u64) -> Result<()> {
        let full_path = self.full_path(path)?;
        let file = OpenOptions::new().write(true).open(&full_path).await?;
        file.set_len(len).await?;
        Ok(())
    }

    /// Syncs the file, then every directory from its parent up to the base
    /// path, because `create` may have made any of them and a directory entry
    /// is durable only once its parent is synced.
    async fn close(&self, path: &str) -> Result<Closed> {
        let full_path = self.full_path(path)?;
        tokio::fs::File::open(&full_path).await?.sync_all().await?;
        for dir in Path::new(path).ancestors().skip(1) {
            sync_dir(&self.base_path.join(dir)).await?;
        }
        debug!(path = %full_path.display(), "File synced to disk");
        Ok(Closed::Durable)
    }

    /// Removes the partial file, whose records are read again or dropped, so
    /// no half-written line is left in the archive.
    async fn abort(&self, path: &str) {
        let Ok(full_path) = self.full_path(path) else {
            return;
        };
        if let Err(e) = tokio::fs::remove_file(&full_path).await
            && e.kind() != std::io::ErrorKind::NotFound
        {
            warn!(path = %full_path.display(), error = %e, "Could not remove an abandoned archive file");
        }
    }

    async fn exists(&self, path: &str) -> Result<bool> {
        let full_path = self.full_path(path)?;
        Ok(full_path.exists())
    }

    async fn delete(&self, path: &str) -> Result<()> {
        let full_path = self.full_path(path)?;
        tokio::fs::remove_file(&full_path).await?;
        Ok(())
    }

    async fn list_prefix(&self, prefix: &str, limit: Option<usize>) -> Result<Vec<String>> {
        let search_dir = self.full_path(prefix)?;
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

/// A staged file still being written.
struct OpenFile {
    local: PathBuf,
    file: tokio::fs::File,
    size: u64,
    /// The length of each block appended, in order.
    blocks: Vec<u64>,
}

/// Cloud object store backend.
///
/// Supports S3, `MinIO`, GCS, and Azure Blob via the `object_store` crate.
/// A file is written to [`Staging`] and uploaded whole once it closes, in
/// `chunk_size` parts, so the store is never on the write path and a failed
/// upload can be tried again from the local copy with fresh credentials.
pub struct ObjectStoreBackend {
    store: Arc<dyn ObjectStore>,
    prefix: String,
    backend_name: &'static str,
    chunk_size: usize,
    staging: Staging,
    /// Files being written, by archive path.
    open: Mutex<HashMap<String, OpenFile>>,
}

impl ObjectStoreBackend {
    /// Stage files for `store` under `staging`.
    #[must_use]
    pub fn with_store(
        store: Arc<dyn ObjectStore>,
        prefix: String,
        backend_name: &'static str,
        chunk_size: usize,
        staging: &Staging,
    ) -> Self {
        Self {
            store,
            prefix,
            backend_name,
            chunk_size,
            staging: staging.clone(),
            open: Mutex::new(HashMap::new()),
        }
    }

    /// Create backend for AWS S3
    pub fn new_s3(
        config: &S3Config,
        prefix: String,
        chunk_size: usize,
        staging: &Staging,
    ) -> Result<Self> {
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

        Ok(Self::with_store(
            Arc::new(store),
            prefix,
            "s3",
            chunk_size,
            staging,
        ))
    }

    /// Create backend for `MinIO` (S3-compatible)
    pub fn new_minio(
        config: &MinioConfig,
        prefix: String,
        chunk_size: usize,
        staging: &Staging,
    ) -> Result<Self> {
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

        Ok(Self::with_store(
            Arc::new(store),
            prefix,
            "minio",
            chunk_size,
            staging,
        ))
    }

    /// Create backend for Google Cloud Storage
    ///
    /// GCS credentials are resolved via (in order):
    /// 1. `service_account_key` (inline JSON key)
    /// 2. `credentials_path` (path to service account JSON file)
    /// 3. `GOOGLE_APPLICATION_CREDENTIALS` env var (Application Default Credentials)
    /// 4. Instance metadata (when running on GCP)
    pub fn new_gcs(
        config: &GcsConfig,
        prefix: String,
        chunk_size: usize,
        staging: &Staging,
    ) -> Result<Self> {
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

        Ok(Self::with_store(
            Arc::new(store),
            prefix,
            "gcs",
            chunk_size,
            staging,
        ))
    }

    /// Create backend for Azure Blob Storage
    pub fn new_azure(
        config: &AzureConfig,
        prefix: String,
        chunk_size: usize,
        staging: &Staging,
    ) -> Result<Self> {
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

        Ok(Self::with_store(
            Arc::new(store),
            prefix,
            "azure",
            chunk_size,
            staging,
        ))
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

/// Whether a failure reading a staged copy is one no retry changes: the copy
/// is gone, not ours to read, or shorter than what was written.
fn unreadable_for_good(e: &std::io::Error) -> bool {
    matches!(
        e.kind(),
        std::io::ErrorKind::NotFound
            | std::io::ErrorKind::PermissionDenied
            | std::io::ErrorKind::UnexpectedEof
            | std::io::ErrorKind::InvalidData
            | std::io::ErrorKind::IsADirectory
    )
}

/// Read from `file` until `buf` is full or the file ends, returning the bytes
/// read, so every part but the last is the full chunk size a store requires.
async fn fill(file: &mut tokio::fs::File, buf: &mut [u8]) -> std::io::Result<usize> {
    let mut filled = 0;
    while filled < buf.len() {
        match file.read(&mut buf[filled..]).await? {
            0 => break,
            read => filled += read,
        }
    }
    Ok(filled)
}

/// A file staged for an object store, uploaded whole on each attempt.
struct StagedUpload {
    store: Arc<dyn ObjectStore>,
    object_path: ObjectPath,
    path: String,
    local: PathBuf,
    size: u64,
    /// The length of each block appended, in order.
    blocks: Vec<u64>,
    chunk_size: usize,
    backend_name: &'static str,
    staging: Staging,
    /// Set once the local copy is gone, so its bytes are counted off once.
    removed: AtomicBool,
}

impl StagedUpload {
    /// Count the local copy off the staging total, once. `false` when it was
    /// already given up.
    fn release_bytes(&self) -> bool {
        if self.removed.swap(true, Ordering::AcqRel) {
            return false;
        }
        self.staging.bytes.fetch_sub(self.size, Ordering::Relaxed);
        true
    }

    /// Remove the local copy and its manifest, and count its bytes off the
    /// staging total.
    async fn remove_local(&self) {
        if !self.release_bytes() {
            return;
        }
        for path in [
            self.local.clone(),
            with_suffix(&self.local, MANIFEST_SUFFIX),
        ] {
            if let Err(e) = tokio::fs::remove_file(&path).await
                && e.kind() != std::io::ErrorKind::NotFound
            {
                warn!(local = %path.display(), error = %e, "Could not remove a staged file");
            }
        }
    }

    /// A failure reading the local copy: unreadable for good, or a storage
    /// error to retry.
    fn local_error(&self, what: &str, e: std::io::Error) -> Error {
        let message = format!("{what} the staged copy of {} failed", self.path);
        if unreadable_for_good(&e) {
            Error::unreadable_with(message, e)
        } else {
            Error::storage_with(message, e)
        }
    }

    /// Stream the local copy into one multipart upload, aborting the upload on
    /// any failure so no part is left behind in the store.
    async fn upload(&self) -> Result<()> {
        if self.size == 0 {
            self.store
                .put(&self.object_path, PutPayload::default())
                .await
                .map_err(|e| store_error(self.backend_name, "put", &self.path, e))?;
            return Ok(());
        }
        let file = tokio::fs::File::open(&self.local)
            .await
            .map_err(|e| self.local_error("opening", e))?;
        let mut upload = self
            .store
            .put_multipart(&self.object_path)
            .await
            .map_err(|e| store_error(self.backend_name, "multipart init", &self.path, e))?;
        let uploaded =
            match self.stream_parts(file, upload.as_mut()).await {
                Ok(()) => upload.complete().await.map(|_| ()).map_err(|e| {
                    store_error(self.backend_name, "multipart complete", &self.path, e)
                }),
                Err(e) => Err(e),
            };
        if uploaded.is_err()
            && let Err(e) = upload.abort().await
        {
            warn!(
                path = %self.path,
                backend = self.backend_name,
                error = %e,
                "Could not abort a failed multipart upload; its parts stay in the store until a lifecycle rule removes them"
            );
        }
        uploaded
    }

    /// The outcome of one part's upload task.
    fn part_outcome(
        &self,
        joined: std::result::Result<object_store::Result<()>, tokio::task::JoinError>,
    ) -> Result<()> {
        match joined {
            Ok(uploaded) => {
                uploaded.map_err(|e| store_error(self.backend_name, "part upload", &self.path, e))
            }
            Err(e) => Err(Error::storage_with(
                format!(
                    "{}: a part upload of {} did not finish",
                    self.backend_name, self.path
                ),
                e,
            )),
        }
    }

    /// Read the local copy into `upload` one `chunk_size` part at a time, each
    /// uploading in its own task while the next is read, at most
    /// [`UPLOAD_PART_CONCURRENCY`] in flight, which bounds its memory.
    async fn stream_parts(
        &self,
        mut file: tokio::fs::File,
        upload: &mut dyn MultipartUpload,
    ) -> Result<()> {
        // Dropped on an early return, which aborts every part still uploading.
        let mut in_flight = tokio::task::JoinSet::new();
        let mut read_total: u64 = 0;
        loop {
            let mut part = vec![0u8; self.chunk_size];
            let filled = fill(&mut file, &mut part)
                .await
                .map_err(|e| self.local_error("reading", e))?;
            if filled == 0 {
                break;
            }
            read_total += filled as u64;
            part.truncate(filled);
            in_flight.spawn(upload.put_part(PutPayload::from(part)));
            if in_flight.len() >= UPLOAD_PART_CONCURRENCY
                && let Some(joined) = in_flight.join_next().await
            {
                self.part_outcome(joined)?;
            }
            if filled < self.chunk_size {
                break;
            }
        }
        while let Some(joined) = in_flight.join_next().await {
            self.part_outcome(joined)?;
        }
        if read_total != self.size {
            // A short copy would reach the store as if it were whole.
            return Err(self.local_error(
                "reading",
                std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    format!("{read_total} of {} bytes", self.size),
                ),
            ));
        }
        Ok(())
    }
}

#[async_trait]
impl PendingUpload for StagedUpload {
    fn path(&self) -> &str {
        &self.path
    }

    fn size(&self) -> u64 {
        self.size
    }

    async fn attempt(&self) -> Result<()> {
        let start = std::time::Instant::now();
        self.upload().await?;
        info!(
            path = %self.path,
            backend = self.backend_name,
            bytes = self.size,
            duration_ms = start.elapsed().as_millis(),
            "Uploaded archive file"
        );
        self.remove_local().await;
        Ok(())
    }

    async fn block(&self, index: usize) -> Result<Option<Vec<u8>>> {
        let Some(&len) = self.blocks.get(index) else {
            return Ok(None);
        };
        let what = format!("reading block {index} of");
        let unreadable = |e: std::io::Error| self.local_error(&what, e);
        let start: u64 = self.blocks.iter().take(index).sum();
        let mut file = tokio::fs::File::open(&self.local)
            .await
            .map_err(unreadable)?;
        file.seek(std::io::SeekFrom::Start(start))
            .await
            .map_err(unreadable)?;
        let mut block = vec![0u8; usize::try_from(len).unwrap_or(usize::MAX)];
        file.read_exact(&mut block).await.map_err(unreadable)?;
        Ok(Some(block))
    }

    async fn discard(&self) {
        self.remove_local().await;
    }

    async fn quarantine(&self) -> bool {
        if !self.release_bytes() {
            return false;
        }
        let staging = self.staging.clone();
        let local = self.local.clone();
        // Blocking filesystem calls, so off the runtime's worker threads.
        tokio::task::spawn_blocking(move || staging.quarantine(&local))
            .await
            .is_ok_and(|placed| placed == Quarantined::Moved)
    }
}

#[async_trait]
impl StorageBackend for ObjectStoreBackend {
    /// Stages the file locally. The key is refused here when the store would
    /// refuse it, before any record is written into the file.
    async fn create(&self, path: &str) -> Result<()> {
        confine(path)?;
        let object_path = self.object_path(path);
        if object_path.as_ref().len() > MAX_OBJECT_KEY_BYTES {
            return Err(Error::refused(format!(
                "{}: the object key for {path} is {} bytes, past the {MAX_OBJECT_KEY_BYTES}-byte limit",
                self.backend_name,
                object_path.as_ref().len()
            )));
        }

        let local = self.staging.next_file();
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&local)
            .await
            .map_err(|e| {
                Error::storage_with(format!("could not stage {path} at {}", local.display()), e)
            })?;
        self.open.lock().await.insert(
            path.to_string(),
            OpenFile {
                local,
                file,
                size: 0,
                blocks: Vec::new(),
            },
        );

        debug!(path = %path, backend = self.backend_name, "Staged a new archive file");
        Ok(())
    }

    async fn append(&self, path: &str, data: &[u8]) -> Result<()> {
        let mut open = self.open.lock().await;
        let staged = open.get_mut(path).ok_or_else(|| {
            Error::storage(format!("{}: no staged file for {path}", self.backend_name))
        })?;
        staged.file.write_all(data).await.map_err(|e| {
            Error::storage_with(
                format!("staging {path} at {} failed", staged.local.display()),
                e,
            )
        })?;
        staged.size += data.len() as u64;
        staged.blocks.push(data.len() as u64);
        self.staging
            .bytes
            .fetch_add(data.len() as u64, Ordering::Relaxed);
        trace!(path = %path, bytes = data.len(), "Staged bytes");
        Ok(())
    }

    /// Cuts the staged copy back to the blocks appended before the one that
    /// failed, which are all `size` and `blocks` count.
    async fn truncate(&self, path: &str, len: u64) -> Result<()> {
        let mut open = self.open.lock().await;
        let staged = open.get_mut(path).ok_or_else(|| {
            Error::storage(format!("{}: no staged file for {path}", self.backend_name))
        })?;
        if len != staged.size {
            return Err(Error::storage(format!(
                "{}: {path} holds {} whole bytes, not {len}",
                self.backend_name, staged.size
            )));
        }
        let local = staged.local.display().to_string();
        let cut = |e: std::io::Error| {
            Error::storage_with(format!("cutting {path} back at {local} failed"), e)
        };
        staged.file.set_len(len).await.map_err(cut)?;
        staged
            .file
            .seek(std::io::SeekFrom::Start(len))
            .await
            .map_err(cut)?;
        Ok(())
    }

    /// Hands back the staged file to upload. Nothing reaches the store here.
    ///
    /// The file gets a manifest naming where it belongs, so a process that
    /// stops before the upload lands leaves it for the next one to upload.
    async fn close(&self, path: &str) -> Result<Closed> {
        let staged = self.open.lock().await.remove(path).ok_or_else(|| {
            Error::storage(format!("{}: no staged file for {path}", self.backend_name))
        })?;
        let OpenFile {
            local,
            mut file,
            size,
            blocks,
        } = staged;
        file.flush().await.map_err(|e| {
            Error::storage_with(format!("staging {path} at {} failed", local.display()), e)
        })?;
        drop(file);
        let manifest = StagedManifest {
            path: path.to_string(),
            blocks,
        };
        // Only a restart reads the manifest, so a file without one still uploads.
        if let Err(e) = write_manifest(&local, &manifest).await {
            warn!(
                local = %local.display(),
                error = %e,
                "Could not write a staged file's manifest; a restart before it uploads quarantines it"
            );
        }

        Ok(Closed::Pending(Box::new(StagedUpload {
            store: Arc::clone(&self.store),
            object_path: self.object_path(path),
            path: path.to_string(),
            local,
            size,
            blocks: manifest.blocks,
            chunk_size: self.chunk_size,
            backend_name: self.backend_name,
            staging: self.staging.clone(),
            removed: AtomicBool::new(false),
        })))
    }

    fn adopt(
        &self,
        file: RecoveredFile,
    ) -> std::result::Result<Box<dyn PendingUpload>, RecoveredFile> {
        if confine(&file.path).is_err() {
            return Err(file);
        }
        Ok(Box::new(StagedUpload {
            store: Arc::clone(&self.store),
            object_path: self.object_path(&file.path),
            path: file.path,
            local: file.local,
            size: file.size,
            blocks: file.blocks,
            chunk_size: self.chunk_size,
            backend_name: self.backend_name,
            staging: self.staging.clone(),
            removed: AtomicBool::new(false),
        }))
    }

    async fn abort(&self, path: &str) {
        let Some(staged) = self.open.lock().await.remove(path) else {
            return;
        };
        self.staging.bytes.fetch_sub(staged.size, Ordering::Relaxed);
        drop(staged.file);
        if let Err(e) = tokio::fs::remove_file(&staged.local).await
            && e.kind() != std::io::ErrorKind::NotFound
        {
            warn!(local = %staged.local.display(), error = %e, "Could not remove an abandoned staged file");
        }
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

/// Create storage backend from destination URL. Object-store files are
/// staged under `staging` until they upload.
pub fn create_backend(
    config: &ArchiveConfig,
    staging: &Staging,
) -> Result<Box<dyn StorageBackend + Send + Sync>> {
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
            staging,
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
            staging,
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
            staging,
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
            staging,
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
#[allow(clippy::expect_used, clippy::panic)]
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

    /// A completed file releases its records' offsets, so a close that cannot
    /// sync the file to disk fails instead of reporting it complete.
    #[tokio::test]
    async fn test_file_backend_close_fails_when_the_file_cannot_be_synced() {
        let temp_dir = TempDir::new().expect("create temp dir");
        let backend = FileBackend::new(temp_dir.path());

        backend.create("hour/lost.jsonl").await.expect("create");
        std::fs::remove_file(temp_dir.path().join("hour/lost.jsonl")).expect("remove");

        backend
            .close("hour/lost.jsonl")
            .await
            .expect_err("a file that is gone cannot be synced");
    }

    /// A bare relative destination writes under the working directory, which
    /// the directory sync has to reach rather than open an empty path.
    #[tokio::test]
    async fn test_file_backend_close_syncs_under_an_empty_base_path() {
        let temp_dir = TempDir::new_in(".").expect("create temp dir under the working directory");
        let dir_name = temp_dir
            .path()
            .file_name()
            .and_then(|name| name.to_str())
            .expect("utf-8 temp dir name");
        let backend = FileBackend::new("");
        let path = format!("{dir_name}/file.jsonl");

        backend.create(&path).await.expect("create");
        backend
            .close(&path)
            .await
            .expect("close syncs up to the working directory");
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

        // Unbounded -- gets all entries.
        let all = backend.list_prefix("data/", None).await.expect("list all");
        assert_eq!(all.len(), 10, "expected all 10 entries with limit=None");

        // Capped -- stops walking as soon as cap is met.
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

    /// A staging area in its own temporary directory.
    fn staging() -> (TempDir, Staging) {
        let dir = TempDir::new().expect("create temp dir");
        let staging = Staging::open(dir.path().join("uploads")).expect("staging");
        (dir, staging)
    }

    fn in_memory(staging: &Staging) -> (Arc<object_store::memory::InMemory>, ObjectStoreBackend) {
        let store = Arc::new(object_store::memory::InMemory::new());
        let backend = ObjectStoreBackend::with_store(
            Arc::clone(&store) as Arc<dyn ObjectStore>,
            "archive".to_string(),
            "memory",
            5 * 1024 * 1024,
            staging,
        );
        (store, backend)
    }

    /// Nothing reaches the store until the closed file uploads, and the upload
    /// carries every staged byte and leaves no local copy behind.
    #[tokio::test]
    async fn a_staged_file_reaches_the_store_only_when_it_uploads() {
        let (_dir, staging) = staging();
        let (store, backend) = in_memory(&staging);

        backend.create("events/a.jsonl").await.expect("create");
        backend
            .append("events/a.jsonl", b"one\n")
            .await
            .expect("append");
        backend
            .append("events/a.jsonl", b"two\n")
            .await
            .expect("append");
        assert_eq!(staging.bytes().load(Ordering::Relaxed), 8);

        let closed = backend.close("events/a.jsonl").await.expect("close");
        assert!(
            matches!(closed, Closed::Pending(_)),
            "an object-store file has to upload"
        );
        let Closed::Pending(upload) = closed else {
            return;
        };
        assert!(
            store
                .head(&ObjectPath::from("archive/events/a.jsonl"))
                .await
                .is_err(),
            "closing stages, it does not upload"
        );

        upload.attempt().await.expect("upload");
        let object = store
            .get(&ObjectPath::from("archive/events/a.jsonl"))
            .await
            .expect("uploaded")
            .bytes()
            .await
            .expect("bytes");
        assert_eq!(object.as_ref(), b"one\ntwo\n");
        assert_eq!(staging.bytes().load(Ordering::Relaxed), 0);
        assert_eq!(
            std::fs::read_dir(staging.dir()).expect("dir").count(),
            0,
            "the local copy is removed once the store holds the file"
        );
    }

    /// Each appended block reads back whole and in order, so a compressed
    /// flush decompresses on its own, and nothing is returned past the last.
    #[tokio::test]
    async fn a_staged_file_reads_back_block_by_block() {
        let (_dir, staging) = staging();
        let (_store, backend) = in_memory(&staging);

        backend.create("events/a.jsonl").await.expect("create");
        for block in [&b"one\n"[..], b"", b"two\nthree\n"] {
            backend
                .append("events/a.jsonl", block)
                .await
                .expect("append");
        }
        let Closed::Pending(upload) = backend.close("events/a.jsonl").await.expect("close") else {
            unreachable!("an object-store file is staged");
        };

        assert_eq!(
            upload.block(0).await.expect("read"),
            Some(b"one\n".to_vec())
        );
        assert_eq!(upload.block(1).await.expect("read"), Some(Vec::new()));
        assert_eq!(
            upload.block(2).await.expect("read"),
            Some(b"two\nthree\n".to_vec())
        );
        assert_eq!(upload.block(3).await.expect("read"), None);
    }

    /// `object` decoded by the codec's own crate, every frame or member of it.
    fn decode_with_the_codecs_reader(codec: &str, object: &[u8]) -> Vec<u8> {
        use std::io::Read;

        let mut out = Vec::new();
        match codec {
            "zstd" => {
                zstd::stream::read::Decoder::new(object)
                    .expect("zstd decoder")
                    .read_to_end(&mut out)
                    .expect("zstd read");
            }
            "gzip" => {
                flate2::read::MultiGzDecoder::new(object)
                    .read_to_end(&mut out)
                    .expect("gzip read");
            }
            "lz4" => {
                // The frame decoder ends its stream at each frame's end.
                let mut rest = object;
                while !rest.is_empty() {
                    lz4_flex::frame::FrameDecoder::new(&mut rest)
                        .read_to_end(&mut out)
                        .expect("lz4 frame read");
                }
            }
            "snappy" => {
                snap::read::FrameDecoder::new(object)
                    .read_to_end(&mut out)
                    .expect("snappy frame read");
            }
            other => unreachable!("no reader for {other}"),
        }
        out
    }

    /// `object` decoded by the codec's command-line tool, or `None` when the
    /// host has none.
    fn decode_with_the_command_line_tool(codec: &str, object: &[u8]) -> Option<Vec<u8>> {
        use std::io::Write;
        use std::process::{Command, Stdio};

        let program = match codec {
            "zstd" | "gzip" | "lz4" => codec,
            _ => return None,
        };
        let mut child = Command::new(program)
            .args(["-d", "-c"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .ok()?;
        child
            .stdin
            .take()?
            .write_all(object)
            .expect("feed the tool");
        let output = child.wait_with_output().expect("run the tool");
        assert!(output.status.success(), "{program} -d refused the object");
        Some(output.stdout)
    }

    /// An archive object written as the pipeline writes one, three flushes
    /// staged and uploaded, reads back whole through a decoder that is not
    /// the archiver's: the codec's own reader, and its command-line tool where
    /// the host has one. Each staged block also decompresses on its own.
    #[tokio::test]
    async fn every_codec_uploads_an_object_standard_decoders_read() {
        use dfe_archiver_core::archive::{ArchiveWriter, RollingPolicy};
        use dfe_archiver_core::compression::create_compressor;
        use dfe_archiver_core::config::ArchiveConfig;

        let flushes: [&[u8]; 3] = [
            b"{\"id\":0}\n{\"id\":1}\n",
            b"{\"id\":2}\n",
            b"{\"id\":3}\n{\"id\":4}\n",
        ];
        let records = flushes.concat();
        for codec in ["zstd", "gzip", "lz4", "snappy"] {
            let (_dir, staging) = staging();
            let (store, backend) = in_memory(&staging);
            let compressor = create_compressor(codec, 3).expect("codec");
            let mut writer = ArchiveWriter::new(
                ArchiveConfig::default(),
                RollingPolicy::default(),
                compressor,
                Box::new(backend),
            );
            for flush in flushes {
                writer.write(flush).await.expect("write");
                writer.flush().await.expect("flush");
            }
            writer.close().await.expect("close");
            let file = writer.take_settled().uploads.pop().expect("a staged file");

            let own = create_compressor(codec, 3).expect("codec");
            let mut blocks = Vec::new();
            while let Some(block) = file.upload.block(blocks.len()).await.expect("block") {
                blocks.push(block);
            }
            assert_eq!(blocks.len(), 3, "{codec}: one block a flush");
            let per_block: Vec<u8> = blocks
                .iter()
                .flat_map(|block| own.decompress(block).expect("block decompress"))
                .collect();
            assert_eq!(
                per_block, records,
                "{codec}: each block decompresses on its own"
            );

            file.upload.attempt().await.expect("upload");
            let object = store
                .get(&ObjectPath::from(format!("archive/{}", file.upload.path())))
                .await
                .expect("uploaded")
                .bytes()
                .await
                .expect("bytes");
            assert_eq!(
                decode_with_the_codecs_reader(codec, &object),
                records,
                "{codec}: the codec's own reader reads every flush"
            );
            if let Some(decoded) = decode_with_the_command_line_tool(codec, &object) {
                assert_eq!(
                    decoded, records,
                    "{codec}: the command-line tool reads every flush"
                );
            }
        }
    }

    /// A key the store would refuse is refused before any record is staged.
    #[tokio::test]
    async fn a_key_past_the_store_limit_is_refused_at_create() {
        let (_dir, staging) = staging();
        let (_store, backend) = in_memory(&staging);
        let long = format!("events/{}.jsonl", "k".repeat(MAX_OBJECT_KEY_BYTES));

        let err = backend.create(&long).await.expect_err("too long");
        assert!(err.is_refused(), "{err:?}");
        assert_eq!(std::fs::read_dir(staging.dir()).expect("dir").count(), 0);
    }

    /// Paths that step out of the destination, refused by both backends alike,
    /// before anything is created.
    const ESCAPING: [&str; 5] = [
        "../escape.jsonl",
        "events/../../escape.jsonl",
        "/etc/escape.jsonl",
        "./events/a.jsonl",
        "events/./a.jsonl",
    ];

    #[tokio::test]
    async fn the_file_backend_refuses_a_path_outside_its_base() {
        let temp_dir = TempDir::new().expect("create temp dir");
        let base = temp_dir.path().join("archive");
        let backend = FileBackend::new(&base);

        for path in ESCAPING {
            let err = backend.create(path).await.expect_err(path);
            assert!(err.is_refused(), "{path}: {err:?}");
            assert!(backend.append(path, b"x").await.is_err(), "{path}");
            assert!(backend.close(path).await.is_err(), "{path}");
        }
        assert_eq!(
            std::fs::read_dir(temp_dir.path()).expect("dir").count(),
            0,
            "nothing was created anywhere"
        );
    }

    #[tokio::test]
    async fn the_object_store_backend_refuses_the_same_paths() {
        let (_dir, staging) = staging();
        let (_store, backend) = in_memory(&staging);

        for path in ESCAPING {
            let err = backend.create(path).await.expect_err(path);
            assert!(err.is_refused(), "{path}: {err:?}");
        }
        assert_eq!(std::fs::read_dir(staging.dir()).expect("dir").count(), 0);
    }

    /// A name longer than the filesystem allows is refused for good, so the
    /// batch goes to the DLQ rather than retrying the same name for ever.
    #[tokio::test]
    async fn a_name_the_filesystem_cannot_hold_is_refused() {
        let temp_dir = TempDir::new().expect("create temp dir");
        let backend = FileBackend::new(temp_dir.path());
        let long = format!("events/{}.jsonl", "n".repeat(300));

        let err = backend.create(&long).await.expect_err("too long a name");
        assert!(err.is_refused(), "{err:?}");
    }

    /// A routed segment names the local file and the object alike: the object
    /// store keeps every byte `path_segment` writes as it is, so a destination
    /// reads the same under `file://` and in a bucket.
    #[tokio::test]
    async fn a_routed_segment_names_the_file_and_the_object_alike() {
        use dfe_archiver_core::routing::path_segment;

        let values = [
            "..",
            ".",
            "",
            "a/b",
            "a\\b",
            "nul\u{0}",
            "=3D",
            "{x}",
            "a%2Fb",
            "caf\u{e9}",
            "q?#|*",
        ];
        let (_dir, staging) = staging();
        let (store, backend) = in_memory(&staging);
        let local = TempDir::new().expect("create temp dir");
        let file_backend = FileBackend::new(local.path());

        for value in values {
            let segment = path_segment(value);
            assert_eq!(
                ObjectPath::from(segment.as_ref()).as_ref(),
                segment.as_ref(),
                "{value:?}: the object store re-encodes {segment:?}"
            );
            let path = format!("events/{segment}/a.jsonl");
            backend.create(&path).await.expect("stage");
            backend.append(&path, b"x\n").await.expect("append");
            let Closed::Pending(upload) = backend.close(&path).await.expect("close") else {
                unreachable!("an object-store file is staged");
            };
            upload.attempt().await.expect("upload");
            store
                .head(&ObjectPath::from(format!("archive/{path}").as_str()))
                .await
                .unwrap_or_else(|e| panic!("{value:?}: no object at archive/{path}: {e}"));

            file_backend.create(&path).await.expect("create");
            file_backend.close(&path).await.expect("close");
            assert!(
                local.path().join(&path).is_file(),
                "{value:?}: no file at {path}"
            );
        }
    }

    /// The only file in the staging directory.
    fn only_staged_file(staging: &Staging) -> PathBuf {
        let mut files: Vec<PathBuf> = std::fs::read_dir(staging.dir())
            .expect("dir")
            .map(|entry| entry.expect("entry").path())
            .filter(|path| path.is_file())
            .collect();
        assert_eq!(files.len(), 1, "{files:?}");
        files.remove(0)
    }

    /// A staged copy torn by a failed append is cut back to its whole blocks,
    /// and the next append lands straight after them.
    #[tokio::test]
    async fn a_torn_staged_copy_is_cut_back_to_its_whole_blocks() {
        let (_dir, staging) = staging();
        let (store, backend) = in_memory(&staging);

        backend.create("events/a.jsonl").await.expect("create");
        backend
            .append("events/a.jsonl", b"one\n")
            .await
            .expect("append");
        // The half of a block a full disk leaves behind.
        std::fs::OpenOptions::new()
            .append(true)
            .open(only_staged_file(&staging))
            .and_then(|mut file| std::io::Write::write_all(&mut file, b"tw"))
            .expect("tear the copy");

        backend
            .truncate("events/a.jsonl", 3)
            .await
            .expect_err("only the whole blocks' length is accepted");
        backend
            .truncate("events/a.jsonl", 4)
            .await
            .expect("cut back");
        backend
            .append("events/a.jsonl", b"two\n")
            .await
            .expect("append after the cut");
        let Closed::Pending(upload) = backend.close("events/a.jsonl").await.expect("close") else {
            unreachable!("an object-store file is staged");
        };
        upload.attempt().await.expect("upload");
        let object = store
            .get(&ObjectPath::from("archive/events/a.jsonl"))
            .await
            .expect("uploaded")
            .bytes()
            .await
            .expect("bytes");
        assert_eq!(object.as_ref(), b"one\ntwo\n");
    }

    /// A local file torn by a failed append is cut back the same way.
    #[tokio::test]
    async fn a_torn_local_file_is_cut_back_to_its_whole_blocks() {
        let temp_dir = TempDir::new().expect("create temp dir");
        let backend = FileBackend::new(temp_dir.path());

        backend.create("events/a.jsonl").await.expect("create");
        backend
            .append("events/a.jsonl", b"one\ntw")
            .await
            .expect("append");
        backend
            .truncate("events/a.jsonl", 4)
            .await
            .expect("cut back");
        backend
            .append("events/a.jsonl", b"two\n")
            .await
            .expect("append");
        let content = std::fs::read(temp_dir.path().join("events/a.jsonl")).expect("read");
        assert_eq!(content, b"one\ntwo\n");
    }

    /// An abandoned file leaves no local copy and no staged bytes behind.
    #[tokio::test]
    async fn an_aborted_file_is_removed_from_staging() {
        let (_dir, staging) = staging();
        let (_store, backend) = in_memory(&staging);

        backend.create("events/b.jsonl").await.expect("create");
        backend
            .append("events/b.jsonl", b"gone\n")
            .await
            .expect("append");
        backend.abort("events/b.jsonl").await;

        assert_eq!(staging.bytes().load(Ordering::Relaxed), 0);
        assert_eq!(std::fs::read_dir(staging.dir()).expect("dir").count(), 0);
    }

    /// Leave in staging, as a process that stopped would, one complete file
    /// holding `complete` and one never completed holding `incomplete`.
    async fn leave_staged(staging: &Staging, complete: &[&[u8]], incomplete: &[u8]) {
        let (_store, backend) = in_memory(staging);
        backend
            .create("events/complete.jsonl")
            .await
            .expect("create");
        for block in complete {
            backend
                .append("events/complete.jsonl", block)
                .await
                .expect("append");
        }
        let Closed::Pending(upload) = backend.close("events/complete.jsonl").await.expect("close")
        else {
            unreachable!("an object-store file is staged");
        };
        // The process stops before the upload runs.
        drop(upload);
        backend.create("events/open.jsonl").await.expect("create");
        backend
            .append("events/open.jsonl", incomplete)
            .await
            .expect("append");
    }

    /// Files a previous process left in staging, when the source delivers
    /// their records again because their offsets were never committed, are
    /// removed rather than uploaded as duplicates.
    #[tokio::test]
    async fn recovery_removes_what_the_source_delivers_again() {
        let dir = TempDir::new().expect("create temp dir");
        let first = Staging::open(dir.path().join("uploads")).expect("staging");
        leave_staged(&first, &[b"one\n"], b"two\n").await;

        let staging = Staging::open(dir.path().join("uploads")).expect("staging");
        let recovery = staging.recover(true).expect("recover");
        assert_eq!(recovery.replayed, 2);
        assert!(recovery.complete.is_empty());
        assert_eq!(recovery.incomplete, 0);
        assert_eq!(std::fs::read_dir(staging.dir()).expect("dir").count(), 0);
        assert_eq!(staging.bytes().load(Ordering::Relaxed), 0);
    }

    /// With no source to deliver them again, a complete file is handed back
    /// and uploads whole, and one never completed is quarantined, not removed.
    #[tokio::test]
    async fn recovery_uploads_a_complete_file_and_quarantines_an_incomplete_one() {
        let dir = TempDir::new().expect("create temp dir");
        let first = Staging::open(dir.path().join("uploads")).expect("staging");
        leave_staged(&first, &[b"one\n", b"two\n"], b"half").await;

        let staging = Staging::open(dir.path().join("uploads")).expect("staging");
        let mut recovery = staging.recover(false).expect("recover");
        assert_eq!(recovery.incomplete, 1, "{recovery:?}");
        assert_eq!(recovery.replayed, 0);
        assert_eq!(recovery.complete.len(), 1, "{recovery:?}");
        let file = recovery.complete.remove(0);
        assert_eq!(file.path, "events/complete.jsonl");
        assert_eq!(file.blocks, vec![4, 4]);
        assert_eq!(staging.bytes().load(Ordering::Relaxed), 8);
        let quarantined: Vec<Vec<u8>> = std::fs::read_dir(staging.dir().join(QUARANTINE_DIR))
            .expect("quarantine dir")
            .map(|entry| std::fs::read(entry.expect("entry").path()).expect("read"))
            .collect();
        assert_eq!(quarantined, vec![b"half".to_vec()]);

        let (store, backend) = in_memory(&staging);
        let upload = backend.adopt(file).expect("adopted");
        assert_eq!(
            upload.block(1).await.expect("block"),
            Some(b"two\n".to_vec())
        );
        upload.attempt().await.expect("upload");
        let object = store
            .get(&ObjectPath::from("archive/events/complete.jsonl"))
            .await
            .expect("uploaded")
            .bytes()
            .await
            .expect("bytes");
        assert_eq!(object.as_ref(), b"one\ntwo\n");
        assert_eq!(staging.bytes().load(Ordering::Relaxed), 0);
        let left: Vec<PathBuf> = std::fs::read_dir(staging.dir())
            .expect("dir")
            .map(|entry| entry.expect("entry").path())
            .filter(|path| path.is_file())
            .collect();
        assert!(
            left.is_empty(),
            "the copy and its manifest are removed: {left:?}"
        );
    }

    /// A manifest that cannot be read, or that disagrees with its file, says
    /// nothing reliable about where the file belongs, so it is quarantined.
    #[tokio::test]
    async fn a_file_with_an_unreadable_manifest_is_quarantined() {
        let dir = TempDir::new().expect("create temp dir");
        let first = Staging::open(dir.path().join("uploads")).expect("staging");
        leave_staged(&first, &[b"one\n"], b"").await;
        let manifest = std::fs::read_dir(first.dir())
            .expect("dir")
            .map(|entry| entry.expect("entry").path())
            .find(|path| path.to_string_lossy().ends_with(MANIFEST_SUFFIX))
            .expect("a manifest");
        std::fs::write(&manifest, b"{not json").expect("corrupt the manifest");

        let staging = Staging::open(dir.path().join("uploads")).expect("staging");
        let recovery = staging.recover(false).expect("recover");
        assert_eq!(recovery.corrupt, 1, "{recovery:?}");
        assert!(recovery.complete.is_empty());
        assert_eq!(
            std::fs::read_dir(staging.dir().join(QUARANTINE_DIR))
                .expect("quarantine dir")
                .count(),
            3,
            "the corrupt file, its manifest and the incomplete file"
        );
    }

    /// Wraps an in-memory store so every multipart completion fails, as a
    /// store answering the completion with an error does, and counts aborts.
    #[derive(Debug)]
    struct FailingCompletion {
        inner: object_store::memory::InMemory,
        aborts: Arc<AtomicU64>,
    }

    impl std::fmt::Display for FailingCompletion {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("FailingCompletion")
        }
    }

    #[derive(Debug)]
    struct FailingUpload {
        inner: Box<dyn MultipartUpload>,
        aborts: Arc<AtomicU64>,
    }

    #[async_trait]
    impl MultipartUpload for FailingUpload {
        fn put_part(&mut self, data: PutPayload) -> object_store::UploadPart {
            self.inner.put_part(data)
        }
        async fn complete(&mut self) -> object_store::Result<object_store::PutResult> {
            Err(object_store::Error::Generic {
                store: "failing",
                source: "completion refused with 503".into(),
            })
        }
        async fn abort(&mut self) -> object_store::Result<()> {
            self.aborts.fetch_add(1, Ordering::Relaxed);
            self.inner.abort().await
        }
    }

    #[async_trait]
    impl ObjectStore for FailingCompletion {
        async fn put_opts(
            &self,
            location: &ObjectPath,
            payload: PutPayload,
            opts: object_store::PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            self.inner.put_opts(location, payload, opts).await
        }
        async fn put_multipart_opts(
            &self,
            location: &ObjectPath,
            opts: object_store::PutMultipartOptions,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            let inner = self.inner.put_multipart_opts(location, opts).await?;
            Ok(Box::new(FailingUpload {
                inner,
                aborts: Arc::clone(&self.aborts),
            }))
        }
        async fn get_opts(
            &self,
            location: &ObjectPath,
            options: object_store::GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            self.inner.get_opts(location, options).await
        }
        fn delete_stream(
            &self,
            locations: futures::stream::BoxStream<'static, object_store::Result<ObjectPath>>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<ObjectPath>> {
            self.inner.delete_stream(locations)
        }
        fn list(
            &self,
            prefix: Option<&ObjectPath>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
        {
            self.inner.list(prefix)
        }
        async fn list_with_delimiter(
            &self,
            prefix: Option<&ObjectPath>,
        ) -> object_store::Result<object_store::ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }
        async fn copy_opts(
            &self,
            from: &ObjectPath,
            to: &ObjectPath,
            options: object_store::CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    /// A multipart upload whose completion fails is aborted, so its parts are
    /// not left in the store, and the file stays staged for the next attempt.
    #[tokio::test]
    async fn a_failed_completion_aborts_the_multipart_upload() {
        let (_dir, staging) = staging();
        let aborts = Arc::new(AtomicU64::new(0));
        let store = Arc::new(FailingCompletion {
            inner: object_store::memory::InMemory::new(),
            aborts: Arc::clone(&aborts),
        });
        let backend = ObjectStoreBackend::with_store(
            store as Arc<dyn ObjectStore>,
            "archive".to_string(),
            "failing",
            5 * 1024 * 1024,
            &staging,
        );
        backend.create("events/a.jsonl").await.expect("create");
        backend
            .append("events/a.jsonl", b"one\n")
            .await
            .expect("append");
        let Closed::Pending(upload) = backend.close("events/a.jsonl").await.expect("close") else {
            unreachable!("an object-store file is staged");
        };

        let err = upload.attempt().await.expect_err("completion fails");
        assert!(!err.is_refused() && !err.is_unreadable(), "{err:?}");
        assert_eq!(aborts.load(Ordering::Relaxed), 1, "the upload is aborted");
        assert_eq!(
            staging.bytes().load(Ordering::Relaxed),
            4,
            "the staged copy is kept for the next attempt"
        );
    }

    /// A staged copy that is gone or shorter than written cannot be uploaded
    /// by any retry, and a short one never reaches the store as if whole.
    #[tokio::test]
    async fn a_missing_or_short_staged_copy_is_unreadable_for_good() {
        let (_dir, staging) = staging();
        let (store, backend) = in_memory(&staging);
        for (name, damage) in [("gone", 0usize), ("short", 3)] {
            let path = format!("events/{name}.jsonl");
            backend.create(&path).await.expect("create");
            backend
                .append(&path, b"0123456789\n")
                .await
                .expect("append");
            let Closed::Pending(upload) = backend.close(&path).await.expect("close") else {
                unreachable!("an object-store file is staged");
            };
            let local = std::fs::read_dir(staging.dir())
                .expect("dir")
                .map(|entry| entry.expect("entry").path())
                .find(|path| path.extension().is_some_and(|ext| ext == STAGED_EXTENSION))
                .expect("the staged copy");
            if damage == 0 {
                std::fs::remove_file(&local).expect("remove the copy");
            } else {
                std::fs::write(&local, &b"0123456789\n"[..damage]).expect("shorten the copy");
            }

            let err = upload.attempt().await.expect_err("unreadable");
            assert!(err.is_unreadable(), "{name}: {err:?}");
            assert!(
                store
                    .head(&ObjectPath::from(format!("archive/{path}").as_str()))
                    .await
                    .is_err(),
                "{name}: nothing reached the store"
            );
            let _ = upload.quarantine().await;
        }
        assert!(!unreadable_for_good(&std::io::Error::from(
            std::io::ErrorKind::Interrupted
        )));
        assert!(!unreadable_for_good(&std::io::Error::from_raw_os_error(5)));
    }

    /// Only a refusal the store gives for the object itself is permanent.
    /// Everything else, including credentials and missing buckets, retries.
    #[test]
    fn only_an_object_level_refusal_is_permanent() {
        #[derive(Debug)]
        struct Response(&'static str);
        impl std::fmt::Display for Response {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(self.0)
            }
        }
        impl std::error::Error for Response {}
        let generic = |body: &'static str| object_store::Error::Generic {
            store: "S3",
            source: Box::new(Response(body)),
        };

        assert!(refused_for_good(&generic(
            "Server returned non-2xx status code: 400 Bad Request: <Code>KeyTooLongError</Code>"
        )));
        assert!(refused_for_good(&object_store::Error::InvalidPath {
            source: object_store::path::Error::EmptySegment {
                path: "a//b".to_string(),
            },
        }));
        assert!(!refused_for_good(&generic(
            "Server returned non-2xx status code: 503 Service Unavailable"
        )));
        assert!(!refused_for_good(&object_store::Error::NotFound {
            path: "bucket".to_string(),
            source: Box::new(Response("NoSuchBucket")),
        }));
        assert!(!refused_for_good(&object_store::Error::PermissionDenied {
            path: "key".to_string(),
            source: Box::new(Response("AccessDenied")),
        }));
    }

    #[test]
    fn test_create_backend_file_url() {
        let (_dir, staging) = staging();
        let config = ArchiveConfig {
            destination: "file:///tmp/test-archive".to_string(),
            ..Default::default()
        };
        let backend = create_backend(&config, &staging).expect("create file backend");
        assert_eq!(backend.name(), "file");
    }

    #[test]
    fn test_create_backend_bare_path_falls_back_to_file() {
        let (_dir, staging) = staging();
        let config = ArchiveConfig {
            destination: "/tmp/bare-path".to_string(),
            ..Default::default()
        };
        let backend = create_backend(&config, &staging).expect("bare path -> file backend");
        assert_eq!(backend.name(), "file");
    }

    #[test]
    fn test_create_backend_minio_requires_config() {
        let (_dir, staging) = staging();
        let config = ArchiveConfig {
            destination: "minio://bucket/prefix".to_string(),
            minio: None,
            ..Default::default()
        };
        let result = create_backend(&config, &staging);
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

        let (_dir, staging) = staging();
        match create_backend(&config, &staging) {
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
        let (_dir, staging) = staging();
        let err = ObjectStoreBackend::new_azure(&config, String::new(), 8 * 1024 * 1024, &staging)
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
        let (_dir, staging) = staging();
        let backend =
            ObjectStoreBackend::new_azure(&config, String::new(), 8 * 1024 * 1024, &staging)
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
        let (_dir, staging) = staging();
        for dest in ["S3://upper-bucket", "blob://bucket/x", "http://host/path"] {
            let config = ArchiveConfig {
                destination: dest.to_string(),
                ..Default::default()
            };
            assert!(
                create_backend(&config, &staging).is_err(),
                "unsupported scheme '{dest}' must be rejected, not written to local disk"
            );
        }
    }
}
