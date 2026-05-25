#![forbid(unsafe_code)]

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, OnceLock};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use http::header::{IF_MATCH, IF_NONE_MATCH};
use http::{HeaderMap, HeaderValue};
use s3::bucket::Bucket;
use s3::creds::Credentials;
use s3::region::Region;
use s3::BucketConfiguration;
use tokio::runtime::Runtime;
use wombatkv_format::{decode_wal_record, encode_wal_record, FormatError, WalRecord};

const DEFAULT_NODE_ID: &str = "node-local";
const WAL_BATCH_MAGIC: &[u8; 12] = b"WMBT_KV_WB01";
const WAL_BATCH_HEADER_SIZE: usize = 36;

/// WAL key layout classifier for migration and audit drills.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WalKeyLayout {
    CanonicalEpochPartitioned,
    LegacyFlat,
}

/// Canonical-vs-legacy key counts for a namespace prefix.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct WalLayoutAuditCounts {
    pub total_keys: usize,
    pub canonical_keys: usize,
    pub legacy_keys: usize,
    pub unknown_keys: usize,
}

/// Flush policy for WAL group commit buffering.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WalBatchPolicy {
    pub flush_interval_ms: u64,
    pub max_records: usize,
    pub max_bytes: usize,
}

impl Default for WalBatchPolicy {
    fn default() -> Self {
        Self { flush_interval_ms: 250, max_records: 32, max_bytes: 256 * 1024 }
    }
}

impl WalBatchPolicy {
    #[must_use]
    pub const fn single_record_fallback() -> Self {
        Self { flush_interval_ms: 0, max_records: 1, max_bytes: 0 }
    }

    #[must_use]
    pub const fn is_single_record_mode(self) -> bool {
        self.max_records <= 1 || self.max_bytes <= 1
    }
}

/// High-level outcome for buffered group-commit publish operations.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GroupCommitPublishResult {
    pub flushed_object_keys: Vec<String>,
    pub buffered_records: usize,
    pub buffered_bytes: usize,
}

impl GroupCommitPublishResult {
    #[must_use]
    fn buffered(buffered_records: usize, buffered_bytes: usize) -> Self {
        Self { flushed_object_keys: Vec::new(), buffered_records, buffered_bytes }
    }

    fn record_flush(&mut self, object_key: String) {
        self.flushed_object_keys.push(object_key);
    }
}

/// Errors from object-store WAL operations.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WalStoreError {
    ObjectNotFound(String),
    CorruptWal(FormatError),
    ObjectStorePoisoned,
    InvalidConfig(String),
    S3(String),
}

impl From<FormatError> for WalStoreError {
    fn from(error: FormatError) -> Self {
        Self::CorruptWal(error)
    }
}

/// Opaque CAS token returned by object stores for conditional writes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CasToken {
    ETag(String),
    ContentHash(u64),
}

/// Result for token-based conditional puts.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ConditionalPutResult {
    Success,
    Conflict { current_token: Option<CasToken> },
    Unsupported,
}

/// Object-store capability flags for conditional writes.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StoreCapabilities {
    pub conditional_create: bool,
    pub conditional_update: bool,
}

/// Shared capability cache keyed by backend+bucket.
#[derive(Default)]
struct CapabilityCache {
    entries: Mutex<BTreeMap<String, StoreCapabilities>>,
}

impl CapabilityCache {
    fn get_or_probe(
        &self,
        cache_key: &str,
        probe: impl FnOnce() -> Result<StoreCapabilities, WalStoreError>,
    ) -> Result<StoreCapabilities, WalStoreError> {
        let maybe_cached = {
            let guard = self.entries.lock().map_err(|_| WalStoreError::ObjectStorePoisoned)?;
            guard.get(cache_key).copied()
        };
        if let Some(cached) = maybe_cached {
            return Ok(cached);
        }

        let probed = probe()?;
        let mut guard = self.entries.lock().map_err(|_| WalStoreError::ObjectStorePoisoned)?;
        let entry = guard.entry(cache_key.to_string()).or_insert(probed);
        Ok(*entry)
    }
}

fn s3_capability_cache() -> &'static CapabilityCache {
    static CACHE: OnceLock<CapabilityCache> = OnceLock::new();
    CACHE.get_or_init(CapabilityCache::default)
}

fn content_hash(payload: &[u8]) -> u64 {
    const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

    let mut hash = FNV_OFFSET;
    for byte in payload {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

/// Minimal object-store boundary used by WAL and segment stores.
pub trait ObjectStore: Clone + Send + Sync + 'static {
    fn put_object(&self, key: &str, payload: &[u8]) -> Result<(), WalStoreError>;
    fn put_object_overwrite(&self, key: &str, payload: &[u8]) -> Result<(), WalStoreError>;
    fn get_object(&self, key: &str) -> Result<Vec<u8>, WalStoreError>;

    /// Batched parallel get. Default is sequential per-key; impls with
    /// a real async runtime (e.g. `S3ObjectStore`) override with N
    /// concurrent in-flight requests sharing the same runtime + HTTP
    /// connection pool. Result vector is ordered to match `keys`.
    ///
    /// This is the an earlier internal prototype pattern (the prior playground §4c): one `block_on`,
    /// N async tasks sharing the runtime's connection pool, vs
    /// N OS threads each calling `block_on` on the shared runtime,
    /// which is what `std::thread::scope` does and what loses on
    /// contention.
    fn get_object_many(&self, keys: &[String]) -> Vec<Result<Vec<u8>, WalStoreError>> {
        keys.iter().map(|k| self.get_object(k)).collect()
    }

    fn get_object_range(
        &self,
        key: &str,
        start: usize,
        end: usize,
    ) -> Result<Vec<u8>, WalStoreError>;
    fn compare_and_swap_object(
        &self,
        key: &str,
        expected: Option<&[u8]>,
        new_payload: &[u8],
    ) -> Result<bool, WalStoreError>;
    fn read_with_token(&self, key: &str) -> Result<Option<(Vec<u8>, CasToken)>, WalStoreError>;
    fn put_if_token_matches(
        &self,
        key: &str,
        payload: &[u8],
        token: &CasToken,
    ) -> Result<ConditionalPutResult, WalStoreError>;
    fn put_if_absent(
        &self,
        key: &str,
        payload: &[u8],
    ) -> Result<ConditionalPutResult, WalStoreError>;
    fn capabilities(&self) -> StoreCapabilities;
    fn head_object(&self, key: &str) -> Result<bool, WalStoreError>;
    fn list_prefix(&self, prefix: &str) -> Result<Vec<String>, WalStoreError>;
    fn delete_object(&self, key: &str) -> Result<bool, WalStoreError>;
}

/// Minimal in-memory object store used by tests and local development slices.
#[derive(Clone, Default)]
pub struct InMemoryObjectStore {
    inner: Arc<Mutex<BTreeMap<String, Vec<u8>>>>,
}

impl InMemoryObjectStore {
    /// Immutable put. Existing keys are preserved.
    pub fn put_object(&self, key: &str, payload: &[u8]) -> Result<(), WalStoreError> {
        ObjectStore::put_object(self, key, payload)
    }

    /// Overwrite-capable put used for mutable pointer keys.
    pub fn put_object_overwrite(&self, key: &str, payload: &[u8]) -> Result<(), WalStoreError> {
        ObjectStore::put_object_overwrite(self, key, payload)
    }

    /// Full object fetch.
    pub fn get_object(&self, key: &str) -> Result<Vec<u8>, WalStoreError> {
        ObjectStore::get_object(self, key)
    }

    /// Byte range fetch (`start` inclusive, `end` exclusive).
    pub fn get_object_range(
        &self,
        key: &str,
        start: usize,
        end: usize,
    ) -> Result<Vec<u8>, WalStoreError> {
        ObjectStore::get_object_range(self, key, start, end)
    }

    /// Compare-and-swap for pointer objects.
    pub fn compare_and_swap_object(
        &self,
        key: &str,
        expected: Option<&[u8]>,
        new_payload: &[u8],
    ) -> Result<bool, WalStoreError> {
        ObjectStore::compare_and_swap_object(self, key, expected, new_payload)
    }

    /// Read payload with current CAS token.
    pub fn read_with_token(&self, key: &str) -> Result<Option<(Vec<u8>, CasToken)>, WalStoreError> {
        ObjectStore::read_with_token(self, key)
    }

    /// Conditional overwrite using a compare token.
    pub fn put_if_token_matches(
        &self,
        key: &str,
        payload: &[u8],
        token: &CasToken,
    ) -> Result<ConditionalPutResult, WalStoreError> {
        ObjectStore::put_if_token_matches(self, key, payload, token)
    }

    /// Conditional create that succeeds only when key does not exist.
    pub fn put_if_absent(
        &self,
        key: &str,
        payload: &[u8],
    ) -> Result<ConditionalPutResult, WalStoreError> {
        ObjectStore::put_if_absent(self, key, payload)
    }

    /// Report conditional-write support for this backend.
    #[must_use]
    pub fn capabilities(&self) -> StoreCapabilities {
        ObjectStore::capabilities(self)
    }

    /// Head semantics: check if object exists.
    pub fn head_object(&self, key: &str) -> Result<bool, WalStoreError> {
        ObjectStore::head_object(self, key)
    }

    /// List keys by prefix.
    pub fn list_prefix(&self, prefix: &str) -> Result<Vec<String>, WalStoreError> {
        ObjectStore::list_prefix(self, prefix)
    }

    /// Delete object key. Returns `true` when key existed.
    pub fn delete_object(&self, key: &str) -> Result<bool, WalStoreError> {
        ObjectStore::delete_object(self, key)
    }

    #[cfg(test)]
    pub fn overwrite_for_test(&self, key: &str, payload: &[u8]) -> Result<(), WalStoreError> {
        let mut guard = self.inner.lock().map_err(|_| WalStoreError::ObjectStorePoisoned)?;
        guard.insert(key.to_string(), payload.to_vec());
        Ok(())
    }
}

impl ObjectStore for InMemoryObjectStore {
    fn put_object(&self, key: &str, payload: &[u8]) -> Result<(), WalStoreError> {
        let mut guard = self.inner.lock().map_err(|_| WalStoreError::ObjectStorePoisoned)?;
        guard.entry(key.to_string()).or_insert_with(|| payload.to_vec());
        Ok(())
    }

    fn put_object_overwrite(&self, key: &str, payload: &[u8]) -> Result<(), WalStoreError> {
        let mut guard = self.inner.lock().map_err(|_| WalStoreError::ObjectStorePoisoned)?;
        guard.insert(key.to_string(), payload.to_vec());
        Ok(())
    }

    fn get_object(&self, key: &str) -> Result<Vec<u8>, WalStoreError> {
        let guard = self.inner.lock().map_err(|_| WalStoreError::ObjectStorePoisoned)?;
        guard.get(key).cloned().ok_or_else(|| WalStoreError::ObjectNotFound(key.to_string()))
    }

    fn get_object_range(
        &self,
        key: &str,
        start: usize,
        end: usize,
    ) -> Result<Vec<u8>, WalStoreError> {
        let guard = self.inner.lock().map_err(|_| WalStoreError::ObjectStorePoisoned)?;
        let bytes = guard.get(key).ok_or_else(|| WalStoreError::ObjectNotFound(key.to_string()))?;
        if start >= bytes.len() || start >= end {
            return Ok(Vec::new());
        }
        let bounded_end = end.min(bytes.len());
        Ok(bytes[start..bounded_end].to_vec())
    }

    fn compare_and_swap_object(
        &self,
        key: &str,
        expected: Option<&[u8]>,
        new_payload: &[u8],
    ) -> Result<bool, WalStoreError> {
        let mut guard = self.inner.lock().map_err(|_| WalStoreError::ObjectStorePoisoned)?;
        let current = guard.get(key).map(Vec::as_slice);
        if current == expected {
            guard.insert(key.to_string(), new_payload.to_vec());
            return Ok(true);
        }
        Ok(false)
    }

    fn read_with_token(&self, key: &str) -> Result<Option<(Vec<u8>, CasToken)>, WalStoreError> {
        let guard = self.inner.lock().map_err(|_| WalStoreError::ObjectStorePoisoned)?;
        let Some(payload) = guard.get(key) else {
            return Ok(None);
        };
        let bytes = payload.clone();
        let token = CasToken::ContentHash(content_hash(&bytes));
        Ok(Some((bytes, token)))
    }

    fn put_if_token_matches(
        &self,
        key: &str,
        payload: &[u8],
        token: &CasToken,
    ) -> Result<ConditionalPutResult, WalStoreError> {
        let mut guard = self.inner.lock().map_err(|_| WalStoreError::ObjectStorePoisoned)?;
        let Some(current) = guard.get(key) else {
            return Ok(ConditionalPutResult::Conflict { current_token: None });
        };

        let current_token = CasToken::ContentHash(content_hash(current));
        if &current_token == token {
            guard.insert(key.to_string(), payload.to_vec());
            return Ok(ConditionalPutResult::Success);
        }

        Ok(ConditionalPutResult::Conflict { current_token: Some(current_token) })
    }

    fn put_if_absent(
        &self,
        key: &str,
        payload: &[u8],
    ) -> Result<ConditionalPutResult, WalStoreError> {
        let mut guard = self.inner.lock().map_err(|_| WalStoreError::ObjectStorePoisoned)?;
        if let Some(current) = guard.get(key) {
            let current_token = CasToken::ContentHash(content_hash(current));
            return Ok(ConditionalPutResult::Conflict { current_token: Some(current_token) });
        }

        guard.insert(key.to_string(), payload.to_vec());
        Ok(ConditionalPutResult::Success)
    }

    fn capabilities(&self) -> StoreCapabilities {
        StoreCapabilities { conditional_create: true, conditional_update: true }
    }

    fn head_object(&self, key: &str) -> Result<bool, WalStoreError> {
        let guard = self.inner.lock().map_err(|_| WalStoreError::ObjectStorePoisoned)?;
        Ok(guard.contains_key(key))
    }

    fn list_prefix(&self, prefix: &str) -> Result<Vec<String>, WalStoreError> {
        let guard = self.inner.lock().map_err(|_| WalStoreError::ObjectStorePoisoned)?;
        Ok(guard.keys().filter(|key| key.starts_with(prefix)).cloned().collect())
    }

    fn delete_object(&self, key: &str) -> Result<bool, WalStoreError> {
        let mut guard = self.inner.lock().map_err(|_| WalStoreError::ObjectStorePoisoned)?;
        Ok(guard.remove(key).is_some())
    }
}

/// S3/MinIO object store configuration.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct S3ObjectStoreConfig {
    pub endpoint: String,
    pub bucket: String,
    pub access_key: String,
    pub secret_key: String,
    pub region: String,
    pub max_retries: u32,
    pub retry_backoff_ms: u64,
}

impl Default for S3ObjectStoreConfig {
    fn default() -> Self {
        Self {
            endpoint: "http://127.0.0.1:9000".to_string(),
            bucket: default_bucket_name(),
            access_key: "minioadmin".to_string(),
            secret_key: "minioadmin".to_string(),
            region: "us-east-1".to_string(),
            max_retries: 2,
            retry_backoff_ms: 50,
        }
    }
}

/// Derive a per-user default bucket name. Falls back to `wombatkv-cache`
/// if `$USER` is unset (e.g. inside a minimal container).
fn default_bucket_name() -> String {
    match std::env::var("USER").ok().filter(|s| !s.is_empty()) {
        Some(user) => format!("wombatkv-cache-{user}"),
        None => "wombatkv-cache".to_string(),
    }
}

impl S3ObjectStoreConfig {
    pub fn from_env() -> Result<Self, WalStoreError> {
        let mut cfg = Self::default();
        // SAFETY DECISION (alpha.11+1): Only accept WMBT_KV_* env vars.
        //
        // Previous versions silently fell back to AWS_ENDPOINT_URL_S3 /
        // AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY / AWS_REGION /
        // AWS_DEFAULT_REGION when our WMBT_KV_S3_* vars were unset. That
        // convenience masked a serious risk: a user running with their
        // production AWS creds in env (common from `~/.aws/credentials`
        // or shell exports) could accidentally have wombatkv write to
        // real S3, racking up bytes/requests on the wrong account, or
        // worse, contaminating an unrelated bucket.
        //
        // Now WombatKV requires its OWN explicit env vars. Refuse to
        // pick up AWS_* fallback. Users wanting AWS-style env layout
        // should explicitly export the WMBT_KV_S3_* names.
        if let Ok(v) = std::env::var("WMBT_KV_S3_ENDPOINT") {
            cfg.endpoint = v;
        }
        match std::env::var("WMBT_KV_BUCKET") {
            Ok(v) if !v.is_empty() => cfg.bucket = v,
            _ => {
                // SAFETY DECISION (alpha.11+1): unset bucket is a hard
                // error, not a warning + derived default. The derived
                // default (`wombatkv-cache-$USER`) was opaque and
                // could collide with a bucket already in use for
                // unrelated data, silently corrupting it. Force
                // explicit declaration.
                return Err(WalStoreError::InvalidConfig(
                    "WMBT_KV_BUCKET is required (no fallback). Set it explicitly to the bucket \
                     name WombatKV should own. We will NOT pick up AWS_* fallbacks or derive a \
                     bucket name from $USER, both risk writing to the wrong place."
                        .to_string(),
                ));
            }
        }
        if let Ok(v) = std::env::var("WMBT_KV_S3_ACCESS_KEY") {
            cfg.access_key = v;
        }
        if let Ok(v) = std::env::var("WMBT_KV_S3_SECRET_KEY") {
            cfg.secret_key = v;
        }
        if let Ok(v) = std::env::var("WMBT_KV_S3_REGION") {
            cfg.region = v;
        }
        if let Ok(v) = std::env::var("WMBT_KV_S3_GET_RETRIES") {
            cfg.max_retries = v
                .parse::<u32>()
                .map_err(|_| WalStoreError::InvalidConfig("WMBT_KV_S3_GET_RETRIES".to_string()))?;
        }
        if let Ok(v) = std::env::var("WMBT_KV_S3_RETRY_BACKOFF_MS") {
            cfg.retry_backoff_ms = v.parse::<u64>().map_err(|_| {
                WalStoreError::InvalidConfig("WMBT_KV_S3_RETRY_BACKOFF_MS".to_string())
            })?;
        }
        Ok(cfg)
    }
}

#[derive(Clone)]
pub struct S3ObjectStore {
    inner: Arc<S3ObjectStoreInner>,
}

struct S3ObjectStoreInner {
    endpoint: String,
    bucket_name: String,
    capability_cache_key: String,
    capabilities: Mutex<StoreCapabilities>,
    bucket: Bucket,
    region: Region,
    credentials: Credentials,
    rt: Runtime,
    max_retries: u32,
    retry_backoff_ms: u64,
}

impl S3ObjectStore {
    pub fn new(config: S3ObjectStoreConfig) -> Result<Self, WalStoreError> {
        let S3ObjectStoreConfig {
            endpoint,
            bucket: bucket_name,
            access_key,
            secret_key,
            region,
            max_retries,
            retry_backoff_ms,
        } = config;

        if endpoint.is_empty()
            || bucket_name.is_empty()
            || access_key.is_empty()
            || secret_key.is_empty()
            || region.is_empty()
        {
            return Err(WalStoreError::InvalidConfig(
                "endpoint, bucket, access_key, secret_key, region must be non-empty".to_string(),
            ));
        }
        if Self::is_default_dev_credentials(&access_key, &secret_key)
            && !Self::local_dev_mode_enabled(&endpoint)
        {
            return Err(WalStoreError::InvalidConfig(
                "refusing default MinIO credentials outside local-dev mode; set WMBT_KV_LOCAL_DEV=1 only for local development"
                    .to_string(),
            ));
        }

        let credentials = Credentials::new(
            Some(access_key.as_str()),
            Some(secret_key.as_str()),
            None,
            None,
            None,
        )
        .map_err(|err| WalStoreError::S3(format!("credentials init failed: {err}")))?;

        let region_name = region.clone();
        let region = Region::Custom { region, endpoint: endpoint.clone() };

        let mut bucket = *Bucket::new(&bucket_name, region.clone(), credentials.clone())
            .map_err(|err| WalStoreError::S3(format!("bucket init failed: {err}")))?;
        bucket.set_path_style();

        // Worker thread count: parallel chunked S3 GET fan-out via JoinSet
        // signs each request with SigV4 (~hundreds of µs each) and dispatches
        // its hyper future on this runtime. With only 2 workers, a 23-way
        // chunked fanout serialized at the signing stage and the first cold
        // request paid ~70 ms vs ~10 ms on a warm pool. 8 workers gives the
        // SigV4 + hyper-dispatch work enough parallelism to align with the
        // boto3 baseline (8,622 MB/s aggregate at concurrency 23 on local
        // MinIO). Override via `WMBT_KV_S3_RUNTIME_THREADS=<N>` if needed.
        let runtime_threads: usize = std::env::var("WMBT_KV_S3_RUNTIME_THREADS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(8);
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(runtime_threads.max(1))
            .enable_all()
            .build()
            .map_err(|err| WalStoreError::S3(format!("tokio runtime init failed: {err}")))?;
        let capability_cache_key = format!("s3|{endpoint}|{region_name}|{bucket_name}");

        let store = Self {
            inner: Arc::new(S3ObjectStoreInner {
                endpoint: endpoint.clone(),
                bucket_name: bucket_name.clone(),
                capability_cache_key,
                capabilities: Mutex::new(StoreCapabilities::default()),
                bucket,
                region,
                credentials,
                rt,
                max_retries,
                retry_backoff_ms,
            }),
        };
        // Env-driven optional connection-pool prewarm. No-op when
        // WMBT_KV_S3_PREWARM is unset / 0.
        store.prewarm_from_env();
        Ok(store)
    }

    pub fn from_env() -> Result<Self, WalStoreError> {
        Self::new(S3ObjectStoreConfig::from_env()?)
    }

    /// Pre-warm the hyper connection pool with `parallelism` concurrent
    /// HEAD requests to a known-absent key. The 404 response is fine -
    /// each completed request leaves an idle HTTP/1 keepalive socket in
    /// the pool, so subsequent parallel chunked GETs skip TCP handshake
    /// + `SigV4` cold-path.
    ///
    /// Diagnosis (RFC 0006/0007 finding from 2026-05-15): with only 2
    /// runtime worker threads, a 23-way cold fanout paid ~70 ms vs ~10
    /// ms on the warmed pool. Even after bumping runtime to 8 workers,
    /// trial-1 first-cold-fanout still pays a connection-establish tax;
    /// this method amortizes it at `S3ObjectStore::new` time when the
    /// caller knows it will issue parallel fanouts later.
    ///
    /// Best-effort, failures are logged, not propagated. Use
    /// `WMBT_KV_S3_PREWARM=<N>` to enable from env (default 0 = off).
    pub fn prewarm_pool(&self, parallelism: usize) {
        if parallelism == 0 {
            return;
        }
        let bucket = self.inner.bucket.clone();
        self.inner.rt.block_on(async move {
            let mut set = tokio::task::JoinSet::new();
            for i in 0..parallelism {
                let b = bucket.clone();
                let probe_key = format!("wombatkv/__prewarm__/{i}");
                set.spawn(async move {
                    // HEAD on a never-present key, we don't care about
                    // the 404. The TCP+TLS+SigV4 round-trip warms the
                    // connection pool, which is what we want.
                    let _ = b.head_object(&probe_key).await;
                });
            }
            while set.join_next().await.is_some() {}
        });
    }

    /// Apply env-driven prewarm at construction time.
    ///
    /// 0.1.0-alpha default: 8 concurrent warmup HEADs. The block-prefix
    /// fanout pays a measurable connection-establish tax on the first
    /// parallel S3 burst (see RFC 0006/0007 §10.2: ~70 ms vs ~10 ms
    /// warmed); prewarm at handle construction amortises that cost.
    ///
    /// Opt-out: `WMBT_KV_S3_PREWARM=0` or `WMBT_KV_S3_PREWARM=` (empty).
    /// Explicit non-zero values still win.
    fn prewarm_from_env(&self) {
        /// Alpha headline default: 8 concurrent warmup HEADs.
        const DEFAULT_PREWARM: usize = 8;
        let parallelism = match std::env::var("WMBT_KV_S3_PREWARM") {
            // Explicit opt-out: empty disables.
            Ok(raw) if raw.is_empty() => 0,
            // Explicit value: parse; unparseable falls back to 0.
            Ok(raw) => raw.parse::<usize>().unwrap_or(0),
            // Unset → alpha default-on.
            Err(_) => DEFAULT_PREWARM,
        };
        if parallelism > 0 {
            self.prewarm_pool(parallelism);
        }
    }

    pub fn ensure_bucket(&self) -> Result<(), WalStoreError> {
        let bucket = self.inner.bucket.clone();
        let bucket_name = self.inner.bucket_name.clone();
        self.with_retry(|| {
            let exists = self
                .inner
                .rt
                .block_on(async { bucket.exists().await })
                .map_err(|err| WalStoreError::S3(format!("bucket exists failed: {err}")))?;
            if exists {
                return Ok(());
            }

            let region = self.inner.region.clone();
            let credentials = self.inner.credentials.clone();
            let _ = self
                .inner
                .rt
                .block_on(async {
                    Bucket::create_with_path_style(
                        &bucket_name,
                        region,
                        credentials,
                        BucketConfiguration::default(),
                    )
                    .await
                })
                .map_err(|err| WalStoreError::S3(format!("bucket create failed: {err}")))?;
            Ok(())
        })?;

        self.probe_capabilities_if_needed()?;
        Ok(())
    }

    fn with_retry<T>(
        &self,
        mut operation: impl FnMut() -> Result<T, WalStoreError>,
    ) -> Result<T, WalStoreError> {
        retry_with_backoff(self.inner.max_retries, self.inner.retry_backoff_ms, &mut operation)
    }

    fn normalize_key(key: &str) -> &str {
        key.strip_prefix('/').unwrap_or(key)
    }

    fn is_default_dev_credentials(access_key: &str, secret_key: &str) -> bool {
        access_key == "minioadmin" && secret_key == "minioadmin"
    }

    fn local_dev_mode_enabled(endpoint: &str) -> bool {
        if let Ok(value) = std::env::var("WMBT_KV_LOCAL_DEV") {
            return value == "1" || value.eq_ignore_ascii_case("true");
        }
        let lowered = endpoint.to_ascii_lowercase();
        lowered.contains("127.0.0.1")
            || lowered.contains("localhost")
            || lowered.contains("host.docker.internal")
            || lowered.starts_with("http://0.0.0.0")
    }

    fn etag_if_match_value(etag: &str) -> String {
        if etag.contains('"') || etag.starts_with("W/") {
            etag.to_string()
        } else {
            format!("\"{etag}\"")
        }
    }

    fn get_cached_capabilities(&self) -> Result<StoreCapabilities, WalStoreError> {
        let guard =
            self.inner.capabilities.lock().map_err(|_| WalStoreError::ObjectStorePoisoned)?;
        Ok(*guard)
    }

    fn set_cached_capabilities(
        &self,
        capabilities: StoreCapabilities,
    ) -> Result<(), WalStoreError> {
        let mut guard =
            self.inner.capabilities.lock().map_err(|_| WalStoreError::ObjectStorePoisoned)?;
        *guard = capabilities;
        Ok(())
    }

    fn conditional_put_with_headers(
        &self,
        key: &str,
        payload: &[u8],
        headers: &HeaderMap,
    ) -> Result<u16, WalStoreError> {
        let key = Self::normalize_key(key).to_string();
        let payload = payload.to_vec();
        self.with_retry(|| {
            let bucket = self.inner.bucket.clone();
            let bucket = bucket.with_extra_headers(headers.clone()).map_err(|err| {
                WalStoreError::S3(format!("bucket header override failed: key={key}: {err}"))
            })?;
            let response =
                self.inner.rt.block_on(async { bucket.put_object(&key, &payload).await }).map_err(
                    |err| WalStoreError::S3(format!("conditional put failed: key={key}: {err}")),
                )?;
            Ok(response.status_code())
        })
    }

    fn head_etag(&self, key: &str) -> Result<Option<String>, WalStoreError> {
        let key = Self::normalize_key(key).to_string();
        self.with_retry(|| {
            let bucket = self.inner.bucket.clone();
            let (head, status) =
                self.inner.rt.block_on(async { bucket.head_object(&key).await }).map_err(
                    |err| WalStoreError::S3(format!("head_object failed: key={key}: {err}")),
                )?;
            if status == 404 {
                return Ok(None);
            }
            if !(200..300).contains(&status) {
                return Err(WalStoreError::S3(format!(
                    "head_object failed: key={key} status={status}"
                )));
            }
            Ok(head.e_tag.filter(|value| !value.is_empty()))
        })
    }

    fn current_token_for_key(&self, key: &str) -> Result<Option<CasToken>, WalStoreError> {
        self.read_with_token(key).map(|maybe| maybe.map(|(_, token)| token))
    }

    fn probe_capabilities_if_needed(&self) -> Result<(), WalStoreError> {
        let cache_key = self.inner.capability_cache_key.clone();
        let capabilities =
            s3_capability_cache().get_or_probe(&cache_key, || self.probe_capabilities())?;
        self.set_cached_capabilities(capabilities)
    }

    fn probe_capabilities(&self) -> Result<StoreCapabilities, WalStoreError> {
        let create_key = format!(
            "wombatkv/capability-probe/{}/create-{}",
            self.inner.bucket_name,
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| d.as_nanos())
        );
        let update_key = format!("{create_key}-update");

        let mut create_headers = HeaderMap::new();
        create_headers.insert(IF_NONE_MATCH, HeaderValue::from_static("*"));
        let conditional_create = match self.conditional_put_with_headers(
            &create_key,
            b"capability-probe-create",
            &create_headers,
        ) {
            Ok(status) if (200..300).contains(&status) || status == 409 || status == 412 => true,
            Ok(status) if status == 400 || status == 405 || status == 501 => false,
            Ok(status) => {
                return Err(WalStoreError::S3(format!(
                    "conditional create probe failed: endpoint={} bucket={} status={status}",
                    self.inner.endpoint, self.inner.bucket_name
                )));
            }
            Err(_) => false,
        };

        let _ = self.delete_object(&create_key);

        self.put_object_overwrite(&update_key, b"capability-probe-update-v1")?;
        let conditional_update = if let Some(etag) = self.head_etag(&update_key)? {
            let mut update_headers = HeaderMap::new();
            let header_value = HeaderValue::from_str(&Self::etag_if_match_value(&etag))
                .map_err(|err| WalStoreError::S3(format!("invalid etag header value: {err}")))?;
            update_headers.insert(IF_MATCH, header_value);
            match self.conditional_put_with_headers(
                &update_key,
                b"capability-probe-update-v2",
                &update_headers,
            ) {
                Ok(status) if (200..300).contains(&status) || status == 409 || status == 412 => {
                    true
                }
                Ok(status) if status == 400 || status == 405 || status == 501 => false,
                Ok(status) => {
                    return Err(WalStoreError::S3(format!(
                        "conditional update probe failed: endpoint={} bucket={} status={status}",
                        self.inner.endpoint, self.inner.bucket_name
                    )));
                }
                Err(_) => false,
            }
        } else {
            false
        };

        let _ = self.delete_object(&update_key);
        Ok(StoreCapabilities { conditional_create, conditional_update })
    }
}

impl ObjectStore for S3ObjectStore {
    fn put_object(&self, key: &str, payload: &[u8]) -> Result<(), WalStoreError> {
        let key = Self::normalize_key(key).to_string();
        let payload = payload.to_vec();
        self.with_retry(|| {
            let bucket = self.inner.bucket.clone();
            let response =
                self.inner.rt.block_on(async { bucket.put_object(&key, &payload).await }).map_err(
                    |err| WalStoreError::S3(format!("put_object failed: key={key}: {err}")),
                )?;
            let status = response.status_code();
            if !(200..300).contains(&status) {
                return Err(WalStoreError::S3(format!(
                    "put_object failed: key={key} status={status}"
                )));
            }
            Ok(())
        })
    }

    fn put_object_overwrite(&self, key: &str, payload: &[u8]) -> Result<(), WalStoreError> {
        self.put_object(key, payload)
    }

    fn get_object(&self, key: &str) -> Result<Vec<u8>, WalStoreError> {
        let key = Self::normalize_key(key).to_string();
        self.with_retry(|| {
            let bucket = self.inner.bucket.clone();
            let data =
                self.inner.rt.block_on(async { bucket.get_object(&key).await }).map_err(|err| {
                    WalStoreError::S3(format!("get_object failed: key={key}: {err}"))
                })?;
            let status = data.status_code();
            if status == 404 {
                return Err(WalStoreError::ObjectNotFound(key.clone()));
            }
            if !(200..300).contains(&status) {
                return Err(WalStoreError::S3(format!(
                    "get_object failed: key={key} status={status}"
                )));
            }
            Ok(data.into_bytes().to_vec())
        })
    }

    /// Parallel fan-out variant: one `block_on`, N concurrent in-flight
    /// rust-s3 `get_object` futures sharing the runtime's reqwest
    /// connection pool. Mirrors the an earlier internal prototype §4c pattern (compio per
    /// shard, `futures::join_all`). Order-preserving.
    ///
    /// Skips the `with_retry` wrapper for now (callers fall back to
    /// monolithic on miss). TODO: add retry inside each task.
    fn get_object_many(&self, keys: &[String]) -> Vec<Result<Vec<u8>, WalStoreError>> {
        let normalized: Vec<String> =
            keys.iter().map(|k| Self::normalize_key(k).to_string()).collect();
        let n = normalized.len();
        let bucket_template = self.inner.bucket.clone();
        let ordered: Vec<Option<Result<Vec<u8>, WalStoreError>>> =
            self.inner.rt.block_on(async move {
                let mut set = tokio::task::JoinSet::new();
                for (idx, key) in normalized.into_iter().enumerate() {
                    let bucket = bucket_template.clone();
                    set.spawn(async move {
                        let result = match bucket.get_object(&key).await {
                            Ok(data) => {
                                let status = data.status_code();
                                if status == 404 {
                                    Err(WalStoreError::ObjectNotFound(key.clone()))
                                } else if !(200..300).contains(&status) {
                                    Err(WalStoreError::S3(format!(
                                        "get_object failed: key={key} status={status}"
                                    )))
                                } else {
                                    Ok(data.into_bytes().to_vec())
                                }
                            }
                            Err(err) => Err(WalStoreError::S3(format!(
                                "get_object failed: key={key}: {err}"
                            ))),
                        };
                        (idx, result)
                    });
                }
                let mut out: Vec<Option<Result<Vec<u8>, WalStoreError>>> =
                    (0..n).map(|_| None).collect();
                while let Some(joined) = set.join_next().await {
                    match joined {
                        Ok((idx, res)) => out[idx] = Some(res),
                        Err(err) => {
                            if let Some(slot) = out.iter_mut().find(|s| s.is_none()) {
                                *slot = Some(Err(WalStoreError::S3(format!(
                                    "get_object_many task panic: {err}"
                                ))));
                            }
                        }
                    }
                }
                out
            });
        ordered
            .into_iter()
            .map(|o| {
                o.unwrap_or_else(|| {
                    Err(WalStoreError::S3("get_object_many missing result".to_string()))
                })
            })
            .collect()
    }

    fn get_object_range(
        &self,
        key: &str,
        start: usize,
        end: usize,
    ) -> Result<Vec<u8>, WalStoreError> {
        if start >= end {
            return Ok(Vec::new());
        }

        let key = Self::normalize_key(key).to_string();
        let start_u64 = u64::try_from(start).unwrap_or(u64::MAX);
        let end_u64 = u64::try_from(end.saturating_sub(1)).unwrap_or(u64::MAX);
        self.with_retry(|| {
            let bucket = self.inner.bucket.clone();
            let data = self
                .inner
                .rt
                .block_on(async { bucket.get_object_range(&key, start_u64, Some(end_u64)).await })
                .map_err(|err| {
                    WalStoreError::S3(format!(
                        "get_object_range failed: key={key} start={start_u64} end={end_u64}: {err}"
                    ))
                })?;
            let status = data.status_code();
            if status == 404 {
                return Err(WalStoreError::ObjectNotFound(key.clone()));
            }
            if !(200..300).contains(&status) {
                return Err(WalStoreError::S3(format!(
                    "get_object_range failed: key={key} status={status}"
                )));
            }
            Ok(data.into_bytes().to_vec())
        })
    }

    fn compare_and_swap_object(
        &self,
        key: &str,
        expected: Option<&[u8]>,
        new_payload: &[u8],
    ) -> Result<bool, WalStoreError> {
        let current = match self.get_object(key) {
            Ok(bytes) => Some(bytes),
            Err(WalStoreError::ObjectNotFound(_)) => None,
            Err(err) => return Err(err),
        };

        if current.as_deref() == expected {
            self.put_object_overwrite(key, new_payload)?;
            return Ok(true);
        }

        Ok(false)
    }

    fn read_with_token(&self, key: &str) -> Result<Option<(Vec<u8>, CasToken)>, WalStoreError> {
        let bytes = match self.get_object(key) {
            Ok(bytes) => bytes,
            Err(WalStoreError::ObjectNotFound(_)) => return Ok(None),
            Err(err) => return Err(err),
        };

        let token = if let Some(etag) = self.head_etag(key)? {
            CasToken::ETag(etag)
        } else {
            CasToken::ContentHash(content_hash(&bytes))
        };

        Ok(Some((bytes, token)))
    }

    fn put_if_token_matches(
        &self,
        key: &str,
        payload: &[u8],
        token: &CasToken,
    ) -> Result<ConditionalPutResult, WalStoreError> {
        if !self.capabilities().conditional_update {
            return Ok(ConditionalPutResult::Unsupported);
        }

        let CasToken::ETag(etag) = token else {
            return Ok(ConditionalPutResult::Unsupported);
        };
        let mut headers = HeaderMap::new();
        let header_value = HeaderValue::from_str(&Self::etag_if_match_value(etag))
            .map_err(|err| WalStoreError::S3(format!("invalid etag header value: {err}")))?;
        headers.insert(IF_MATCH, header_value);
        let status = self.conditional_put_with_headers(key, payload, &headers)?;
        match status {
            200..=299 => Ok(ConditionalPutResult::Success),
            404 | 409 | 412 => Ok(ConditionalPutResult::Conflict {
                current_token: self.current_token_for_key(key)?,
            }),
            400 | 405 | 501 => Ok(ConditionalPutResult::Unsupported),
            _ => Err(WalStoreError::S3(format!(
                "put_if_token_matches failed: key={} status={status}",
                Self::normalize_key(key)
            ))),
        }
    }

    fn put_if_absent(
        &self,
        key: &str,
        payload: &[u8],
    ) -> Result<ConditionalPutResult, WalStoreError> {
        if !self.capabilities().conditional_create {
            return Ok(ConditionalPutResult::Unsupported);
        }

        let mut headers = HeaderMap::new();
        headers.insert(IF_NONE_MATCH, HeaderValue::from_static("*"));
        let status = self.conditional_put_with_headers(key, payload, &headers)?;
        match status {
            200..=299 => Ok(ConditionalPutResult::Success),
            404 | 409 | 412 => Ok(ConditionalPutResult::Conflict {
                current_token: self.current_token_for_key(key)?,
            }),
            400 | 405 | 501 => Ok(ConditionalPutResult::Unsupported),
            _ => Err(WalStoreError::S3(format!(
                "put_if_absent failed: key={} status={status}",
                Self::normalize_key(key)
            ))),
        }
    }

    fn capabilities(&self) -> StoreCapabilities {
        self.get_cached_capabilities().unwrap_or_default()
    }

    fn head_object(&self, key: &str) -> Result<bool, WalStoreError> {
        let key = Self::normalize_key(key).to_string();
        self.with_retry(|| {
            let bucket = self.inner.bucket.clone();
            let (_head, status) =
                self.inner.rt.block_on(async { bucket.head_object(&key).await }).map_err(
                    |err| WalStoreError::S3(format!("head_object failed: key={key}: {err}")),
                )?;
            if status == 404 {
                return Ok(false);
            }
            if !(200..300).contains(&status) {
                return Err(WalStoreError::S3(format!(
                    "head_object failed: key={key} status={status}"
                )));
            }
            Ok(true)
        })
    }

    fn list_prefix(&self, prefix: &str) -> Result<Vec<String>, WalStoreError> {
        let prefix = Self::normalize_key(prefix).to_string();
        self.with_retry(|| {
            let bucket = self.inner.bucket.clone();
            let listed =
                self.inner.rt.block_on(async { bucket.list(prefix.clone(), None).await }).map_err(
                    |err| WalStoreError::S3(format!("list failed: prefix={prefix}: {err}")),
                )?;

            let mut out = Vec::new();
            for page in listed {
                for object in page.contents {
                    out.push(object.key);
                }
            }
            Ok(out)
        })
    }

    fn delete_object(&self, key: &str) -> Result<bool, WalStoreError> {
        let key = Self::normalize_key(key).to_string();
        self.with_retry(|| {
            let bucket = self.inner.bucket.clone();
            let response =
                self.inner.rt.block_on(async { bucket.delete_object(&key).await }).map_err(
                    |err| WalStoreError::S3(format!("delete_object failed: key={key}: {err}")),
                )?;

            let status = response.status_code();
            if status == 404 {
                return Ok(false);
            }
            if !(200..300).contains(&status) {
                return Err(WalStoreError::S3(format!(
                    "delete_object failed: key={key} status={status}"
                )));
            }
            Ok(true)
        })
    }
}

fn retry_with_backoff<T>(
    max_retries: u32,
    retry_backoff_ms: u64,
    operation: &mut impl FnMut() -> Result<T, WalStoreError>,
) -> Result<T, WalStoreError> {
    let attempts = max_retries.saturating_add(1);
    let mut last_error: Option<WalStoreError> = None;

    for attempt in 0..attempts {
        match operation() {
            Ok(value) => return Ok(value),
            Err(err) => {
                last_error = Some(err);
                if attempt + 1 < attempts {
                    thread::sleep(Duration::from_millis(retry_backoff_ms));
                }
            }
        }
    }

    Err(last_error.unwrap_or_else(|| WalStoreError::S3("unknown retry failure".to_string())))
}

/// WAL publisher/replayer over an object store.
pub struct WalStore<S: ObjectStore = InMemoryObjectStore> {
    namespace_prefix: String,
    node_id: String,
    object_store: S,
    next_seq: u64,
}

impl<S: ObjectStore> WalStore<S> {
    #[must_use]
    pub fn new(namespace_prefix: impl Into<String>, object_store: S) -> Self {
        Self::with_node_id(namespace_prefix, DEFAULT_NODE_ID, object_store)
    }

    #[must_use]
    pub fn with_node_id(
        namespace_prefix: impl Into<String>,
        node_id: impl Into<String>,
        object_store: S,
    ) -> Self {
        let namespace_prefix = namespace_prefix.into();
        let node_id = sanitize_node_id(&node_id.into());
        let next_seq = recover_next_seq(&namespace_prefix, &node_id, &object_store).unwrap_or(0);
        Self { namespace_prefix, node_id, object_store, next_seq }
    }

    /// Publish a WAL record and verify durability via `head_object`.
    pub fn publish_record(
        &mut self,
        record: &WalRecord,
    ) -> Result<PublishedWalRecord, WalStoreError> {
        let bytes = encode_wal_record(record)?;
        let object_key = loop {
            let candidate = build_canonical_wal_key(
                &self.namespace_prefix,
                &self.node_id,
                current_epoch_ms(),
                self.next_seq,
            );
            if self.object_store.head_object(&candidate)? {
                self.next_seq = self.next_seq.saturating_add(1);
                continue;
            }

            self.object_store.put_object(&candidate, &bytes)?;
            let stored = self.object_store.get_object(&candidate)?;
            if stored == bytes {
                break candidate;
            }
            self.next_seq = self.next_seq.saturating_add(1);
        };
        self.next_seq = self.next_seq.saturating_add(1);

        let exists = self.object_store.head_object(&object_key)?;
        if !exists {
            return Err(WalStoreError::ObjectNotFound(object_key));
        }

        Ok(PublishedWalRecord { object_key })
    }

    /// Replay all records from this namespace prefix.
    pub fn replay_records(&self) -> Result<Vec<WalRecord>, WalStoreError> {
        let mut keys = self.object_store.list_prefix(&self.namespace_prefix)?;
        keys.sort_by(|left, right| compare_wal_object_keys(&self.namespace_prefix, left, right));

        let mut out = Vec::new();
        for key in keys {
            let bytes = self.object_store.get_object(&key)?;
            out.extend(decode_wal_object_records(&bytes)?);
        }
        Ok(out)
    }

    #[cfg(test)]
    #[must_use]
    pub fn object_store(&self) -> S {
        self.object_store.clone()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct BufferedWalRecord {
    seq: u64,
    encoded_record: Vec<u8>,
}

/// Bounded WAL group-commit scheduler that emits single-record or batch objects.
pub struct WalGroupCommitStore<S: ObjectStore = InMemoryObjectStore> {
    wal_store: WalStore<S>,
    policy: WalBatchPolicy,
    buffer: Vec<BufferedWalRecord>,
    buffered_bytes: usize,
    first_buffered_at_ms: Option<u64>,
}

impl<S: ObjectStore> WalGroupCommitStore<S> {
    #[must_use]
    pub fn with_wal_store(wal_store: WalStore<S>, policy: WalBatchPolicy) -> Self {
        Self {
            wal_store,
            policy,
            buffer: Vec::new(),
            buffered_bytes: 0,
            first_buffered_at_ms: None,
        }
    }

    #[must_use]
    pub fn new(
        namespace_prefix: impl Into<String>,
        node_id: impl Into<String>,
        object_store: S,
        policy: WalBatchPolicy,
    ) -> Self {
        let wal_store = WalStore::with_node_id(namespace_prefix, node_id, object_store);
        Self::with_wal_store(wal_store, policy)
    }

    pub fn replay_records(&self) -> Result<Vec<WalRecord>, WalStoreError> {
        self.wal_store.replay_records()
    }

    pub fn publish_record(
        &mut self,
        record: &WalRecord,
        now_ms: u64,
    ) -> Result<GroupCommitPublishResult, WalStoreError> {
        if self.policy.is_single_record_mode() {
            let published = self.wal_store.publish_record(record)?;
            return Ok(GroupCommitPublishResult {
                flushed_object_keys: vec![published.object_key],
                buffered_records: 0,
                buffered_bytes: 0,
            });
        }

        let encoded = encode_wal_record(record)?;
        let mut result = GroupCommitPublishResult::buffered(self.buffer.len(), self.buffered_bytes);

        if self.policy.max_bytes > 0
            && !self.buffer.is_empty()
            && self.buffered_bytes.saturating_add(encoded.len()) > self.policy.max_bytes
        {
            if let Some(flushed_key) = self.flush_buffer_as_object(now_ms)? {
                result.record_flush(flushed_key);
            }
        }

        let seq = self.wal_store.next_seq;
        self.wal_store.next_seq = self.wal_store.next_seq.saturating_add(1);
        self.buffer.push(BufferedWalRecord { seq, encoded_record: encoded });
        self.buffered_bytes =
            self.buffer.iter().map(|record| record.encoded_record.len()).sum::<usize>();
        if self.first_buffered_at_ms.is_none() {
            self.first_buffered_at_ms = Some(now_ms);
        }

        let flush_by_records =
            self.policy.max_records > 0 && self.buffer.len() >= self.policy.max_records;
        let flush_by_bytes =
            self.policy.max_bytes > 0 && self.buffered_bytes >= self.policy.max_bytes;
        if flush_by_records || flush_by_bytes {
            if let Some(flushed_key) = self.flush_buffer_as_object(now_ms)? {
                result.record_flush(flushed_key);
            }
        }

        result.buffered_records = self.buffer.len();
        result.buffered_bytes = self.buffered_bytes;
        Ok(result)
    }

    pub fn flush_due(&mut self, now_ms: u64) -> Result<Option<String>, WalStoreError> {
        if self.buffer.is_empty() || self.policy.flush_interval_ms == 0 {
            return Ok(None);
        }
        let Some(first_buffered_at_ms) = self.first_buffered_at_ms else {
            return Ok(None);
        };
        if now_ms.saturating_sub(first_buffered_at_ms) < self.policy.flush_interval_ms {
            return Ok(None);
        }
        self.flush_buffer_as_object(now_ms)
    }

    pub fn flush_now(&mut self, now_ms: u64) -> Result<Option<String>, WalStoreError> {
        self.flush_buffer_as_object(now_ms)
    }

    fn flush_buffer_as_object(&mut self, now_ms: u64) -> Result<Option<String>, WalStoreError> {
        if self.buffer.is_empty() {
            return Ok(None);
        }

        let epoch_ms = now_ms.max(current_epoch_ms());
        let object_key = if self.buffer.len() == 1 {
            let BufferedWalRecord { seq, encoded_record } = self.buffer.remove(0);
            self.write_single_object_with_seq(seq, &encoded_record, epoch_ms)?
        } else {
            let start_seq = self.buffer.first().map_or(0, |record| record.seq);
            let end_seq = self.buffer.last().map_or(start_seq, |record| record.seq);
            let payload = encode_wal_batch_payload(&self.buffer)?;
            self.write_batch_object(start_seq, end_seq, &payload, epoch_ms)?
        };

        self.buffer.clear();
        self.buffered_bytes = 0;
        self.first_buffered_at_ms = None;
        Ok(Some(object_key))
    }

    fn write_single_object_with_seq(
        &self,
        seq: u64,
        payload: &[u8],
        epoch_ms: u64,
    ) -> Result<String, WalStoreError> {
        let candidate = build_canonical_wal_key(
            &self.wal_store.namespace_prefix,
            &self.wal_store.node_id,
            epoch_ms,
            seq,
        );
        self.wal_store.object_store.put_object(&candidate, payload)?;
        let stored = self.wal_store.object_store.get_object(&candidate)?;
        if stored != payload {
            return Err(WalStoreError::CorruptWal(FormatError::ChecksumMismatch));
        }
        Ok(candidate)
    }

    fn write_batch_object(
        &self,
        start_seq: u64,
        end_seq: u64,
        payload: &[u8],
        epoch_ms: u64,
    ) -> Result<String, WalStoreError> {
        let candidate = build_canonical_wal_batch_key(
            &self.wal_store.namespace_prefix,
            &self.wal_store.node_id,
            epoch_ms,
            start_seq,
            end_seq,
        );
        self.wal_store.object_store.put_object(&candidate, payload)?;
        let stored = self.wal_store.object_store.get_object(&candidate)?;
        if stored != payload {
            return Err(WalStoreError::CorruptWal(FormatError::ChecksumMismatch));
        }
        Ok(candidate)
    }
}

fn sanitize_node_id(node_id: &str) -> String {
    let sanitized = node_id
        .chars()
        .map(|ch| if ch.is_ascii_alphanumeric() || ch == '-' { ch } else { '-' })
        .collect::<String>();
    if sanitized.is_empty() {
        DEFAULT_NODE_ID.to_string()
    } else {
        sanitized
    }
}

/// Build canonical WAL object key:
/// `<namespace>/<epoch_ms>/chunk-<node_id>-<seq>.bin`.
#[must_use]
pub fn build_canonical_wal_key(
    namespace_prefix: &str,
    node_id: &str,
    epoch_ms: u64,
    seq: u64,
) -> String {
    format!("{namespace_prefix}/{epoch_ms:020}/chunk-{node_id}-{seq:020}.bin")
}

/// Build canonical WAL batch object key:
/// `<namespace>/<epoch_ms>/batch-<node_id>-<start_seq>-<end_seq>.bin`.
#[must_use]
pub fn build_canonical_wal_batch_key(
    namespace_prefix: &str,
    node_id: &str,
    epoch_ms: u64,
    start_seq: u64,
    end_seq: u64,
) -> String {
    format!("{namespace_prefix}/{epoch_ms:020}/batch-{node_id}-{start_seq:020}-{end_seq:020}.bin")
}

/// Decode one WAL object payload into one-or-more records.
pub fn decode_wal_object_records(payload: &[u8]) -> Result<Vec<WalRecord>, WalStoreError> {
    if payload.starts_with(WAL_BATCH_MAGIC) {
        decode_wal_batch_payload(payload)
    } else {
        Ok(vec![decode_wal_record(payload)?])
    }
}

/// Count canonical-vs-legacy key shape coverage for a namespace.
#[must_use]
pub fn audit_wal_layout_counts(namespace_prefix: &str, keys: &[String]) -> WalLayoutAuditCounts {
    let mut counts = WalLayoutAuditCounts::default();
    for key in keys {
        if !is_under_namespace(namespace_prefix, key) {
            continue;
        }
        counts.total_keys = counts.total_keys.saturating_add(1);
        match parse_wal_object_key(namespace_prefix, key) {
            Some(parsed) => match parsed.layout {
                WalKeyLayout::CanonicalEpochPartitioned => {
                    counts.canonical_keys = counts.canonical_keys.saturating_add(1);
                }
                WalKeyLayout::LegacyFlat => {
                    counts.legacy_keys = counts.legacy_keys.saturating_add(1);
                }
            },
            None => {
                counts.unknown_keys = counts.unknown_keys.saturating_add(1);
            }
        }
    }
    counts
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum WalObjectKind {
    Single,
    Batch,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ParsedWalObjectKey {
    layout: WalKeyLayout,
    kind: WalObjectKind,
    start_seq: u64,
    end_seq: u64,
    node_id: String,
}

fn is_under_namespace(namespace_prefix: &str, key: &str) -> bool {
    let prefix = format!("{namespace_prefix}/");
    key.starts_with(&prefix)
}

fn parse_canonical_tail(tail: &str) -> Option<(u64, ParsedWalObjectKey)> {
    let (epoch_part, filename) = tail.split_once('/')?;
    if epoch_part.is_empty() || !epoch_part.chars().all(|ch| ch.is_ascii_digit()) {
        return None;
    }
    let epoch_ms = epoch_part.parse::<u64>().ok()?;
    let parsed = parse_filename_to_parsed_key(filename, WalKeyLayout::CanonicalEpochPartitioned)?;
    Some((epoch_ms, parsed))
}

fn parse_node_seq_filename(filename: &str) -> Option<(String, u64)> {
    let stem = filename.strip_prefix("chunk-")?.strip_suffix(".bin")?;
    let (candidate_node, seq_part) = stem.rsplit_once('-')?;
    if candidate_node.is_empty() {
        return None;
    }
    let seq = seq_part.parse::<u64>().ok()?;
    Some((candidate_node.to_string(), seq))
}

fn parse_batch_seq_filename(filename: &str) -> Option<(String, u64, u64)> {
    let stem = filename.strip_prefix("batch-")?.strip_suffix(".bin")?;
    let mut fields = stem.rsplitn(3, '-');
    let end_seq = fields.next()?.parse::<u64>().ok()?;
    let start_seq = fields.next()?.parse::<u64>().ok()?;
    let node_id = fields.next()?;
    if node_id.is_empty() {
        return None;
    }
    Some((node_id.to_string(), start_seq, end_seq))
}

fn parse_filename_to_parsed_key(
    filename: &str,
    layout: WalKeyLayout,
) -> Option<ParsedWalObjectKey> {
    if let Some((node_id, seq)) = parse_node_seq_filename(filename) {
        return Some(ParsedWalObjectKey {
            layout,
            kind: WalObjectKind::Single,
            start_seq: seq,
            end_seq: seq,
            node_id,
        });
    }
    if let Some((node_id, start_seq, end_seq)) = parse_batch_seq_filename(filename) {
        return Some(ParsedWalObjectKey {
            layout,
            kind: WalObjectKind::Batch,
            start_seq,
            end_seq,
            node_id,
        });
    }
    None
}

fn parse_legacy_default_filename(filename: &str) -> Option<u64> {
    let stem = filename.strip_prefix("chunk-")?.strip_suffix(".bin")?;
    stem.parse::<u64>().ok()
}

fn parse_wal_object_key(namespace_prefix: &str, key: &str) -> Option<ParsedWalObjectKey> {
    let prefix = format!("{namespace_prefix}/");
    let tail = key.strip_prefix(&prefix)?;

    if let Some((_, parsed)) = parse_canonical_tail(tail) {
        return Some(parsed);
    }

    if let Some(parsed) = parse_filename_to_parsed_key(tail, WalKeyLayout::LegacyFlat) {
        return Some(parsed);
    }

    let seq = parse_legacy_default_filename(tail)?;
    Some(ParsedWalObjectKey {
        layout: WalKeyLayout::LegacyFlat,
        kind: WalObjectKind::Single,
        start_seq: seq,
        end_seq: seq,
        node_id: DEFAULT_NODE_ID.to_string(),
    })
}

fn compare_wal_object_keys(namespace_prefix: &str, left: &str, right: &str) -> Ordering {
    let left_parsed = parse_wal_object_key(namespace_prefix, left);
    let right_parsed = parse_wal_object_key(namespace_prefix, right);
    match (left_parsed, right_parsed) {
        (Some(left_parsed), Some(right_parsed)) => {
            left_parsed.start_seq.cmp(&right_parsed.start_seq).then_with(|| left.cmp(right))
        }
        (Some(_), None) => Ordering::Less,
        (None, Some(_)) => Ordering::Greater,
        (None, None) => left.cmp(right),
    }
}

fn encode_wal_batch_payload(records: &[BufferedWalRecord]) -> Result<Vec<u8>, WalStoreError> {
    if records.is_empty() {
        return Err(WalStoreError::CorruptWal(FormatError::UnexpectedEof));
    }

    let start_seq = records.first().map_or(0, |record| record.seq);
    let end_seq = records.last().map_or(start_seq, |record| record.seq);
    let record_count = u32::try_from(records.len())
        .map_err(|_| WalStoreError::CorruptWal(FormatError::FieldTooLarge))?;
    let mut body = Vec::new();
    for record in records {
        let encoded_len = u32::try_from(record.encoded_record.len())
            .map_err(|_| WalStoreError::CorruptWal(FormatError::FieldTooLarge))?;
        body.extend_from_slice(&encoded_len.to_le_bytes());
        body.extend_from_slice(&record.encoded_record);
    }
    let checksum = wombatkv_format::checksum32(&body);
    let mut out = Vec::with_capacity(WAL_BATCH_HEADER_SIZE + body.len());
    out.extend_from_slice(WAL_BATCH_MAGIC);
    out.extend_from_slice(&start_seq.to_le_bytes());
    out.extend_from_slice(&end_seq.to_le_bytes());
    out.extend_from_slice(&record_count.to_le_bytes());
    out.extend_from_slice(&checksum.to_le_bytes());
    out.extend_from_slice(&body);
    Ok(out)
}

fn decode_wal_batch_payload(payload: &[u8]) -> Result<Vec<WalRecord>, WalStoreError> {
    if payload.len() < WAL_BATCH_HEADER_SIZE {
        return Err(WalStoreError::CorruptWal(FormatError::UnexpectedEof));
    }
    if &payload[..WAL_BATCH_MAGIC.len()] != WAL_BATCH_MAGIC {
        return Err(WalStoreError::CorruptWal(FormatError::InvalidMagic));
    }
    let start_seq = u64::from_le_bytes([
        payload[12],
        payload[13],
        payload[14],
        payload[15],
        payload[16],
        payload[17],
        payload[18],
        payload[19],
    ]);
    let end_seq = u64::from_le_bytes([
        payload[20],
        payload[21],
        payload[22],
        payload[23],
        payload[24],
        payload[25],
        payload[26],
        payload[27],
    ]);
    let record_count = u32::from_le_bytes([payload[28], payload[29], payload[30], payload[31]]);
    let checksum = u32::from_le_bytes([payload[32], payload[33], payload[34], payload[35]]);
    let body = &payload[WAL_BATCH_HEADER_SIZE..];
    if wombatkv_format::checksum32(body) != checksum {
        return Err(WalStoreError::CorruptWal(FormatError::ChecksumMismatch));
    }

    let expected_count = end_seq.saturating_sub(start_seq).saturating_add(1);
    if expected_count != u64::from(record_count) {
        return Err(WalStoreError::CorruptWal(FormatError::FieldTooLarge));
    }

    let mut cursor = 0_usize;
    let mut out = Vec::with_capacity(usize::try_from(record_count).unwrap_or(0));
    for _ in 0..record_count {
        if body.len().saturating_sub(cursor) < 4 {
            return Err(WalStoreError::CorruptWal(FormatError::UnexpectedEof));
        }
        let encoded_len = usize::try_from(u32::from_le_bytes([
            body[cursor],
            body[cursor + 1],
            body[cursor + 2],
            body[cursor + 3],
        ]))
        .map_err(|_| WalStoreError::CorruptWal(FormatError::FieldTooLarge))?;
        cursor = cursor.saturating_add(4);
        if body.len().saturating_sub(cursor) < encoded_len {
            return Err(WalStoreError::CorruptWal(FormatError::UnexpectedEof));
        }
        let record_bytes = &body[cursor..cursor + encoded_len];
        cursor = cursor.saturating_add(encoded_len);
        out.push(decode_wal_record(record_bytes)?);
    }
    if cursor != body.len() {
        return Err(WalStoreError::CorruptWal(FormatError::UnexpectedEof));
    }
    Ok(out)
}

fn current_epoch_ms() -> u64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => u64::try_from(duration.as_millis()).unwrap_or(u64::MAX),
        Err(_) => 0,
    }
}

fn recover_next_seq<S: ObjectStore>(
    namespace_prefix: &str,
    node_id: &str,
    object_store: &S,
) -> Result<u64, WalStoreError> {
    let keys = object_store.list_prefix(namespace_prefix)?;
    let max = keys
        .iter()
        .filter_map(|key| parse_seq_for_node(namespace_prefix, node_id, key))
        .max()
        .unwrap_or(0);
    Ok(max.saturating_add(1))
}

fn parse_seq_for_node(namespace_prefix: &str, node_id: &str, key: &str) -> Option<u64> {
    let parsed = parse_wal_object_key(namespace_prefix, key)?;
    if parsed.node_id == node_id {
        return Some(parsed.end_seq);
    }
    None
}

/// Durable publish acknowledgement.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PublishedWalRecord {
    pub object_key: String,
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::{Mutex, OnceLock};

    use super::{
        audit_wal_layout_counts, decode_wal_object_records, retry_with_backoff, CapabilityCache,
        CasToken, ConditionalPutResult, InMemoryObjectStore, ObjectStore, S3ObjectStore,
        S3ObjectStoreConfig, WalBatchPolicy, WalGroupCommitStore, WalStore, WalStoreError,
    };
    use wombatkv_format::{encode_wal_record, WalRecord};

    fn env_guard() -> std::sync::MutexGuard<'static, ()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(())).lock().expect("env mutex")
    }

    #[test]
    fn publish_restart_replay_round_trip() {
        let object_store = InMemoryObjectStore::default();
        let mut first = WalStore::new("wal/tenant-a/fp", object_store.clone());
        first
            .publish_record(&WalRecord {
                key: b"k1".to_vec(),
                meta: b"m1".to_vec(),
                payload: b"p1".to_vec(),
            })
            .expect("publish first");

        let second = WalStore::new("wal/tenant-a/fp", object_store);
        let replayed = second.replay_records().expect("replay");
        assert_eq!(replayed.len(), 1);
        assert_eq!(replayed[0].payload, b"p1".to_vec());
    }

    #[test]
    fn checksum_mismatch_rejects_corrupt_record() {
        let object_store = InMemoryObjectStore::default();
        let mut writer = WalStore::new("wal/tenant-a/fp", object_store.clone());
        let published = writer
            .publish_record(&WalRecord {
                key: b"k1".to_vec(),
                meta: b"m1".to_vec(),
                payload: b"p1".to_vec(),
            })
            .expect("publish");

        let mut bytes = object_store.get_object(&published.object_key).expect("fetch object");
        let last = bytes.len() - 1;
        bytes[last] ^= 0xFF;
        object_store.overwrite_for_test(&published.object_key, &bytes).expect("overwrite");

        let reader = WalStore::new("wal/tenant-a/fp", object_store);
        let replay = reader.replay_records();
        assert!(matches!(
            replay,
            Err(WalStoreError::CorruptWal(wombatkv_format::FormatError::ChecksumMismatch))
        ));
    }

    #[test]
    fn compare_and_swap_updates_only_on_expected_value_match() {
        let store = InMemoryObjectStore::default();
        assert!(store
            .compare_and_swap_object("manifests/t1/fp/latest", None, b"snapshot-1")
            .expect("cas create"));
        assert!(!store
            .compare_and_swap_object(
                "manifests/t1/fp/latest",
                Some(b"stale-snapshot"),
                b"snapshot-2"
            )
            .expect("cas stale"));
        assert!(store
            .compare_and_swap_object("manifests/t1/fp/latest", Some(b"snapshot-1"), b"snapshot-2")
            .expect("cas match"));
        assert_eq!(
            store.get_object("manifests/t1/fp/latest").expect("fetch latest"),
            b"snapshot-2".to_vec()
        );
    }

    #[test]
    fn read_with_token_returns_stable_token_for_unchanged_object() {
        let store = InMemoryObjectStore::default();
        store.put_object("manifests/t1/fp/latest", b"snapshot-1").expect("put");

        let first = store
            .read_with_token("manifests/t1/fp/latest")
            .expect("read first")
            .expect("present first");
        let second = store
            .read_with_token("manifests/t1/fp/latest")
            .expect("read second")
            .expect("present second");

        assert_eq!(first.0, b"snapshot-1".to_vec());
        assert_eq!(second.0, b"snapshot-1".to_vec());
        assert_eq!(first.1, second.1);
    }

    #[test]
    fn put_if_token_matches_reports_conflict_for_stale_token() {
        let store = InMemoryObjectStore::default();
        store.put_object_overwrite("manifests/t1/fp/latest", b"snapshot-1").expect("seed");
        let (_, stale_token) =
            store.read_with_token("manifests/t1/fp/latest").expect("read token").expect("present");

        assert_eq!(
            store
                .put_if_token_matches("manifests/t1/fp/latest", b"snapshot-2", &stale_token)
                .expect("first conditional"),
            ConditionalPutResult::Success
        );

        let result = store
            .put_if_token_matches("manifests/t1/fp/latest", b"snapshot-3", &stale_token)
            .expect("stale conditional");
        match result {
            ConditionalPutResult::Conflict { current_token: Some(current) } => {
                let (_, latest_token) = store
                    .read_with_token("manifests/t1/fp/latest")
                    .expect("read latest")
                    .expect("present latest");
                assert_eq!(current, latest_token);
            }
            other => panic!("expected conflict, got {other:?}"),
        }
    }

    #[test]
    fn put_if_absent_enforces_create_once_semantics() {
        let store = InMemoryObjectStore::default();
        assert_eq!(
            store.put_if_absent("manifests/t1/fp/latest", b"snapshot-1").expect("create"),
            ConditionalPutResult::Success
        );

        let second =
            store.put_if_absent("manifests/t1/fp/latest", b"snapshot-2").expect("create conflict");
        match second {
            ConditionalPutResult::Conflict { current_token: Some(CasToken::ContentHash(_)) } => {}
            other => panic!("expected conflict with token, got {other:?}"),
        }

        assert_eq!(
            store.get_object("manifests/t1/fp/latest").expect("fetch"),
            b"snapshot-1".to_vec()
        );
    }

    #[test]
    fn capability_probe_cache_is_cached_per_backend_bucket() {
        let cache = CapabilityCache::default();
        let probe_count = AtomicU32::new(0);

        let first = cache
            .get_or_probe("s3|http://127.0.0.1:9000|tenant-a", || {
                probe_count.fetch_add(1, Ordering::SeqCst);
                Ok(super::StoreCapabilities { conditional_create: true, conditional_update: false })
            })
            .expect("first probe");
        let second = cache
            .get_or_probe("s3|http://127.0.0.1:9000|tenant-a", || {
                probe_count.fetch_add(1, Ordering::SeqCst);
                Ok(super::StoreCapabilities { conditional_create: false, conditional_update: true })
            })
            .expect("second lookup cached");
        let third = cache
            .get_or_probe("s3|http://127.0.0.1:9000|tenant-b", || {
                probe_count.fetch_add(1, Ordering::SeqCst);
                Ok(super::StoreCapabilities {
                    conditional_create: false,
                    conditional_update: false,
                })
            })
            .expect("third probe distinct bucket");

        assert_eq!(probe_count.load(Ordering::SeqCst), 2);
        assert_eq!(first, second);
        assert_ne!(first, third);
    }

    #[test]
    fn s3_rejects_default_dev_credentials_for_non_local_endpoint() {
        let _guard = env_guard();
        std::env::set_var("WMBT_KV_LOCAL_DEV", "0");
        let config = S3ObjectStoreConfig {
            endpoint: "https://s3.example.com".to_string(),
            bucket: "wombatkv".to_string(),
            access_key: "minioadmin".to_string(),
            secret_key: "minioadmin".to_string(),
            region: "us-east-1".to_string(),
            max_retries: 0,
            retry_backoff_ms: 0,
        };
        let result = S3ObjectStore::new(config);
        std::env::remove_var("WMBT_KV_LOCAL_DEV");
        assert!(
            matches!(result, Err(WalStoreError::InvalidConfig(message)) if message.contains("WMBT_KV_LOCAL_DEV"))
        );
    }

    #[test]
    fn s3_allows_default_dev_credentials_for_local_endpoint() {
        let _guard = env_guard();
        std::env::remove_var("WMBT_KV_LOCAL_DEV");
        let config = S3ObjectStoreConfig {
            endpoint: "http://127.0.0.1:9000".to_string(),
            bucket: "wombatkv".to_string(),
            access_key: "minioadmin".to_string(),
            secret_key: "minioadmin".to_string(),
            region: "us-east-1".to_string(),
            max_retries: 0,
            retry_backoff_ms: 0,
        };
        assert!(S3ObjectStore::new(config).is_ok());
    }

    #[test]
    fn delete_object_removes_existing_key() {
        let store = InMemoryObjectStore::default();
        store.put_object("gc/tombstones/key-1", b"x").expect("put");
        assert!(store.delete_object("gc/tombstones/key-1").expect("delete"));
        assert!(!store.delete_object("gc/tombstones/key-1").expect("delete missing"));
        assert!(matches!(
            store.get_object("gc/tombstones/key-1"),
            Err(WalStoreError::ObjectNotFound(_))
        ));
    }

    #[test]
    fn restart_with_same_node_id_continues_sequence_without_collision() {
        let object_store = InMemoryObjectStore::default();
        let mut first = WalStore::with_node_id("wal/tenant-a/fp", "node-a", object_store.clone());
        let first_published = first
            .publish_record(&WalRecord {
                key: b"k1".to_vec(),
                meta: b"m1".to_vec(),
                payload: b"p1".to_vec(),
            })
            .expect("publish first");

        let mut restarted =
            WalStore::with_node_id("wal/tenant-a/fp", "node-a", object_store.clone());
        let second_published = restarted
            .publish_record(&WalRecord {
                key: b"k2".to_vec(),
                meta: b"m2".to_vec(),
                payload: b"p2".to_vec(),
            })
            .expect("publish second");

        assert_ne!(first_published.object_key, second_published.object_key);

        let replayed = restarted.replay_records().expect("replay");
        assert_eq!(replayed.len(), 2);
        assert_eq!(replayed[0].payload, b"p1".to_vec());
        assert_eq!(replayed[1].payload, b"p2".to_vec());
    }

    #[test]
    fn cross_node_keys_are_unique_for_same_sequence_numbers() {
        let object_store = InMemoryObjectStore::default();
        let mut node_a = WalStore::with_node_id("wal/tenant-a/fp", "node-a", object_store.clone());
        let mut node_b = WalStore::with_node_id("wal/tenant-a/fp", "node-b", object_store.clone());

        let a = node_a
            .publish_record(&WalRecord {
                key: b"k-a".to_vec(),
                meta: b"m-a".to_vec(),
                payload: b"p-a".to_vec(),
            })
            .expect("publish a");
        let b = node_b
            .publish_record(&WalRecord {
                key: b"k-b".to_vec(),
                meta: b"m-b".to_vec(),
                payload: b"p-b".to_vec(),
            })
            .expect("publish b");

        assert_ne!(a.object_key, b.object_key);
    }

    #[test]
    fn publish_record_uses_epoch_partitioned_canonical_key_shape() {
        let object_store = InMemoryObjectStore::default();
        let mut wal = WalStore::with_node_id("wal/tenant-a/fp", "node-a", object_store);
        let published = wal
            .publish_record(&WalRecord {
                key: b"k1".to_vec(),
                meta: b"m1".to_vec(),
                payload: b"p1".to_vec(),
            })
            .expect("publish");

        let namespace_prefix = "wal/tenant-a/fp/";
        assert!(published.object_key.starts_with(namespace_prefix));
        let tail = &published.object_key[namespace_prefix.len()..];
        let (epoch_partition, filename) = tail.split_once('/').expect("epoch partition");
        assert!(epoch_partition.chars().all(|ch| ch.is_ascii_digit()));
        assert!(filename.starts_with("chunk-node-a-"));
        assert!(std::path::Path::new(filename)
            .extension()
            .is_some_and(|ext| ext.eq_ignore_ascii_case("bin")));
    }

    #[test]
    fn restart_recovers_next_seq_from_mixed_legacy_and_canonical_layouts() {
        let object_store = InMemoryObjectStore::default();
        object_store
            .put_object("wal/tenant-a/fp/chunk-node-a-00000000000000000005.bin", b"legacy")
            .expect("insert legacy");
        object_store
            .put_object(
                "wal/tenant-a/fp/00000000000170000000/chunk-node-a-00000000000000000007.bin",
                b"canonical",
            )
            .expect("insert canonical");

        let mut restarted = WalStore::with_node_id("wal/tenant-a/fp", "node-a", object_store);
        let published = restarted
            .publish_record(&WalRecord {
                key: b"k2".to_vec(),
                meta: b"m2".to_vec(),
                payload: b"p2".to_vec(),
            })
            .expect("publish");
        assert!(published.object_key.ends_with("chunk-node-a-00000000000000000008.bin"));
    }

    #[test]
    fn replay_records_orders_mixed_layouts_by_sequence_then_key() {
        let object_store = InMemoryObjectStore::default();
        let namespace = "wal/tenant-a/fp";

        let seq_ten =
            WalRecord { key: b"k10".to_vec(), meta: b"m10".to_vec(), payload: b"p10".to_vec() };
        let seq_two =
            WalRecord { key: b"k2".to_vec(), meta: b"m2".to_vec(), payload: b"p2".to_vec() };

        object_store
            .put_object(
                "wal/tenant-a/fp/00000000000170000000/chunk-node-a-00000000000000000010.bin",
                &encode_wal_record(&seq_ten).expect("encode seq10"),
            )
            .expect("insert seq10");
        object_store
            .put_object(
                "wal/tenant-a/fp/chunk-node-a-00000000000000000002.bin",
                &encode_wal_record(&seq_two).expect("encode seq2"),
            )
            .expect("insert seq2");

        let replayed = WalStore::with_node_id(namespace, "node-a", object_store)
            .replay_records()
            .expect("replay");
        assert_eq!(replayed.len(), 2);
        assert_eq!(replayed[0].payload, b"p2".to_vec());
        assert_eq!(replayed[1].payload, b"p10".to_vec());
    }

    #[test]
    fn audit_wal_layout_counts_classifies_canonical_vs_legacy_keys() {
        let namespace = "wal/tenant-a/fp";
        let keys = vec![
            "wal/tenant-a/fp/00000000000170000000/chunk-node-a-00000000000000000010.bin"
                .to_string(),
            "wal/tenant-a/fp/chunk-node-a-00000000000000000002.bin".to_string(),
            "wal/tenant-a/fp/chunk-00000000000000000003.bin".to_string(),
            "wal/tenant-a/fp/not-a-wal-object.json".to_string(),
            "wal/tenant-b/fp/chunk-node-a-00000000000000000001.bin".to_string(),
        ];

        let counts = audit_wal_layout_counts(namespace, &keys);
        assert_eq!(counts.total_keys, 4);
        assert_eq!(counts.canonical_keys, 1);
        assert_eq!(counts.legacy_keys, 2);
        assert_eq!(counts.unknown_keys, 1);
    }

    #[test]
    fn group_commit_flushes_by_time_window_into_single_batch_object() {
        let object_store = InMemoryObjectStore::default();
        let policy = WalBatchPolicy { flush_interval_ms: 100, max_records: 32, max_bytes: 4096 };
        let mut group =
            WalGroupCommitStore::new("wal/tenant-a/fp", "node-a", object_store.clone(), policy);

        let first = group
            .publish_record(
                &WalRecord { key: b"k1".to_vec(), meta: b"m1".to_vec(), payload: b"p1".to_vec() },
                1_000,
            )
            .expect("publish first");
        assert!(first.flushed_object_keys.is_empty());

        let second = group
            .publish_record(
                &WalRecord { key: b"k2".to_vec(), meta: b"m2".to_vec(), payload: b"p2".to_vec() },
                1_050,
            )
            .expect("publish second");
        assert!(second.flushed_object_keys.is_empty());

        let flushed = group.flush_due(1_101).expect("flush due");
        let flushed_key = flushed.expect("flushed key");
        assert!(flushed_key.contains("/batch-node-a-"));

        let keys = object_store.list_prefix("wal/tenant-a/fp/").expect("list wal keys");
        assert_eq!(keys.len(), 1);

        let replayed = group.replay_records().expect("replay");
        assert_eq!(replayed.len(), 2);
        assert_eq!(replayed[0].payload, b"p1".to_vec());
        assert_eq!(replayed[1].payload, b"p2".to_vec());
    }

    #[test]
    fn group_commit_flushes_by_size_threshold_with_bounded_record_count() {
        let object_store = InMemoryObjectStore::default();
        let record = WalRecord { key: b"k".to_vec(), meta: b"m".to_vec(), payload: vec![1_u8; 64] };
        let encoded_len = encode_wal_record(&record).expect("encode").len();
        let policy = WalBatchPolicy {
            flush_interval_ms: 5_000,
            max_records: 32,
            max_bytes: encoded_len * 2,
        };
        let mut group =
            WalGroupCommitStore::new("wal/tenant-a/fp", "node-a", object_store.clone(), policy);

        group.publish_record(&record, 10).expect("publish 1");
        let second = group.publish_record(&record, 20).expect("publish 2");
        assert_eq!(second.flushed_object_keys.len(), 1);
        assert!(second.flushed_object_keys[0].contains("/batch-node-a-"));

        group.publish_record(&record, 30).expect("publish 3 buffered");
        let third_flush = group.flush_now(40).expect("flush final").expect("flushed final");
        assert!(third_flush.contains("/chunk-node-a-") || third_flush.contains("/batch-node-a-"));

        let keys = object_store.list_prefix("wal/tenant-a/fp/").expect("list keys");
        assert_eq!(keys.len(), 2);

        let replayed = group.replay_records().expect("replay");
        assert_eq!(replayed.len(), 3);
    }

    #[test]
    fn group_commit_restart_without_flush_does_not_claim_durable_publish() {
        let object_store = InMemoryObjectStore::default();
        let policy = WalBatchPolicy { flush_interval_ms: 500, max_records: 32, max_bytes: 8192 };

        let mut first =
            WalGroupCommitStore::new("wal/tenant-a/fp", "node-a", object_store.clone(), policy);
        let buffered = first
            .publish_record(
                &WalRecord { key: b"k1".to_vec(), meta: b"m1".to_vec(), payload: b"p1".to_vec() },
                100,
            )
            .expect("buffered publish");
        assert!(buffered.flushed_object_keys.is_empty());
        drop(first);

        let restarted = WalStore::with_node_id("wal/tenant-a/fp", "node-a", object_store.clone());
        let replayed = restarted.replay_records().expect("replay after unflushed crash");
        assert!(replayed.is_empty());

        let mut second =
            WalGroupCommitStore::new("wal/tenant-a/fp", "node-a", object_store.clone(), policy);
        second
            .publish_record(
                &WalRecord { key: b"k2".to_vec(), meta: b"m2".to_vec(), payload: b"p2".to_vec() },
                200,
            )
            .expect("buffer second");
        second.flush_now(300).expect("flush now").expect("flushed");

        let replayed_after_flush =
            WalStore::with_node_id("wal/tenant-a/fp", "node-a", object_store)
                .replay_records()
                .expect("replay after flush");
        assert_eq!(replayed_after_flush.len(), 1);
        assert_eq!(replayed_after_flush[0].payload, b"p2".to_vec());
    }

    #[test]
    fn group_commit_single_record_fallback_uses_chunk_objects() {
        let object_store = InMemoryObjectStore::default();
        let policy = WalBatchPolicy::single_record_fallback();
        let mut group =
            WalGroupCommitStore::new("wal/tenant-a/fp", "node-a", object_store.clone(), policy);

        let first = group
            .publish_record(
                &WalRecord { key: b"k1".to_vec(), meta: b"m1".to_vec(), payload: b"p1".to_vec() },
                100,
            )
            .expect("publish first");
        let second = group
            .publish_record(
                &WalRecord { key: b"k2".to_vec(), meta: b"m2".to_vec(), payload: b"p2".to_vec() },
                101,
            )
            .expect("publish second");
        assert_eq!(first.flushed_object_keys.len(), 1);
        assert_eq!(second.flushed_object_keys.len(), 1);
        assert!(first.flushed_object_keys[0].contains("/chunk-node-a-"));
        assert!(second.flushed_object_keys[0].contains("/chunk-node-a-"));

        let keys = object_store.list_prefix("wal/tenant-a/fp/").expect("list keys");
        assert_eq!(keys.len(), 2);
        assert!(keys.iter().all(|key| key.contains("/chunk-node-a-")));
    }

    #[test]
    fn replay_decodes_batch_and_single_objects_in_sequence_order() {
        let object_store = InMemoryObjectStore::default();
        let policy = WalBatchPolicy { flush_interval_ms: 1_000, max_records: 4, max_bytes: 8192 };
        let mut group =
            WalGroupCommitStore::new("wal/tenant-a/fp", "node-a", object_store.clone(), policy);

        group
            .publish_record(
                &WalRecord { key: b"k1".to_vec(), meta: b"m1".to_vec(), payload: b"p1".to_vec() },
                1,
            )
            .expect("publish 1");
        group
            .publish_record(
                &WalRecord { key: b"k2".to_vec(), meta: b"m2".to_vec(), payload: b"p2".to_vec() },
                2,
            )
            .expect("publish 2");
        let batch_key = group.flush_now(3).expect("flush batch").expect("batch key");
        let batch_payload = object_store.get_object(&batch_key).expect("batch payload");
        let decoded_batch = decode_wal_object_records(&batch_payload).expect("decode batch");
        assert_eq!(decoded_batch.len(), 2);

        let mut single = WalStore::with_node_id("wal/tenant-a/fp", "node-a", object_store.clone());
        single
            .publish_record(&WalRecord {
                key: b"k3".to_vec(),
                meta: b"m3".to_vec(),
                payload: b"p3".to_vec(),
            })
            .expect("publish single");

        let replayed = single.replay_records().expect("replay");
        assert_eq!(replayed.len(), 3);
        assert_eq!(replayed[0].payload, b"p1".to_vec());
        assert_eq!(replayed[1].payload, b"p2".to_vec());
        assert_eq!(replayed[2].payload, b"p3".to_vec());
    }

    #[test]
    fn retry_logic_retries_transient_failures_before_success() {
        let attempts = AtomicU32::new(0);
        let result = retry_with_backoff(2, 0, &mut || {
            let attempt = attempts.fetch_add(1, Ordering::SeqCst);
            if attempt < 2 {
                return Err(WalStoreError::S3("transient".to_string()));
            }
            Ok(42_u8)
        })
        .expect("eventually succeeds");

        assert_eq!(result, 42);
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
    }

    fn run_live_minio_tests() -> bool {
        matches!(std::env::var("WMBT_KV_TEST_MINIO_INTEGRATION"), Ok(v) if v == "1")
    }

    fn live_s3_store() -> S3ObjectStore {
        let cfg = S3ObjectStoreConfig::from_env().expect("env config");
        let store = S3ObjectStore::new(cfg).expect("s3 store init");
        store.ensure_bucket().expect("ensure bucket");
        store
    }

    #[test]
    fn s3_minio_object_store_round_trip_live() {
        if !run_live_minio_tests() {
            return;
        }

        let store = live_s3_store();
        let key = "object-store/live/object-store-roundtrip.bin";
        let key_cas = "object-store/live/object-store-cas.bin";
        let key_conditional = "object-store/live/object-store-conditional.bin";
        let _ = store.delete_object(key_conditional);

        store.put_object(key, b"abcdef").expect("put");
        assert!(store.head_object(key).expect("head"));
        assert_eq!(store.get_object(key).expect("get"), b"abcdef".to_vec());
        assert_eq!(store.get_object_range(key, 1, 4).expect("range"), b"bcd".to_vec());

        let list = store.list_prefix("object-store/live/").expect("list");
        assert!(list.iter().any(|candidate| candidate.ends_with("object-store-roundtrip.bin")));

        assert!(store.compare_and_swap_object(key_cas, None, b"snap-1").expect("cas create"));
        assert!(!store
            .compare_and_swap_object(key_cas, Some(b"stale"), b"snap-2")
            .expect("cas stale"));
        assert!(store
            .compare_and_swap_object(key_cas, Some(b"snap-1"), b"snap-2")
            .expect("cas match"));

        let capabilities = store.capabilities();
        match store.put_if_absent(key_conditional, b"cond-1").expect("conditional create") {
            ConditionalPutResult::Success => {
                assert!(capabilities.conditional_create);
                let second = store
                    .put_if_absent(key_conditional, b"cond-2")
                    .expect("conditional create conflict");
                assert!(matches!(second, ConditionalPutResult::Conflict { .. }));
            }
            ConditionalPutResult::Unsupported => {
                assert!(!capabilities.conditional_create);
            }
            ConditionalPutResult::Conflict { .. } => {
                panic!("unexpected create conflict for new key");
            }
        }

        if let Some((_, token)) = store.read_with_token(key_conditional).expect("read token") {
            match store
                .put_if_token_matches(key_conditional, b"cond-3", &token)
                .expect("conditional update")
            {
                ConditionalPutResult::Success => assert!(capabilities.conditional_update),
                ConditionalPutResult::Unsupported => assert!(!capabilities.conditional_update),
                ConditionalPutResult::Conflict { .. } => {
                    panic!("unexpected update conflict for fresh token")
                }
            }
        }

        assert!(store.delete_object(key).expect("delete"));
        assert!(matches!(store.get_object(key), Err(WalStoreError::ObjectNotFound(_))));

        let _ = store.delete_object(key_cas);
        let _ = store.delete_object(key_conditional);
    }

    #[test]
    fn wal_publish_replay_with_s3_minio_live() {
        if !run_live_minio_tests() {
            return;
        }

        let store = live_s3_store();
        let namespace = "object-store/live/wal/tenant-a/fp";

        for key in store.list_prefix(namespace).expect("list precleanup") {
            let _ = store.delete_object(&key);
        }

        let mut wal = WalStore::with_node_id(namespace, "node-it", store.clone());
        wal.publish_record(&WalRecord {
            key: b"k1".to_vec(),
            meta: b"m1".to_vec(),
            payload: b"p1".to_vec(),
        })
        .expect("publish 1");
        wal.publish_record(&WalRecord {
            key: b"k2".to_vec(),
            meta: b"m2".to_vec(),
            payload: b"p2".to_vec(),
        })
        .expect("publish 2");

        let replay = wal.replay_records().expect("replay");
        assert_eq!(replay.len(), 2);
        assert_eq!(replay[0].payload, b"p1".to_vec());
        assert_eq!(replay[1].payload, b"p2".to_vec());
    }
}
