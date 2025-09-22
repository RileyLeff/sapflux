Title: Fix Tokio runtime-inside-runtime panic by making ObjectStore/S3 initialization async

Summary

Symptom: API container exits on startup with “Cannot start a runtime from within a runtime.”
Root cause: S3Store::from_env() uses Handle::try_current().block_on inside ObjectStore::from_env(), which is being called after Axum’s runtime has started.
Fix: Introduce async initializers:
ObjectStore::from_env_async() and S3Store::from_env_async(), both async
Update API and admin CLI to await from_env_async() during startup
Update the S3 smoke test to use the async initializer
Changes to make

crates/sapflux-core/src/object_store.rs
Add an async constructor: pub async fn from_env_async() -> Result<Self>
Keep the existing sync from_env() for non-runtime builds and local/noop/localdir only, but make it error on "s3" to prevent accidental sync S3 init inside a runtime
Replace S3Store::from_env() (which uses Handle::block_on) with pub async fn from_env_async()
What to change:

In ObjectStore::from_env():
Leave noop/local support as-is
For "s3", return an error that instructs callers to use from_env_async().await
Add ObjectStore::from_env_async() (cfg(feature="runtime")):
Decide kind from env (noop/local/s3)
For local, reuse LocalDirStore::new
For s3, call S3Store::from_env_async().await
Rename S3Store::from_env() to S3Store::from_env_async() (cfg(feature="runtime")):
Load S3Config::from_env()
Call Self::from_config(config).await
Delete the Handle::try_current().block_on usage entirely
2. crates/sapflux/src/main.rs

In run_server(), replace ObjectStore::from_env()? with ObjectStore::from_env_async().await?
3. crates/sapflux-admin/src/main.rs

In handle_object_store_gc(), replace ObjectStore::from_env()? with ObjectStore::from_env_async().await?
4. crates/sapflux/tests/object_store_s3.rs

Replace ObjectStore::from_env()? with ObjectStore::from_env_async().await?
Optional compose note (ARM hosts)

The platform mismatch warning is benign for multi-arch images, but you can add platform: linux/arm64/v8 under each service on ARM hosts to quiet it. Not required for functionality.
Acceptance checklist

docker compose up --build starts without the runtime panic
POST /admin/migrate and /admin/seed succeed
scripts/smoke.sh completes (multipart transaction accepted, artifacts published, download endpoint presigns and fetches parquet)
S3 smoke test passes when env vars are provided (SAPFLUX_TEST_*)
Concrete edits (minimal)

File: crates/sapflux-core/src/object_store.rs

Add:
#[cfg(feature = "runtime")] pub async fn from_env_async() -> Result<Self>
#[cfg(feature = "runtime")] impl S3Store { pub async fn from_env_async() -> Result<Self> { let cfg = S3Config::from_env()?; Self::from_config(cfg).await } }
Change:
In ObjectStore::from_env(), for "s3" branch (when feature=runtime), return Err(anyhow!("...use ObjectStore::from_env_async().await"))
Remove Handle::try_current().block_on(...) usage entirely
File: crates/sapflux/src/main.rs

In run_server():
let store = ObjectStore::from_env_async().await.context("failed to configure object store")?;
File: crates/sapflux-admin/src/main.rs

In handle_object_store_gc():
let store = ObjectStore::from_env_async().await.context("failed to configure object store")?;
File: crates/sapflux/tests/object_store_s3.rs

In the test:
let store = ObjectStore::from_env_async().await.context("failed to build S3 object store")?;
Why this works

The S3 SDK client building is async and should be awaited inside the already-running Tokio runtime. Removing Handle::block_on prevents the nested-runtime panic. The API and admin CLI both run under #[tokio::main], so awaiting the async initializer is the correct, idiomatic approach.
