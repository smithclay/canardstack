use crate::config::Config;
use crate::ingest::Ingestor;
use crate::lanes::LaneController;
use crate::maintenance::Maintenance;
use crate::metadata::Metadata;
use crate::metrics::Metrics;
use crate::query::QueryEngine;
use crate::storage::Storage;
use anyhow::Result;
use std::sync::Arc;

pub struct AppState {
    pub config: Config,
    pub storage: Arc<Storage>,
    pub ingestor: Arc<Ingestor>,
    pub lanes: LaneController,
    pub queries: QueryEngine,
    pub metadata: Metadata,
    pub maintenance: Maintenance,
    pub metrics: Arc<Metrics>,
}

impl AppState {
    pub fn new(config: Config) -> Result<Self> {
        Self::new_with_storage_hook(config, |_| {})
    }

    #[cfg(debug_assertions)]
    pub fn new_with_storage_hook_for_tests<F>(config: Config, storage_hook: F) -> Result<Self>
    where
        F: FnOnce(&Arc<Storage>),
    {
        Self::new_with_storage_hook(config, storage_hook)
    }

    fn new_with_storage_hook<F>(config: Config, storage_hook: F) -> Result<Self>
    where
        F: FnOnce(&Arc<Storage>),
    {
        let storage = Arc::new(Storage::open(&config)?);
        storage_hook(&storage);
        let ingestor = Arc::new(Ingestor::new(config.clone())?);
        let lanes = LaneController::new(&config);
        let metrics = Arc::new(Metrics::default());
        if config.serve_role.accepts_ingest() {
            ingestor.start_ingest_workers(storage.clone())?;
            let replayed = ingestor.replay_raw_spool(&storage, &lanes, metrics.clone())?;
            if replayed > 0 {
                tracing::info!(event = "raw_spool_replayed", records = replayed);
            }
        }
        Ok(Self {
            storage,
            ingestor,
            lanes,
            queries: QueryEngine::new(&config),
            metadata: Metadata::new(),
            maintenance: Maintenance::new(&config),
            metrics,
            config,
        })
    }
}
