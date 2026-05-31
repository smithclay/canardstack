use crate::admission_control::AdmissionController;
use crate::config::Config;
use crate::metadata::Metadata;
use crate::metrics::Metrics;
use crate::query::QueryEngine;
use crate::storage::Storage;
use anyhow::Result;
use std::sync::Arc;

pub struct AppState {
    pub config: Config,
    pub storage: Arc<Storage>,
    pub admission: AdmissionController,
    pub queries: QueryEngine,
    pub metadata: Metadata,
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
        let admission = AdmissionController::new(&config);
        let metrics = Arc::new(Metrics::default());
        Ok(Self {
            storage,
            admission,
            queries: QueryEngine::new(&config),
            metadata: Metadata::new(),
            metrics,
            config,
        })
    }
}
