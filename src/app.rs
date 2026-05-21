use crate::config::Config;
use crate::ingest::Ingestor;
use crate::lanes::LaneController;
use crate::maintenance::Maintenance;
use crate::metadata::Metadata;
use crate::metrics::Metrics;
use crate::query::QueryEngine;
use crate::storage::Storage;
use anyhow::Result;

pub struct AppState {
    pub config: Config,
    pub storage: Storage,
    pub ingestor: Ingestor,
    pub lanes: LaneController,
    pub queries: QueryEngine,
    pub metadata: Metadata,
    pub maintenance: Maintenance,
    pub metrics: Metrics,
}

impl AppState {
    pub fn new(config: Config) -> Result<Self> {
        let storage = Storage::open(&config)?;
        let ingestor = Ingestor::new(config.clone())?;
        let lanes = LaneController::new(&config);
        let metrics = Metrics::default();
        if config.serve_role.accepts_ingest() {
            let replayed = ingestor.replay_raw_spool(&storage, &metrics)?;
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
