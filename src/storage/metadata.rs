use super::Storage;
use std::sync::atomic::Ordering;

impl Storage {
    pub fn metadata_generation(&self) -> u64 {
        self.metadata_generation.load(Ordering::SeqCst)
    }
}
