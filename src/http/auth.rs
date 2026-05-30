use crate::validation::{self, ApiError};
use crate::AppState;
use std::collections::HashMap;

pub(super) fn api_auth<T>(
    headers: &HashMap<String, String>,
    state: &AppState,
    f: impl FnOnce() -> Result<T, ApiError>,
) -> Result<T, ApiError> {
    validation::validate_api_key(headers, &state.config, false)?;
    f()
}
