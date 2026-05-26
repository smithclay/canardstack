use crate::validation::ApiError;
use tonic::{Code, Status};

pub(crate) fn from_api_error(err: ApiError) -> Status {
    let message = format!("{}: {}", err.reason, err.message);
    match err.status {
        400 => Status::new(Code::InvalidArgument, message),
        401 | 403 => Status::new(Code::Unauthenticated, message),
        429 => Status::new(Code::ResourceExhausted, message),
        503 => Status::new(Code::Unavailable, message),
        500..=599 => Status::new(Code::Internal, message),
        _ => Status::new(Code::Unknown, message),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn api_error_status_codes_map_to_grpc_codes() {
        let cases = [
            (
                ApiError::new(400, "invalid_payload", "bad protobuf"),
                Code::InvalidArgument,
            ),
            (
                ApiError::new(401, "missing_api_key", "missing key"),
                Code::Unauthenticated,
            ),
            (
                ApiError::new(403, "bad_api_key", "bad key"),
                Code::Unauthenticated,
            ),
            (
                ApiError::new(429, "freshness_budget_exceeded", "under pressure"),
                Code::ResourceExhausted,
            ),
            (
                ApiError::new(503, "dependency_unhealthy", "storage unavailable"),
                Code::Unavailable,
            ),
            (ApiError::new(500, "internal", "failed"), Code::Internal),
        ];

        for (err, code) in cases {
            assert_eq!(from_api_error(err).code(), code);
        }
    }
}
