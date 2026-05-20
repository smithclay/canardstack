mod auth;
mod compat_routes;
mod parser;
mod response;
mod router;
mod server;

pub use response::HttpResponse;
pub(crate) use router::record_operator_gauges;
pub use router::route;
pub use server::{serve, serve_until};
