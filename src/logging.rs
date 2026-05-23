use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use tracing::field::{Field, Visit};
use tracing::level_filters::LevelFilter;
use tracing::{Event, Id, Level, Metadata, Subscriber};

static NEXT_SPAN_ID: AtomicU64 = AtomicU64::new(1);

pub fn init_logging() {
    let subscriber = LogfmtSubscriber {
        max_level: configured_level(),
    };
    let _ = tracing::subscriber::set_global_default(subscriber);
}

fn configured_level() -> LevelFilter {
    std::env::var("CANARDSTACK_LOG")
        .or_else(|_| std::env::var("RUST_LOG"))
        .ok()
        .and_then(|value| parse_level(&value))
        .unwrap_or(LevelFilter::INFO)
}

fn parse_level(value: &str) -> Option<LevelFilter> {
    let value = value
        .split(',')
        .find_map(|directive| {
            directive
                .rsplit_once('=')
                .map_or(Some(directive), |(_, level)| Some(level))
        })?
        .trim()
        .to_ascii_lowercase();
    match value.as_str() {
        "off" => Some(LevelFilter::OFF),
        "error" => Some(LevelFilter::ERROR),
        "warn" | "warning" => Some(LevelFilter::WARN),
        "info" => Some(LevelFilter::INFO),
        "debug" => Some(LevelFilter::DEBUG),
        "trace" => Some(LevelFilter::TRACE),
        _ => None,
    }
}

struct LogfmtSubscriber {
    max_level: LevelFilter,
}

impl Subscriber for LogfmtSubscriber {
    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        level_enabled(self.max_level, metadata.level())
    }

    fn new_span(&self, _span: &tracing::span::Attributes<'_>) -> Id {
        Id::from_u64(NEXT_SPAN_ID.fetch_add(1, Ordering::Relaxed))
    }

    fn record(&self, _span: &Id, _values: &tracing::span::Record<'_>) {}

    fn record_follows_from(&self, _span: &Id, _follows: &Id) {}

    fn event(&self, event: &Event<'_>) {
        if !self.enabled(event.metadata()) {
            return;
        }
        let mut fields = LogfmtFields::default();
        event.record(&mut fields);
        eprintln!("level={} {}", level_name(event.metadata().level()), fields);
    }

    fn enter(&self, _span: &Id) {}

    fn exit(&self, _span: &Id) {}
}

#[derive(Default)]
struct LogfmtFields {
    values: Vec<(&'static str, String)>,
}

impl Visit for LogfmtFields {
    fn record_bool(&mut self, field: &Field, value: bool) {
        self.push(field, value.to_string());
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        self.push(field, value.to_string());
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        self.push(field, value.to_string());
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        self.push(field, value.to_string());
    }

    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        self.push(field, format!("{value:?}"));
    }
}

impl LogfmtFields {
    fn push(&mut self, field: &Field, value: String) {
        self.values.push((field.name(), value));
    }
}

impl fmt::Display for LogfmtFields {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (idx, (key, value)) in self.values.iter().enumerate() {
            if idx > 0 {
                write!(f, " ")?;
            }
            write!(f, "{key}={}", LogfmtValue(value))?;
        }
        Ok(())
    }
}

struct LogfmtValue<'a>(&'a str);

impl fmt::Display for LogfmtValue<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.0.is_empty()
            || self
                .0
                .chars()
                .any(|c| c.is_whitespace() || c == '=' || c == '"')
        {
            write!(f, "\"")?;
            for c in self.0.chars() {
                match c {
                    '\\' => write!(f, "\\\\")?,
                    '"' => write!(f, "\\\"")?,
                    _ => write!(f, "{c}")?,
                }
            }
            write!(f, "\"")
        } else {
            write!(f, "{}", self.0)
        }
    }
}

fn level_name(level: &Level) -> &'static str {
    match *level {
        Level::ERROR => "error",
        Level::WARN => "warn",
        Level::INFO => "info",
        Level::DEBUG => "debug",
        Level::TRACE => "trace",
    }
}

fn level_enabled(max_level: LevelFilter, level: &Level) -> bool {
    match max_level {
        LevelFilter::OFF => false,
        LevelFilter::ERROR => matches!(*level, Level::ERROR),
        LevelFilter::WARN => matches!(*level, Level::ERROR | Level::WARN),
        LevelFilter::INFO => matches!(*level, Level::ERROR | Level::WARN | Level::INFO),
        LevelFilter::DEBUG => {
            matches!(
                *level,
                Level::ERROR | Level::WARN | Level::INFO | Level::DEBUG
            )
        }
        LevelFilter::TRACE => true,
    }
}

#[cfg(test)]
mod tests {
    use super::{parse_level, LogfmtValue};
    use std::fmt::Write;
    use tracing::level_filters::LevelFilter;

    #[test]
    fn parses_rust_log_style_level_directive() {
        assert_eq!(parse_level("warn"), Some(LevelFilter::WARN));
        assert_eq!(parse_level("canardstack=debug"), Some(LevelFilter::DEBUG));
        assert_eq!(parse_level("info,duckdb=warn"), Some(LevelFilter::INFO));
    }

    #[test]
    fn quotes_logfmt_values_when_needed() {
        let mut out = String::new();
        write!(&mut out, "{}", LogfmtValue("a value=\"x\"")).unwrap();
        assert_eq!(out, "\"a value=\\\"x\\\"\"");
    }

    #[test]
    fn level_filter_enables_more_severe_events() {
        assert!(super::level_enabled(
            LevelFilter::WARN,
            &tracing::Level::ERROR
        ));
        assert!(super::level_enabled(
            LevelFilter::WARN,
            &tracing::Level::WARN
        ));
        assert!(!super::level_enabled(
            LevelFilter::WARN,
            &tracing::Level::INFO
        ));
    }
}
