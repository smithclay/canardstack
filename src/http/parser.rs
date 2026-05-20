use std::collections::HashMap;
use std::io::{BufRead, BufReader};
use std::net::TcpStream;

pub(super) const MAX_REQUEST_LINE_BYTES: usize = 8 * 1024;
pub(super) const MAX_HEADER_LINE_BYTES: usize = 8 * 1024;
pub(super) const MAX_HEADER_BYTES: usize = 64 * 1024;
pub(super) const MAX_HEADER_COUNT: usize = 100;

pub(super) enum LineRead {
    Read(usize),
    TooLong,
}

pub(super) fn read_bounded_line(
    reader: &mut BufReader<TcpStream>,
    line: &mut String,
    max_bytes: usize,
) -> anyhow::Result<LineRead> {
    let mut out = Vec::new();
    let mut total = 0usize;
    loop {
        let available = reader.fill_buf()?;
        if available.is_empty() {
            break;
        }
        let take = available
            .iter()
            .position(|b| *b == b'\n')
            .map(|idx| idx + 1)
            .unwrap_or(available.len());
        total += take;
        if total > max_bytes {
            return Ok(LineRead::TooLong);
        }
        out.extend_from_slice(&available[..take]);
        reader.consume(take);
        if out.ends_with(b"\n") {
            break;
        }
    }
    *line = String::from_utf8_lossy(&out).to_string();
    Ok(LineRead::Read(total))
}

pub(super) fn split_target(target: &str) -> (String, HashMap<String, String>) {
    let (path, raw_query) = target.split_once('?').unwrap_or((target, ""));
    let mut query = HashMap::new();
    for pair in raw_query.split('&').filter(|s| !s.is_empty()) {
        let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
        query.insert(percent_decode(k), percent_decode(v));
    }
    (path.to_string(), query)
}

pub(super) fn percent_decode(input: &str) -> String {
    let bytes = input.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let Some(hex) = hex_byte(bytes[i + 1], bytes[i + 2]) {
                out.push(hex);
                i += 3;
                continue;
            }
        }
        out.push(if bytes[i] == b'+' { b' ' } else { bytes[i] });
        i += 1;
    }
    String::from_utf8_lossy(&out).to_string()
}

fn hex_byte(high: u8, low: u8) -> Option<u8> {
    Some(hex_nibble(high)? << 4 | hex_nibble(low)?)
}

fn hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}
