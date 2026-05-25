//! Optional in-binary TLS termination for the `serve-catalog` role
//! (`catalog-tls` feature). Quack's server never speaks TLS itself and a
//! non-local client assumes HTTPS, so on platforms without managed TLS (e.g. ECS
//! behind Cloud Map) the catalog needs a TLS front. This terminates TLS with an
//! ephemeral self-signed cert and forwards the plaintext to the local Quack
//! server. Clients reach it over HTTPS and skip cert verification with a scoped
//! `(TYPE HTTP, VERIFY_SSL 0)` secret (see `CANARDSTACK_DUCKLAKE_QUACK_INSECURE_TLS`);
//! the Quack token does authentication and TLS provides encryption in transit.

use anyhow::{Context, Result};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use rustls::{ServerConfig, ServerConnection, StreamOwned};
use std::io::{ErrorKind, Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

const ACCEPT_POLL_INTERVAL: Duration = Duration::from_millis(50);
const PUMP_IDLE_SLEEP: Duration = Duration::from_millis(1);
const BUF_BYTES: usize = 32 * 1024;

/// rustls config with a freshly generated self-signed cert. The cert identity is
/// not verified by clients (they use `VERIFY_SSL 0`), so it exists only to
/// establish the encrypted channel; nothing sensitive is baked into the image.
fn self_signed_config() -> Result<Arc<ServerConfig>> {
    let cert = rcgen::generate_simple_self_signed(vec!["canardstack-catalog".to_string()])
        .context("generate self-signed catalog certificate")?;
    let cert_der = CertificateDer::from(cert.cert.der().to_vec());
    let key_der = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(cert.key_pair.serialize_der()));
    let mut config =
        ServerConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
            .with_safe_default_protocol_versions()
            .context("configure catalog TLS protocol versions")?
            .with_no_client_auth()
            .with_single_cert(vec![cert_der], key_der)
            .context("build catalog TLS server config")?;
    // The plaintext Quack backend speaks HTTP/1.1, so advertise only http/1.1 to
    // the client and forward the decrypted HTTP/1.1 bytes through unchanged.
    config.alpn_protocols = vec![b"http/1.1".to_vec()];
    Ok(Arc::new(config))
}

/// Terminate TLS on `public_addr` and forward decrypted bytes to the local
/// plaintext Quack server at `backend_addr`. Blocks until `shutdown`.
pub fn run_tls_terminator(
    public_addr: &str,
    backend_addr: String,
    shutdown: &'static AtomicBool,
) -> Result<()> {
    let config = self_signed_config()?;
    let listener = TcpListener::bind(public_addr)
        .with_context(|| format!("bind catalog TLS listener {public_addr}"))?;
    listener.set_nonblocking(true)?;
    tracing::info!(
        event = "catalog_tls_listening",
        addr = %public_addr,
        backend = %backend_addr,
        "terminating TLS in front of the Quack catalog"
    );
    while !shutdown.load(Ordering::SeqCst) {
        match listener.accept() {
            Ok((stream, _)) => {
                let config = config.clone();
                let backend = backend_addr.clone();
                thread::spawn(move || {
                    if let Err(err) = proxy_connection(stream, config, &backend) {
                        tracing::debug!(event = "catalog_tls_conn_failed", error = %err);
                    }
                });
            }
            Err(err) if err.kind() == ErrorKind::WouldBlock => thread::sleep(ACCEPT_POLL_INTERVAL),
            Err(err) if err.kind() == ErrorKind::Interrupted => continue,
            Err(err) => return Err(err.into()),
        }
    }
    Ok(())
}

/// Single-threaded non-blocking bidirectional pump for one connection: decrypt
/// client->backend and encrypt backend->client. Quack is request/response over
/// HTTP/1.1, so one pump thread per connection is sufficient.
fn proxy_connection(client: TcpStream, config: Arc<ServerConfig>, backend: &str) -> Result<()> {
    client.set_nonblocking(true)?;
    let conn = ServerConnection::new(config)?;
    let mut tls = StreamOwned::new(conn, client);

    let backend = TcpStream::connect(backend).context("connect to local Quack backend")?;
    backend.set_nonblocking(true)?;
    let mut backend = backend;

    let mut c2b = Pending::default();
    let mut b2c = Pending::default();
    let mut client_eof = false;
    let mut backend_eof = false;
    let mut backend_wr_closed = false;
    let mut client_wr_closed = false;
    let mut scratch = vec![0u8; BUF_BYTES];

    loop {
        let mut progress = false;

        // client (TLS) -> backend
        if c2b.is_empty() && !client_eof {
            match read_into(&mut tls, &mut scratch) {
                ReadState::Data(n) => {
                    c2b.set(&scratch[..n]);
                    progress = true;
                }
                ReadState::Eof => client_eof = true,
                ReadState::WouldBlock => {}
                ReadState::Err(e) if peer_closed(&e) => client_eof = true,
                ReadState::Err(e) => return Err(e.into()),
            }
        }
        if c2b.has_data() {
            match write_some(&mut backend, c2b.unwritten()) {
                WriteState::Wrote(n) => {
                    c2b.advance(n);
                    progress = true;
                }
                WriteState::WouldBlock => {}
                WriteState::Err(e) if peer_closed(&e) => backend_eof = true,
                WriteState::Err(e) => return Err(e.into()),
            }
        }
        // Propagate the client's half-close: once the full request is forwarded,
        // half-close the backend write side so Quack stops waiting for more input
        // and produces its response (the request/response is request-then-FIN).
        if client_eof && c2b.is_empty() && !backend_wr_closed {
            let _ = backend.shutdown(Shutdown::Write);
            backend_wr_closed = true;
        }

        // backend -> client (TLS)
        if b2c.is_empty() && !backend_eof {
            match read_into(&mut backend, &mut scratch) {
                ReadState::Data(n) => {
                    b2c.set(&scratch[..n]);
                    progress = true;
                }
                ReadState::Eof => backend_eof = true,
                ReadState::WouldBlock => {}
                ReadState::Err(e) if peer_closed(&e) => backend_eof = true,
                ReadState::Err(e) => return Err(e.into()),
            }
        }
        if b2c.has_data() {
            // rustls buffers the plaintext and only best-effort-writes to the
            // socket, so the response must be flushed to completion or the client
            // sees a truncated read.
            match write_some(&mut tls, b2c.unwritten()) {
                WriteState::Wrote(n) => {
                    b2c.advance(n);
                    flush_spin(&mut tls)?;
                    progress = true;
                }
                WriteState::WouldBlock => flush_spin(&mut tls)?,
                WriteState::Err(e) if peer_closed(&e) => client_eof = true,
                WriteState::Err(e) => return Err(e.into()),
            }
        }
        // Propagate the backend's half-close: once the full response is delivered,
        // send the TLS close_notify so the client sees a clean end of response.
        if backend_eof && b2c.is_empty() && !client_wr_closed {
            tls.conn.send_close_notify();
            let _ = flush_spin(&mut tls);
            client_wr_closed = true;
        }

        // Exit only once BOTH directions have closed and drained, so a client that
        // half-closes after its request still receives the full response.
        if client_eof && backend_eof && c2b.is_empty() && b2c.is_empty() {
            break;
        }
        if !progress {
            thread::sleep(PUMP_IDLE_SLEEP);
        }
    }
    Ok(())
}

fn peer_closed(e: &std::io::Error) -> bool {
    matches!(
        e.kind(),
        ErrorKind::BrokenPipe
            | ErrorKind::ConnectionReset
            | ErrorKind::ConnectionAborted
            | ErrorKind::UnexpectedEof
    )
}

/// Flush rustls's buffered output to the non-blocking socket, retrying on
/// WouldBlock so the client receives the complete response before close.
fn flush_spin<S: Write>(tls: &mut S) -> std::io::Result<()> {
    loop {
        match tls.flush() {
            Ok(()) => return Ok(()),
            Err(e) if e.kind() == ErrorKind::WouldBlock || e.kind() == ErrorKind::Interrupted => {
                thread::sleep(PUMP_IDLE_SLEEP);
            }
            Err(e) => return Err(e),
        }
    }
}

#[derive(Default)]
struct Pending {
    buf: Vec<u8>,
    start: usize,
}

impl Pending {
    fn is_empty(&self) -> bool {
        self.start >= self.buf.len()
    }
    fn has_data(&self) -> bool {
        !self.is_empty()
    }
    fn set(&mut self, data: &[u8]) {
        self.buf.clear();
        self.buf.extend_from_slice(data);
        self.start = 0;
    }
    fn unwritten(&self) -> &[u8] {
        &self.buf[self.start..]
    }
    fn advance(&mut self, n: usize) {
        self.start += n;
        if self.is_empty() {
            self.buf.clear();
            self.start = 0;
        }
    }
}

enum ReadState {
    Data(usize),
    Eof,
    WouldBlock,
    Err(std::io::Error),
}

fn read_into<R: Read>(reader: &mut R, scratch: &mut [u8]) -> ReadState {
    match reader.read(scratch) {
        Ok(0) => ReadState::Eof,
        Ok(n) => ReadState::Data(n),
        Err(e) if e.kind() == ErrorKind::WouldBlock => ReadState::WouldBlock,
        Err(e) if e.kind() == ErrorKind::Interrupted => ReadState::WouldBlock,
        Err(e) => ReadState::Err(e),
    }
}

enum WriteState {
    Wrote(usize),
    WouldBlock,
    Err(std::io::Error),
}

fn write_some<W: Write>(writer: &mut W, data: &[u8]) -> WriteState {
    match writer.write(data) {
        Ok(n) => WriteState::Wrote(n),
        Err(e) if e.kind() == ErrorKind::WouldBlock => WriteState::WouldBlock,
        Err(e) if e.kind() == ErrorKind::Interrupted => WriteState::WouldBlock,
        Err(e) => WriteState::Err(e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::Command;

    // Large POST: the backend reads the full request body, verifies every byte,
    // and echoes the count. Catches request-direction truncation/corruption.
    #[test]
    #[ignore = "needs curl; exercises the live TLS terminator"]
    fn tls_terminator_forwards_large_request() {
        const REQ_LEN: usize = 120_000;
        let backend = TcpListener::bind("127.0.0.1:0").unwrap();
        let backend_addr = backend.local_addr().unwrap().to_string();
        thread::spawn(move || {
            for stream in backend.incoming() {
                let mut s = stream.unwrap();
                let mut data = Vec::new();
                let mut tmp = [0u8; 8192];
                // Read headers + body until we have the full Content-Length body.
                let mut content_len: Option<usize> = None;
                let mut header_end: Option<usize> = None;
                loop {
                    if let (Some(he), Some(cl)) = (header_end, content_len) {
                        if data.len() >= he + cl {
                            break;
                        }
                    }
                    let n = match s.read(&mut tmp) {
                        Ok(0) => break,
                        Ok(n) => n,
                        Err(_) => break,
                    };
                    data.extend_from_slice(&tmp[..n]);
                    if header_end.is_none() {
                        if let Some(p) = data.windows(4).position(|w| w == b"\r\n\r\n") {
                            header_end = Some(p + 4);
                            let head = String::from_utf8_lossy(&data[..p]).to_lowercase();
                            for line in head.lines() {
                                if let Some(v) = line.strip_prefix("content-length:") {
                                    content_len = v.trim().parse().ok();
                                }
                            }
                        }
                    }
                }
                let body = &data[header_end.unwrap_or(data.len())..];
                let ok = body.len() == REQ_LEN && body.iter().all(|&b| b == b'x');
                let msg = if ok {
                    format!("OK {}", body.len())
                } else {
                    format!("BAD {}", body.len())
                };
                let _ = s.write_all(
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                        msg.len(),
                        msg
                    )
                    .as_bytes(),
                );
                let _ = s.flush();
            }
        });
        let publ = TcpListener::bind("127.0.0.1:0").unwrap();
        let public_addr = publ.local_addr().unwrap().to_string();
        drop(publ);
        let shutdown: &'static AtomicBool = Box::leak(Box::new(AtomicBool::new(false)));
        let pa = public_addr.clone();
        thread::spawn(move || {
            let _ = run_tls_terminator(&pa, backend_addr, shutdown);
        });
        thread::sleep(Duration::from_millis(400));
        let out = Command::new("curl")
            .args([
                "-sk",
                "--max-time",
                "8",
                "-X",
                "POST",
                "--data-binary",
                &"x".repeat(REQ_LEN),
                &format!("https://{public_addr}/"),
            ])
            .output()
            .unwrap();
        shutdown.store(true, Ordering::SeqCst);
        let body = String::from_utf8_lossy(&out.stdout);
        assert_eq!(
            body,
            format!("OK {REQ_LEN}"),
            "curl exit={:?} stderr={}",
            out.status.code(),
            String::from_utf8_lossy(&out.stderr)
        );
    }

    // Isolates the TLS terminator from Quack: a trivial HTTP/1.1 backend behind
    // the shim, hit with curl -k. Needs curl; run with --ignored.
    #[test]
    #[ignore = "needs curl; exercises the live TLS terminator"]
    fn tls_terminator_forwards_http_roundtrip() {
        const BODY_LEN: usize = 250_000; // larger than the 32k pump buffer
        let backend = TcpListener::bind("127.0.0.1:0").unwrap();
        let backend_addr = backend.local_addr().unwrap().to_string();
        thread::spawn(move || {
            for stream in backend.incoming() {
                let mut s = stream.unwrap();
                let mut buf = [0u8; 8192];
                let _ = s.read(&mut buf);
                let _ = s.write_all(
                    format!("HTTP/1.1 200 OK\r\nContent-Length: {BODY_LEN}\r\nConnection: close\r\n\r\n")
                        .as_bytes(),
                );
                let _ = s.write_all(&vec![b'x'; BODY_LEN]);
                let _ = s.flush();
            }
        });
        let publ = TcpListener::bind("127.0.0.1:0").unwrap();
        let public_addr = publ.local_addr().unwrap().to_string();
        drop(publ);
        let shutdown: &'static AtomicBool = Box::leak(Box::new(AtomicBool::new(false)));
        let pa = public_addr.clone();
        thread::spawn(move || {
            let _ = run_tls_terminator(&pa, backend_addr, shutdown);
        });
        thread::sleep(Duration::from_millis(400));
        let out = Command::new("curl")
            .args(["-sk", "--max-time", "5", &format!("https://{public_addr}/")])
            .output()
            .unwrap();
        shutdown.store(true, Ordering::SeqCst);
        assert_eq!(
            out.stdout.len(),
            BODY_LEN,
            "expected {BODY_LEN} bytes, got {}; curl exit={:?} stderr={}",
            out.stdout.len(),
            out.status.code(),
            String::from_utf8_lossy(&out.stderr)
        );
    }
}
