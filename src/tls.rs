//! Optional in-binary TLS termination for the public `serve` listener. The
//! query server keeps a plaintext HTTP backend; this module terminates TLS on a
//! public address and forwards decrypted HTTP/1.1 bytes to the configured
//! loopback backend.

use anyhow::{Context, Result};
use base64::Engine;
use rustls::pki_types::{
    CertificateDer, PrivateKeyDer, PrivatePkcs1KeyDer, PrivatePkcs8KeyDer, PrivateSec1KeyDer,
};
use rustls::{ServerConfig, ServerConnection, StreamOwned};
use std::fs;
use std::io::{ErrorKind, Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

const ACCEPT_POLL_INTERVAL: Duration = Duration::from_millis(50);
const PUMP_IDLE_SLEEP: Duration = Duration::from_millis(1);
const BUF_BYTES: usize = 32 * 1024;

#[derive(Clone, Debug)]
pub enum TlsIdentity {
    File {
        cert_file: PathBuf,
        key_file: PathBuf,
    },
    EphemeralSelfSigned {
        subject_alt_names: Vec<String>,
    },
}

impl TlsIdentity {
    fn server_config(&self) -> Result<Arc<ServerConfig>> {
        match self {
            Self::File {
                cert_file,
                key_file,
            } => file_config(cert_file, key_file),
            Self::EphemeralSelfSigned { subject_alt_names } => {
                self_signed_config(subject_alt_names)
            }
        }
    }
}

fn file_config(cert_file: &PathBuf, key_file: &PathBuf) -> Result<Arc<ServerConfig>> {
    let cert_bytes =
        fs::read(cert_file).with_context(|| format!("read TLS cert {}", cert_file.display()))?;
    let key_bytes =
        fs::read(key_file).with_context(|| format!("read TLS key {}", key_file.display()))?;
    let certs = pem_blocks(&cert_bytes, &["CERTIFICATE"])
        .with_context(|| format!("parse TLS cert {}", cert_file.display()))?
        .into_iter()
        .map(CertificateDer::from)
        .collect::<Vec<_>>();
    if certs.is_empty() {
        anyhow::bail!(
            "{} must contain at least one PEM CERTIFICATE block",
            cert_file.display()
        );
    }

    let key = first_private_key(&key_bytes)
        .with_context(|| format!("parse TLS key {}", key_file.display()))?;
    build_server_config(certs, key)
}

fn first_private_key(key_bytes: &[u8]) -> Result<PrivateKeyDer<'static>> {
    if let Some(bytes) = pem_blocks(key_bytes, &["PRIVATE KEY"])?.into_iter().next() {
        return Ok(PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(bytes)));
    }
    if let Some(bytes) = pem_blocks(key_bytes, &["RSA PRIVATE KEY"])?
        .into_iter()
        .next()
    {
        return Ok(PrivateKeyDer::Pkcs1(PrivatePkcs1KeyDer::from(bytes)));
    }
    if let Some(bytes) = pem_blocks(key_bytes, &["EC PRIVATE KEY"])?
        .into_iter()
        .next()
    {
        return Ok(PrivateKeyDer::Sec1(PrivateSec1KeyDer::from(bytes)));
    }
    anyhow::bail!(
        "TLS key must contain a PEM PRIVATE KEY, RSA PRIVATE KEY, or EC PRIVATE KEY block"
    );
}

fn pem_blocks(input: &[u8], labels: &[&str]) -> Result<Vec<Vec<u8>>> {
    let text = std::str::from_utf8(input).context("PEM file must be UTF-8 text")?;
    let mut blocks = Vec::new();
    for label in labels {
        let begin = format!("-----BEGIN {label}-----");
        let end = format!("-----END {label}-----");
        let mut rest = text;
        while let Some(begin_idx) = rest.find(&begin) {
            let after_begin = &rest[begin_idx + begin.len()..];
            let Some(end_idx) = after_begin.find(&end) else {
                anyhow::bail!("PEM block {label} is missing its END marker");
            };
            let body = after_begin[..end_idx]
                .chars()
                .filter(|ch| !ch.is_whitespace())
                .collect::<String>();
            let decoded = base64::engine::general_purpose::STANDARD
                .decode(body.as_bytes())
                .with_context(|| format!("decode PEM block {label}"))?;
            blocks.push(decoded);
            rest = &after_begin[end_idx + end.len()..];
        }
    }
    Ok(blocks)
}

fn self_signed_config(subject_alt_names: &[String]) -> Result<Arc<ServerConfig>> {
    let names = if subject_alt_names.is_empty() {
        vec!["localhost".to_string()]
    } else {
        subject_alt_names.to_vec()
    };
    let cert = rcgen::generate_simple_self_signed(names)
        .context("generate ephemeral self-signed TLS certificate")?;
    let cert_der = CertificateDer::from(cert.cert.der().to_vec());
    let key_der = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(cert.key_pair.serialize_der()));
    build_server_config(vec![cert_der], key_der)
}

fn build_server_config(
    certs: Vec<CertificateDer<'static>>,
    key: PrivateKeyDer<'static>,
) -> Result<Arc<ServerConfig>> {
    let mut config =
        ServerConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
            .with_safe_default_protocol_versions()
            .context("configure TLS protocol versions")?
            .with_no_client_auth()
            .with_single_cert(certs, key)
            .context("build TLS server config")?;
    config.alpn_protocols = vec![b"http/1.1".to_vec()];
    Ok(Arc::new(config))
}

/// Terminate TLS on `public_addr` and forward decrypted bytes to the plaintext
/// HTTP backend at `backend_addr`. Blocks until `shutdown`.
pub fn run_tls_terminator(
    public_addr: &str,
    backend_addr: String,
    identity: TlsIdentity,
    endpoint: &'static str,
    shutdown: &'static AtomicBool,
) -> Result<()> {
    let config = identity.server_config()?;
    let listener = TcpListener::bind(public_addr)
        .with_context(|| format!("bind TLS listener {public_addr}"))?;
    listener.set_nonblocking(true)?;
    tracing::info!(
        event = "tls_listening",
        endpoint,
        addr = %public_addr,
        backend = %backend_addr,
        "terminating TLS in front of plaintext HTTP backend"
    );
    while !shutdown.load(Ordering::SeqCst) {
        match listener.accept() {
            Ok((stream, _)) => {
                let config = config.clone();
                let backend = backend_addr.clone();
                thread::spawn(move || {
                    if let Err(err) = proxy_connection(stream, config, &backend) {
                        tracing::debug!(event = "tls_conn_failed", endpoint, error = %err);
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

fn proxy_connection(client: TcpStream, config: Arc<ServerConfig>, backend: &str) -> Result<()> {
    client.set_nonblocking(true)?;
    let conn = ServerConnection::new(config)?;
    let mut tls = StreamOwned::new(conn, client);

    let mut backend = TcpStream::connect(backend).context("connect to plaintext TLS backend")?;
    backend.set_nonblocking(true)?;

    let mut c2b = Pending::default();
    let mut b2c = Pending::default();
    let mut client_eof = false;
    let mut backend_eof = false;
    let mut backend_wr_closed = false;
    let mut client_wr_closed = false;
    let mut scratch = vec![0u8; BUF_BYTES];

    loop {
        let mut progress = false;

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
        if client_eof && c2b.is_empty() && !backend_wr_closed {
            let _ = backend.shutdown(Shutdown::Write);
            backend_wr_closed = true;
        }

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
        if backend_eof && b2c.is_empty() && !client_wr_closed {
            tls.conn.send_close_notify();
            let _ = flush_spin(&mut tls);
            client_wr_closed = true;
        }

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

    #[test]
    fn parses_pem_cert_and_pkcs8_key() {
        let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
        let cert_pem = pem_text("CERTIFICATE", cert.cert.der());
        let key_der = cert.key_pair.serialize_der();
        let key_pem = pem_text("PRIVATE KEY", &key_der);
        assert_eq!(
            pem_blocks(cert_pem.as_bytes(), &["CERTIFICATE"])
                .unwrap()
                .len(),
            1
        );
        assert!(matches!(
            first_private_key(key_pem.as_bytes()).unwrap(),
            PrivateKeyDer::Pkcs8(_)
        ));
    }

    fn pem_text(label: &str, der: &[u8]) -> String {
        let body = base64::engine::general_purpose::STANDARD.encode(der);
        format!("-----BEGIN {label}-----\n{body}\n-----END {label}-----\n")
    }

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
            let _ = run_tls_terminator(
                &pa,
                backend_addr,
                TlsIdentity::EphemeralSelfSigned {
                    subject_alt_names: vec!["localhost".to_string()],
                },
                "test",
                shutdown,
            );
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

    #[test]
    #[ignore = "needs curl; exercises the live TLS terminator"]
    fn tls_terminator_forwards_http_roundtrip() {
        const BODY_LEN: usize = 250_000;
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
            let _ = run_tls_terminator(
                &pa,
                backend_addr,
                TlsIdentity::EphemeralSelfSigned {
                    subject_alt_names: vec!["localhost".to_string()],
                },
                "test",
                shutdown,
            );
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
