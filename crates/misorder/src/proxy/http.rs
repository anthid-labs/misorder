//! The HTTP adapter.
//!
//! One adapter covers Stripe, Plaid and a thousand REST vendors at once, which
//! is why it is worth writing before either broker codec: the protocol is the
//! same for all of them and the surprises are in the ordering, not the framing.
//!
//! # Which way round this sits
//!
//! The NATS and Postgres adapters sit between the service and something it
//! calls. A webhook runs the other way: the vendor calls the service, and the
//! thing whose ordering matters is a queue of deliveries arriving at an
//! endpoint. So this adapter is an **ingress** proxy. It binds a port, the
//! workload driver posts to that port, and it forwards to the service under
//! test.
//!
//! [`Adapter::bind`] already expresses both placements, because `upstream` is
//! only ever "where I forward to". The one thing that differs is
//! [`Endpoint::env`]: an egress proxy injects its address into the service's
//! configuration, and an ingress proxy has nothing to inject, because the
//! service is not the one connecting. Egress additionally needs a scenario to
//! be able to declare an HTTP dependency, which `Deps` cannot express yet, so
//! it is not wired up here.
//!
//! # Where the forks are
//!
//! - [`PointKind::Connection`] on accept. Refusing here is the delivery that
//!   never arrives, which is what makes a vendor retry.
//! - [`PointKind::Deliver`] on every request about to go to the service.
//! - [`PointKind::Response`] on every response about to go back. Delaying one
//!   is the most valuable fault this adapter has: a vendor whose delivery times
//!   out sends it again, so a slow handler and a duplicate handler are the same
//!   bug seen from two ends.
//!
//! # Events describe the service, not the harness
//!
//! A request is only observed once it has actually been written to the service,
//! and a response once the service has actually produced one. A request the
//! scheduler dropped is never observed at all.
//!
//! This is not a detail. `every_request_reaches_terminal_state` fires when a
//! connection closes with a request unanswered, and if a request misorder
//! itself withheld were observed, that invariant would report the harness's
//! fault as the service's. One invented failure costs more trust than several
//! missed real ones.
//!
//! # Reordering, and what it needs from the client
//!
//! [`Decision::Reorder`] means "let the fork after this one go first", and on
//! one connection that only has meaning if two requests can be in flight at
//! once. HTTP/1.1 allows exactly that, and a deferred request is released when
//! the request that overtakes it arrives, or when the client stops sending.
//!
//! So the ingress contract is that the client sends its requests without
//! waiting for each response and then shuts down its write half. That is a
//! contract this repository keeps with itself: for an ingress proxy the client
//! is misorder's own workload driver, never the user's code. A client that
//! waits for every response before sending the next gives a reorder nothing to
//! swap with, and the deferred request then waits for the write half to close.
//!
//! # What this does not do
//!
//! No TLS and no HTTP/2. Both are real gaps and neither is in the way yet: the
//! service under test is on loopback, and a vendor's delivery has already been
//! terminated by the time misorder sees it.
//!
//! `Transfer-Encoding` is hop-by-hop, so a chunked body is decoded and
//! forwarded with a `Content-Length`. That is a re-framing the specification
//! allows a proxy to do, and it keeps one representation of a body in the
//! events rather than two.

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinSet;

use crate::error::{Error, Result};
use crate::event::{ConnectionId, Event, HttpEvent};
use crate::proxy::{Adapter, Endpoint, ProxyContext};
use crate::trace::{Decision, PointKind};

const PROTOCOL: &str = "http";

/// Bytes of head accepted before a connection is refused.
///
/// A bound rather than a preference. The head is read a line at a time from a
/// socket that may be a service mid-bug, and a peer that sends headers forever
/// would otherwise be an unbounded allocation in the harness.
const MAX_HEAD: usize = 64 * 1024;

/// Bytes of body accepted.
const MAX_BODY: usize = 8 * 1024 * 1024;

/// Header names carrying a vendor's idempotency key, in the order they are
/// tried.
///
/// Stripe spells it the first way and enough others spell it the second that
/// checking both costs nothing. A vendor with a third spelling is a line here.
const IDEMPOTENCY_HEADERS: [&str; 2] = ["idempotency-key", "x-idempotency-key"];

/// Proxies HTTP in front of the service under test.
#[derive(Debug, Default)]
pub struct HttpAdapter {
    listener: Option<TcpListener>,
}

impl HttpAdapter {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl Adapter for HttpAdapter {
    fn protocol(&self) -> &'static str {
        PROTOCOL
    }

    async fn bind(&mut self, upstream: &str) -> Result<Endpoint> {
        if upstream.trim().is_empty() {
            return Err(Error::Environment(
                "the http adapter has no upstream to forward to".to_string(),
            ));
        }

        // Loopback and an ephemeral port. Nothing about a run should be
        // reachable from off the machine, and a fixed port would make two runs
        // on one host collide, which reads as a flaky service rather than as
        // two harnesses fighting.
        let listener = TcpListener::bind(("127.0.0.1", 0)).await?;
        let listen = listener.local_addr()?;

        self.listener = Some(listener);

        Ok(Endpoint {
            protocol: PROTOCOL,
            listen,
            // Nothing to inject: the service under test does not connect to an
            // ingress proxy, the workload driver does. An egress placement is
            // where this carries a vendor's base URL.
            env: Vec::new(),
        })
    }

    async fn serve(&mut self, context: ProxyContext) -> Result<()> {
        let listener = self.listener.take().ok_or_else(|| {
            Error::Internal("the http adapter was served before it was bound".to_string())
        })?;

        let context = Arc::new(context);
        let mut connections = JoinSet::new();

        loop {
            // `biased` so the polling order is fixed rather than left to the
            // runtime. This select decides nothing about the run: it ends the
            // accept loop. Everything the service can observe goes through
            // `decide`.
            let accepted = tokio::select! {
                biased;
                () = context.cancel.cancelled() => break,
                accepted = listener.accept() => accepted,
            };

            let (client, peer) = match accepted {
                Ok(accepted) => accepted,
                Err(error) => return Err(Error::Io(error)),
            };

            let connection = context.next_connection();

            // Before anything is forwarded, so a refused connection costs the
            // upstream nothing. The service never learns this one happened,
            // which is the point: it is the delivery a vendor will retry.
            if let Decision::CloseConnection =
                context.decide(PointKind::Connection, connection, peer.to_string())
            {
                tracing::debug!(%connection, "connection refused by the schedule");
                drop(client);
                continue;
            }

            let context = Arc::clone(&context);

            connections.spawn(async move {
                let result = serve_connection(&context, connection, client).await;

                // Emitted however the connection ended, including badly. The
                // terminal-state invariant is watching for exactly this, and a
                // close that went unreported would be a violation nobody sees.
                context.observe(connection, Event::Http(HttpEvent::ConnectionClosed));

                result
            });
        }

        // Joined rather than detached. A connection task that outlived the run
        // would be writing to a service the runner has already torn down, and
        // its error would surface as a timeout somewhere unrelated.
        let mut first_error = None;

        while let Some(joined) = connections.join_next().await {
            match joined {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    tracing::debug!(%error, "http connection ended with an error");
                    first_error.get_or_insert(error);
                }
                Err(error) if error.is_cancelled() => {}
                Err(error) => {
                    first_error.get_or_insert(Error::Internal(format!(
                        "an http connection task panicked: {error}"
                    )));
                }
            }
        }

        match first_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}

/// One client connection, from accept to close.
///
/// Sequential on purpose. Two tasks serving one connection would race over the
/// order requests reach the service, and that order is the scheduler's to
/// decide.
async fn serve_connection(
    context: &ProxyContext,
    connection: ConnectionId,
    client: TcpStream,
) -> Result<()> {
    let upstream = TcpStream::connect(&context.upstream)
        .await
        .map_err(|error| {
            Error::Environment(format!(
                "the service under test did not accept a connection on {}: {error}",
                context.upstream
            ))
        })?;

    let (client_read, mut client_write) = client.into_split();
    let (upstream_read, mut upstream_write) = upstream.into_split();

    let mut client_read = BufReader::new(client_read);
    let mut upstream_read = BufReader::new(upstream_read);

    // Requests the schedule deferred, most recently deferred first. `Reorder`
    // always names the fork immediately after itself, so releasing in reverse
    // is what "let the next one go first" composes to when it happens twice in
    // a row.
    let mut deferred: Vec<(u64, Request)> = Vec::new();
    let mut arrived: u64 = 0;

    loop {
        let Some(request) = read_request(&mut client_read).await? else {
            break;
        };

        let order = arrived;
        arrived += 1;

        let decision = context.decide(
            PointKind::Deliver,
            connection,
            format!("{} {}", request.method, request.target),
        );

        match decision {
            Decision::Reorder { .. } => {
                deferred.push((order, request));
                continue;
            }
            // The request is never written upstream and never observed, so the
            // service is not asked to answer something it was never sent.
            Decision::Drop => {
                tracing::debug!(%connection, "request dropped by the schedule");
                continue;
            }
            Decision::CloseConnection => {
                tracing::debug!(%connection, "connection closed by the schedule");
                return Ok(());
            }
            Decision::Deliver { .. } | Decision::Corrupt { .. } => {}
            Decision::Hold { .. } => {
                return Err(Error::Internal(format!(
                    "the schedule answered an http request fork with {decision}, which no \
                     http fork can carry out"
                )));
            }
        }

        let mut batch = vec![(order, request, decision)];
        while let Some((order, request)) = deferred.pop() {
            batch.push((order, request, Decision::NEUTRAL));
        }

        if !exchange(
            context,
            connection,
            batch,
            &mut upstream_write,
            &mut upstream_read,
            &mut client_write,
        )
        .await?
        {
            return Ok(());
        }
    }

    // The client stopped sending, so nothing is left to overtake a deferred
    // request. This is the release the ingress contract depends on.
    if !deferred.is_empty() {
        let batch = std::iter::from_fn(|| deferred.pop())
            .map(|(order, request)| (order, request, Decision::NEUTRAL))
            .collect();

        exchange(
            context,
            connection,
            batch,
            &mut upstream_write,
            &mut upstream_read,
            &mut client_write,
        )
        .await?;
    }

    Ok(())
}

/// Forwards a batch of requests, then answers the client in the order it asked.
///
/// Returns whether the connection survived.
///
/// The service is a server, so it answers in the order it was sent, which is
/// the reordered order. Writing those answers back in that order would leave a
/// pipelining client matching every response to the wrong request, so the
/// answers are restored to the client's order before they go out. The service
/// still saw the ordering the scheduler chose, which is the whole object of the
/// exercise.
///
/// Each request is answered before the next is written, rather than sending the
/// batch and collecting replies. Two replies in flight would complete in an
/// order this adapter does not control, and an adapter that let the runtime
/// pick would have put nondeterminism somewhere the trace cannot describe.
async fn exchange(
    context: &ProxyContext,
    connection: ConnectionId,
    batch: Vec<(u64, Request, Decision)>,
    upstream_write: &mut OwnedWriteHalf,
    upstream_read: &mut BufReader<OwnedReadHalf>,
    client_write: &mut OwnedWriteHalf,
) -> Result<bool> {
    let mut answers = Vec::with_capacity(batch.len());

    for (order, request, decision) in batch {
        if let Decision::Deliver { delay } = decision
            && !delay.is_zero()
        {
            tokio::time::sleep(delay).await;
        }

        let mut encoded = request.encode();

        if let Decision::Corrupt { offset } = decision {
            corrupt(&mut encoded, offset);
        }

        upstream_write.write_all(&encoded).await?;
        upstream_write.flush().await?;

        context.observe(
            connection,
            Event::Http(HttpEvent::Request {
                method: request.method.clone(),
                path: request.target.clone(),
                idempotency_key: request.idempotency_key(),
                body: request.body.clone(),
            }),
        );

        let response = read_response(upstream_read, &request.method).await?;

        context.observe(
            connection,
            Event::Http(HttpEvent::Response {
                status: response.status,
                body: response.body.clone(),
            }),
        );

        answers.push((order, response));
    }

    answers.sort_by_key(|(order, _)| *order);

    let mut deferred: Vec<Response> = Vec::new();
    let mut alive = true;

    for (_, response) in answers {
        let decision = context.decide(PointKind::Response, connection, response.status.to_string());

        match decision {
            Decision::Reorder { .. } => {
                deferred.push(response);
                continue;
            }
            Decision::Drop => {
                tracing::debug!(%connection, "response dropped by the schedule");
                continue;
            }
            Decision::CloseConnection => {
                tracing::debug!(%connection, "connection closed by the schedule");
                return Ok(false);
            }
            Decision::Deliver { delay } => {
                if !delay.is_zero() {
                    tokio::time::sleep(delay).await;
                }
            }
            Decision::Corrupt { .. } => {}
            Decision::Hold { .. } => {
                return Err(Error::Internal(format!(
                    "the schedule answered an http response fork with {decision}, which no \
                     http fork can carry out"
                )));
            }
        }

        alive &= !response.closes_connection();

        write_response(client_write, &response, decision).await?;
    }

    // Whatever is left was deferred by a reorder with nothing after it to go
    // first, so it goes now rather than waiting for a fork that will not
    // arrive.
    while let Some(response) = deferred.pop() {
        alive &= !response.closes_connection();

        write_response(client_write, &response, Decision::NEUTRAL).await?;
    }

    Ok(alive)
}

async fn write_response(
    client_write: &mut OwnedWriteHalf,
    response: &Response,
    decision: Decision,
) -> Result<()> {
    let mut encoded = response.encode();

    if let Decision::Corrupt { offset } = decision {
        corrupt(&mut encoded, offset);
    }

    client_write.write_all(&encoded).await?;
    client_write.flush().await?;

    Ok(())
}

/// Flips one bit, at an offset the schedule chose.
///
/// The offset is taken modulo the frame rather than skipped when it is past the
/// end. A decision that quietly did nothing would be a recorded fault that did
/// not happen, and the trace would describe a run nobody had.
fn corrupt(frame: &mut [u8], offset: usize) {
    if frame.is_empty() {
        return;
    }

    let at = offset % frame.len();

    frame[at] ^= 0b0000_0001;
}

/// A request, as it arrived.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Request {
    method: String,
    target: String,
    version: String,
    headers: Vec<(String, String)>,
    body: Bytes,
}

impl Request {
    fn idempotency_key(&self) -> Option<String> {
        IDEMPOTENCY_HEADERS
            .iter()
            .find_map(|name| header(&self.headers, name))
            .map(str::to_string)
    }

    fn encode(&self) -> Vec<u8> {
        let mut out = format!("{} {} {}\r\n", self.method, self.target, self.version).into_bytes();

        write_headers(&mut out, &self.headers, Some(self.body.len()));
        out.extend_from_slice(&self.body);

        out
    }
}

/// A response, as the service produced it.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Response {
    version: String,
    status: u16,
    reason: String,
    headers: Vec<(String, String)>,
    body: Bytes,
    /// Whether the framing was "until the connection closes", in which case
    /// re-framing it with a length would change what the client is told.
    open_ended: bool,
}

impl Response {
    fn closes_connection(&self) -> bool {
        self.open_ended
            || header(&self.headers, "connection").is_some_and(|value| {
                value
                    .split(',')
                    .any(|token| token.trim().eq_ignore_ascii_case("close"))
            })
    }

    fn encode(&self) -> Vec<u8> {
        let mut out = format!(
            "{} {} {}\r\n",
            self.version,
            self.status,
            if self.reason.is_empty() {
                "OK"
            } else {
                &self.reason
            }
        )
        .into_bytes();

        let length = if self.open_ended || !has_body(self.status) {
            None
        } else {
            Some(self.body.len())
        };

        write_headers(&mut out, &self.headers, length);
        out.extend_from_slice(&self.body);

        out
    }
}

/// Writes headers, replacing whatever said how long the body was.
///
/// `Content-Length` and `Transfer-Encoding` are dropped and one length is
/// written back, because the body here has already been decoded. Both are
/// hop-by-hop, so replacing them is a proxy's to do; leaving a stale
/// `Transfer-Encoding: chunked` on a body that is no longer chunked is how a
/// proxy corrupts a stream by accident rather than by decision.
fn write_headers(out: &mut Vec<u8>, headers: &[(String, String)], length: Option<usize>) {
    for (name, value) in headers {
        if name.eq_ignore_ascii_case("content-length")
            || name.eq_ignore_ascii_case("transfer-encoding")
        {
            continue;
        }

        out.extend_from_slice(format!("{name}: {value}\r\n").as_bytes());
    }

    if let Some(length) = length {
        out.extend_from_slice(format!("Content-Length: {length}\r\n").as_bytes());
    }

    out.extend_from_slice(b"\r\n");
}

fn header<'a>(headers: &'a [(String, String)], name: &str) -> Option<&'a str> {
    headers
        .iter()
        .find(|(candidate, _)| candidate.eq_ignore_ascii_case(name))
        .map(|(_, value)| value.as_str())
}

/// Whether a status carries a body at all.
fn has_body(status: u16) -> bool {
    !(matches!(status, 204 | 304) || (100..200).contains(&status))
}

/// How long a body is, and how to find out.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Framing {
    Empty,
    Length(usize),
    Chunked,
    UntilClose,
}

fn framing(headers: &[(String, String)], default_open_ended: bool) -> Result<Framing> {
    // Transfer-Encoding wins over Content-Length when both are present, which
    // is also the case a request smuggling attack is built out of. Preferring
    // one and dropping the other on re-encode is what keeps the service and
    // misorder reading the same bytes as one message.
    if let Some(encoding) = header(headers, "transfer-encoding")
        && encoding
            .split(',')
            .any(|token| token.trim().eq_ignore_ascii_case("chunked"))
    {
        return Ok(Framing::Chunked);
    }

    if let Some(length) = header(headers, "content-length") {
        let length: usize = length.trim().parse().map_err(|_| {
            Error::protocol(
                PROTOCOL,
                format!("content-length `{length}` is not a number"),
            )
        })?;

        if length > MAX_BODY {
            return Err(Error::protocol(
                PROTOCOL,
                format!("body of {length} bytes is over the {MAX_BODY} byte limit"),
            ));
        }

        return Ok(Framing::Length(length));
    }

    if default_open_ended {
        Ok(Framing::UntilClose)
    } else {
        Ok(Framing::Empty)
    }
}

/// Reads one request, or `None` at a clean end of stream.
async fn read_request(reader: &mut BufReader<OwnedReadHalf>) -> Result<Option<Request>> {
    let Some(head) = read_head(reader).await? else {
        return Ok(None);
    };

    let (start, headers) = parse_head(&head)?;

    let mut parts = start.split_whitespace();

    let method = parts.next().unwrap_or_default().to_string();
    let target = parts.next().unwrap_or_default().to_string();
    let version = parts.next().unwrap_or("HTTP/1.1").to_string();

    if method.is_empty() || target.is_empty() {
        return Err(Error::protocol(
            PROTOCOL,
            format!("`{start}` is not a request line"),
        ));
    }

    let body = read_body(reader, framing(&headers, false)?).await?;

    Ok(Some(Request {
        method,
        target,
        version,
        headers,
        body,
    }))
}

/// Reads one response to a request that used `method`.
async fn read_response(reader: &mut BufReader<OwnedReadHalf>, method: &str) -> Result<Response> {
    let head = read_head(reader).await?.ok_or_else(|| {
        Error::protocol(
            PROTOCOL,
            "the service closed the connection without answering".to_string(),
        )
    })?;

    let (start, headers) = parse_head(&head)?;

    let mut parts = start.splitn(3, ' ');

    let version = parts.next().unwrap_or("HTTP/1.1").to_string();
    let status = parts.next().unwrap_or_default();
    let reason = parts.next().unwrap_or_default().trim().to_string();

    let status: u16 = status
        .trim()
        .parse()
        .map_err(|_| Error::protocol(PROTOCOL, format!("`{start}` is not a status line")))?;

    // A HEAD response describes a body it does not carry, so reading one would
    // consume the next response instead.
    let framing = if method.eq_ignore_ascii_case("head") || !has_body(status) {
        Framing::Empty
    } else {
        framing(&headers, true)?
    };

    let body = read_body(reader, framing).await?;

    Ok(Response {
        version,
        status,
        reason,
        headers,
        body,
        open_ended: framing == Framing::UntilClose,
    })
}

/// Reads up to and including the blank line, or `None` at a clean end of
/// stream.
async fn read_head(reader: &mut BufReader<OwnedReadHalf>) -> Result<Option<Vec<u8>>> {
    let mut head = Vec::new();

    loop {
        let before = head.len();

        if reader.read_until(b'\n', &mut head).await? == 0 {
            // Nothing at all is a client that finished. A partial head is a
            // peer that went away mid-message, and that is worth reporting.
            return if head.is_empty() {
                Ok(None)
            } else {
                Err(Error::protocol(
                    PROTOCOL,
                    "the connection ended part way through a head".to_string(),
                ))
            };
        }

        if head.len() > MAX_HEAD {
            return Err(Error::protocol(
                PROTOCOL,
                format!("head is over the {MAX_HEAD} byte limit"),
            ));
        }

        if matches!(&head[before..], b"\r\n" | b"\n") {
            return Ok(Some(head));
        }
    }
}

/// Splits a head into its first line and its headers.
fn parse_head(head: &[u8]) -> Result<(String, Vec<(String, String)>)> {
    let text = std::str::from_utf8(head)
        .map_err(|_| Error::protocol(PROTOCOL, "head is not utf-8".to_string()))?;

    let mut lines = text.split('\n').map(|line| line.trim_end_matches('\r'));

    let start = lines.next().unwrap_or_default().to_string();
    let mut headers = Vec::new();

    for line in lines {
        if line.is_empty() {
            break;
        }

        // Refused rather than joined to the line before it. Obsolete line
        // folding is exactly the ambiguity that lets two parsers disagree about
        // where a message ends, and this adapter has to agree with the service.
        if line.starts_with(' ') || line.starts_with('\t') {
            return Err(Error::protocol(
                PROTOCOL,
                "a folded header line is not accepted".to_string(),
            ));
        }

        let (name, value) = line
            .split_once(':')
            .ok_or_else(|| Error::protocol(PROTOCOL, format!("`{line}` is not a header")))?;

        if name.is_empty() || name.contains(' ') {
            return Err(Error::protocol(
                PROTOCOL,
                format!("`{name}` is not a header name"),
            ));
        }

        headers.push((name.to_string(), value.trim().to_string()));
    }

    Ok((start, headers))
}

async fn read_body(reader: &mut BufReader<OwnedReadHalf>, framing: Framing) -> Result<Bytes> {
    match framing {
        Framing::Empty => Ok(Bytes::new()),
        Framing::Length(length) => {
            let mut body = vec![0u8; length];

            reader.read_exact(&mut body).await?;

            Ok(Bytes::from(body))
        }
        Framing::Chunked => read_chunked(reader).await,
        Framing::UntilClose => {
            let mut body = Vec::new();

            reader
                .take(MAX_BODY as u64 + 1)
                .read_to_end(&mut body)
                .await?;

            if body.len() > MAX_BODY {
                return Err(Error::protocol(
                    PROTOCOL,
                    format!("body is over the {MAX_BODY} byte limit"),
                ));
            }

            Ok(Bytes::from(body))
        }
    }
}

async fn read_chunked(reader: &mut BufReader<OwnedReadHalf>) -> Result<Bytes> {
    let mut body = Vec::new();

    loop {
        let mut line = Vec::new();

        if reader.read_until(b'\n', &mut line).await? == 0 {
            return Err(Error::protocol(
                PROTOCOL,
                "the connection ended part way through a chunked body".to_string(),
            ));
        }

        let line = String::from_utf8_lossy(&line);
        // Chunk extensions are after a semicolon and mean nothing here.
        let size = line.split(';').next().unwrap_or_default().trim();

        let size = usize::from_str_radix(size, 16)
            .map_err(|_| Error::protocol(PROTOCOL, format!("`{size}` is not a chunk size")))?;

        if size == 0 {
            // The trailer section, then a blank line. Dropped rather than
            // forwarded: a trailer belongs to a chunked framing that no longer
            // exists once the body is re-framed with a length.
            loop {
                let mut trailer = Vec::new();

                if reader.read_until(b'\n', &mut trailer).await? == 0
                    || matches!(trailer.as_slice(), b"\r\n" | b"\n")
                {
                    break;
                }
            }

            return Ok(Bytes::from(body));
        }

        if body.len().saturating_add(size) > MAX_BODY {
            return Err(Error::protocol(
                PROTOCOL,
                format!("chunked body is over the {MAX_BODY} byte limit"),
            ));
        }

        let mut chunk = vec![0u8; size];

        reader.read_exact(&mut chunk).await?;
        body.extend_from_slice(&chunk);

        let mut terminator = [0u8; 2];

        reader.read_exact(&mut terminator).await?;

        if &terminator != b"\r\n" {
            return Err(Error::protocol(
                PROTOCOL,
                "a chunk did not end where its size said it would".to_string(),
            ));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::net::SocketAddr;
    use std::sync::Mutex;
    use std::time::Duration;

    use tokio::sync::mpsc;
    use tokio_util::sync::CancellationToken;

    use crate::event::Observed;
    use crate::proxy::EventSink;
    use crate::schedule::{DecisionSource, Scheduler};
    use crate::trace::{DecisionPoint, Recorder};

    /// Answers one chosen fork and stays neutral everywhere else.
    ///
    /// A seeded source would work only by finding a seed that happens to
    /// produce the decision under test, which is a test that breaks when the
    /// profile changes rather than when the adapter does.
    struct At {
        kind: PointKind,
        ordinal: u64,
        decision: Decision,
    }

    impl At {
        fn nothing() -> Arc<dyn DecisionSource> {
            Arc::new(Self {
                kind: PointKind::Deliver,
                ordinal: u64::MAX,
                decision: Decision::NEUTRAL,
            })
        }

        fn once(kind: PointKind, ordinal: u64, decision: Decision) -> Arc<dyn DecisionSource> {
            Arc::new(Self {
                kind,
                ordinal,
                decision,
            })
        }
    }

    impl DecisionSource for At {
        fn decide(&self, point: &DecisionPoint) -> Decision {
            if point.key.kind == self.kind && point.key.ordinal == self.ordinal {
                self.decision
            } else {
                Decision::NEUTRAL
            }
        }
    }

    /// A stand-in for the service under test.
    ///
    /// Speaks HTTP with this module's own reader, which is deliberate: a bug
    /// that makes the proxy emit something it cannot itself read is a bug the
    /// service would have hit too.
    async fn service(listener: TcpListener, seen: Arc<Mutex<Vec<Request>>>, status: u16) {
        while let Ok((stream, _)) = listener.accept().await {
            let seen = Arc::clone(&seen);

            tokio::spawn(async move {
                let (read, mut write) = stream.into_split();
                let mut read = BufReader::new(read);

                while let Ok(Some(request)) = read_request(&mut read).await {
                    let body = format!("answer{}", request.target);

                    seen.lock().expect("seen").push(request);

                    let response = if status == 204 {
                        "HTTP/1.1 204 No Content\r\n\r\n".to_string()
                    } else {
                        format!(
                            "HTTP/1.1 {status} OK\r\nContent-Length: {}\r\n\r\n{body}",
                            body.len()
                        )
                    };

                    if write.write_all(response.as_bytes()).await.is_err() {
                        return;
                    }
                }
            });
        }
    }

    struct Harness {
        proxy: SocketAddr,
        seen: Arc<Mutex<Vec<Request>>>,
        events: mpsc::UnboundedReceiver<Observed>,
        cancel: CancellationToken,
        serving: tokio::task::JoinHandle<Result<()>>,
    }

    impl Harness {
        async fn start(source: Arc<dyn DecisionSource>) -> Self {
            Self::answering(source, 200).await
        }

        async fn answering(source: Arc<dyn DecisionSource>, status: u16) -> Self {
            let listener = TcpListener::bind(("127.0.0.1", 0))
                .await
                .expect("bind the service");
            let upstream = listener.local_addr().expect("service address").to_string();

            let seen = Arc::new(Mutex::new(Vec::new()));

            tokio::spawn(service(listener, Arc::clone(&seen), status));

            let mut adapter = HttpAdapter::new();
            let endpoint = adapter.bind(&upstream).await.expect("bind the proxy");

            let (events, receiver) = EventSink::new();
            let cancel = CancellationToken::new();
            let scheduler = Scheduler::new(source, Recorder::new(0, "http_test"));
            let context = ProxyContext::new(scheduler, upstream, events, cancel.clone());

            let serving = tokio::spawn(async move { adapter.serve(context).await });

            Self {
                proxy: endpoint.listen,
                seen,
                events: receiver,
                cancel,
                serving,
            }
        }

        /// Sends every target on one connection, then stops sending.
        ///
        /// The half-close is the ingress contract: it is what releases a
        /// request the schedule deferred behind one that never arrived.
        async fn post(&self, targets: &[&str]) -> Vec<Response> {
            let stream = TcpStream::connect(self.proxy)
                .await
                .expect("reach the proxy");
            let (read, mut write) = stream.into_split();
            let mut read = BufReader::new(read);

            for target in targets {
                let body = format!("{{\"for\":\"{target}\"}}");

                write
                    .write_all(
                        format!(
                            "POST {target} HTTP/1.1\r\nHost: service\r\nIdempotency-Key: \
                             key{target}\r\nContent-Length: {}\r\n\r\n{body}",
                            body.len()
                        )
                        .as_bytes(),
                    )
                    .await
                    .expect("send");
            }

            write.shutdown().await.expect("half close");

            let mut answers = Vec::new();

            while answers.len() < targets.len() {
                match read_response(&mut read, "POST").await {
                    Ok(response) => answers.push(response),
                    Err(_) => break,
                }
            }

            answers
        }

        async fn finish(mut self) -> (Vec<Request>, Vec<Observed>) {
            self.cancel.cancel();

            self.serving.await.expect("join").expect("serve");

            let mut events = Vec::new();

            while let Ok(observed) = self.events.try_recv() {
                events.push(observed);
            }

            let seen = self.seen.lock().expect("seen").clone();

            (seen, events)
        }
    }

    fn requests(events: &[Observed]) -> Vec<&HttpEvent> {
        events
            .iter()
            .filter_map(|observed| match &observed.event {
                Event::Http(event @ HttpEvent::Request { .. }) => Some(event),
                _ => None,
            })
            .collect()
    }

    #[tokio::test]
    async fn a_post_reaches_the_service_and_its_answer_comes_back() {
        let harness = Harness::start(At::nothing()).await;

        let answers = harness.post(&["/webhooks/stripe"]).await;

        assert_eq!(answers.len(), 1);
        assert_eq!(answers[0].status, 200);
        assert_eq!(answers[0].body, Bytes::from("answer/webhooks/stripe"));

        let (seen, events) = harness.finish().await;

        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].method, "POST");
        assert_eq!(seen[0].target, "/webhooks/stripe");
        assert_eq!(seen[0].body, Bytes::from("{\"for\":\"/webhooks/stripe\"}"));
        assert_eq!(requests(&events).len(), 1);
    }

    #[tokio::test]
    async fn an_idempotency_key_reaches_the_invariants() {
        let harness = Harness::start(At::nothing()).await;

        harness.post(&["/charges"]).await;

        let (_, events) = harness.finish().await;

        match requests(&events).first().expect("a request") {
            HttpEvent::Request {
                idempotency_key, ..
            } => assert_eq!(idempotency_key.as_deref(), Some("key/charges")),
            other => panic!("expected a request, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn a_refused_connection_is_never_shown_to_the_service() {
        let harness = Harness::start(At::once(
            PointKind::Connection,
            0,
            Decision::CloseConnection,
        ))
        .await;

        assert!(harness.post(&["/webhooks/stripe"]).await.is_empty());

        let (seen, events) = harness.finish().await;

        assert!(
            seen.is_empty(),
            "the service was sent {} request(s)",
            seen.len()
        );
        assert!(
            events.is_empty(),
            "a connection the harness refused must not be attributed to the service"
        );
    }

    #[tokio::test]
    async fn a_dropped_request_is_never_observed() {
        let harness = Harness::start(At::once(PointKind::Deliver, 0, Decision::Drop)).await;

        assert!(harness.post(&["/webhooks/stripe"]).await.is_empty());

        let (seen, events) = harness.finish().await;

        assert!(seen.is_empty());
        assert!(
            requests(&events).is_empty(),
            "a request the harness withheld must not look like one the service ignored"
        );
        assert!(
            events
                .iter()
                .any(|observed| matches!(observed.event, Event::Http(HttpEvent::ConnectionClosed))),
            "the close is still reported"
        );
    }

    #[tokio::test]
    async fn a_reorder_swaps_what_the_service_sees_and_not_what_the_client_gets() {
        let harness = Harness::start(At::once(
            PointKind::Deliver,
            0,
            Decision::Reorder { ahead_of: 1 },
        ))
        .await;

        let answers = harness.post(&["/first", "/second"]).await;

        assert_eq!(
            answers
                .iter()
                .map(|answer| answer.body.clone())
                .collect::<Vec<_>>(),
            vec![Bytes::from("answer/first"), Bytes::from("answer/second")],
            "a pipelining client must get its answers in the order it asked"
        );

        let (seen, _) = harness.finish().await;

        assert_eq!(
            seen.iter()
                .map(|request| request.target.as_str())
                .collect::<Vec<_>>(),
            vec!["/second", "/first"],
            "the service is the one that sees the reordering"
        );
    }

    #[tokio::test]
    async fn a_reorder_with_nothing_after_it_still_delivers() {
        let harness = Harness::start(At::once(
            PointKind::Deliver,
            0,
            Decision::Reorder { ahead_of: 1 },
        ))
        .await;

        let answers = harness.post(&["/only"]).await;

        assert_eq!(
            answers.len(),
            1,
            "the half close releases the deferred request"
        );

        let (seen, _) = harness.finish().await;

        assert_eq!(seen.len(), 1);
    }

    #[tokio::test]
    async fn a_delayed_response_still_arrives() {
        let harness = Harness::start(At::once(
            PointKind::Response,
            0,
            Decision::Deliver {
                delay: Duration::from_millis(20),
            },
        ))
        .await;

        let answers = harness.post(&["/slow"]).await;

        assert_eq!(answers.len(), 1);

        let (seen, _) = harness.finish().await;

        assert_eq!(
            seen.len(),
            1,
            "a delay holds the answer, it does not lose it"
        );
    }

    #[tokio::test]
    async fn a_response_with_no_body_does_not_swallow_the_next_one() {
        let harness = Harness::answering(At::nothing(), 204).await;

        let answers = harness.post(&["/a", "/b"]).await;

        assert_eq!(answers.len(), 2, "204 carries no body to read");
        assert!(answers.iter().all(|answer| answer.status == 204));

        harness.finish().await;
    }

    #[tokio::test]
    async fn a_chunked_body_reaches_the_service_with_a_length() {
        let harness = Harness::start(At::nothing()).await;

        let stream = TcpStream::connect(harness.proxy)
            .await
            .expect("reach the proxy");
        let (read, mut write) = stream.into_split();
        let mut read = BufReader::new(read);

        write
            .write_all(
                b"POST /chunked HTTP/1.1\r\nHost: service\r\nTransfer-Encoding: chunked\r\n\r\n\
                  5\r\nhello\r\n6\r\n world\r\n0\r\n\r\n",
            )
            .await
            .expect("send");
        write.shutdown().await.expect("half close");

        read_response(&mut read, "POST").await.expect("an answer");

        let (seen, _) = harness.finish().await;

        assert_eq!(seen[0].body, Bytes::from("hello world"));
        assert_eq!(
            header(&seen[0].headers, "content-length"),
            Some("11"),
            "a decoded body is re-framed with the length it actually has"
        );
        assert_eq!(
            header(&seen[0].headers, "transfer-encoding"),
            None,
            "a stale chunked header on an unchunked body corrupts the next message"
        );
    }

    #[test]
    fn a_folded_header_is_refused() {
        let error = parse_head(b"POST / HTTP/1.1\r\nHost: a\r\n b\r\n\r\n").expect_err("folded");

        assert!(error.to_string().contains("folded"), "got {error}");
    }

    #[test]
    fn a_header_without_a_colon_is_refused() {
        let error = parse_head(b"POST / HTTP/1.1\r\nnonsense\r\n\r\n").expect_err("no colon");

        assert!(error.to_string().contains("not a header"), "got {error}");
    }

    #[test]
    fn a_body_longer_than_the_limit_is_refused_before_it_is_read() {
        let headers = vec![("Content-Length".to_string(), (MAX_BODY + 1).to_string())];

        let error = framing(&headers, false).expect_err("over the limit");

        assert!(error.to_string().contains("limit"), "got {error}");
    }

    #[test]
    fn transfer_encoding_wins_over_a_content_length_that_disagrees() {
        let headers = vec![
            ("Content-Length".to_string(), "9".to_string()),
            ("Transfer-Encoding".to_string(), "chunked".to_string()),
        ];

        assert_eq!(framing(&headers, false).expect("framing"), Framing::Chunked);
    }

    #[test]
    fn corrupting_a_frame_flips_exactly_one_bit() {
        let mut frame = b"POST / HTTP/1.1".to_vec();
        let original = frame.clone();

        corrupt(&mut frame, 3);

        assert_eq!(frame.len(), original.len());
        assert_eq!(
            frame
                .iter()
                .zip(&original)
                .filter(|(after, before)| after != before)
                .count(),
            1
        );
    }

    #[test]
    fn an_offset_past_the_end_still_corrupts_something() {
        let mut frame = b"short".to_vec();
        let original = frame.clone();

        corrupt(&mut frame, 61);

        assert_ne!(
            frame, original,
            "a decision that quietly did nothing is a fault the trace claims and the run never had"
        );
    }

    #[test]
    fn an_idempotency_key_is_found_however_it_is_spelled() {
        for name in ["Idempotency-Key", "X-Idempotency-Key", "idempotency-key"] {
            let request = Request {
                method: "POST".to_string(),
                target: "/charges".to_string(),
                version: "HTTP/1.1".to_string(),
                headers: vec![(name.to_string(), "abc".to_string())],
                body: Bytes::new(),
            };

            assert_eq!(request.idempotency_key().as_deref(), Some("abc"), "{name}");
        }
    }

    #[test]
    fn encoding_a_request_states_the_length_of_the_body_it_carries() {
        let request = Request {
            method: "POST".to_string(),
            target: "/charges".to_string(),
            version: "HTTP/1.1".to_string(),
            headers: vec![
                ("Content-Length".to_string(), "999".to_string()),
                ("Transfer-Encoding".to_string(), "chunked".to_string()),
                ("Host".to_string(), "service".to_string()),
            ],
            body: Bytes::from("hello"),
        };

        let encoded = String::from_utf8(request.encode()).expect("utf-8");

        assert!(
            encoded.starts_with("POST /charges HTTP/1.1\r\n"),
            "{encoded}"
        );
        assert!(encoded.contains("Host: service\r\n"), "{encoded}");
        assert!(encoded.contains("Content-Length: 5\r\n"), "{encoded}");
        assert!(!encoded.contains("999"), "{encoded}");
        assert!(!encoded.contains("Transfer-Encoding"), "{encoded}");
        assert!(encoded.ends_with("\r\n\r\nhello"), "{encoded}");
    }

    #[test]
    fn a_status_with_no_body_is_encoded_without_a_length() {
        let response = Response {
            version: "HTTP/1.1".to_string(),
            status: 204,
            reason: "No Content".to_string(),
            headers: Vec::new(),
            body: Bytes::new(),
            open_ended: false,
        };

        let encoded = String::from_utf8(response.encode()).expect("utf-8");

        assert_eq!(encoded, "HTTP/1.1 204 No Content\r\n\r\n");
    }
}
