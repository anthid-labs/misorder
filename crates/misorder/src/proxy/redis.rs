//! The Redis adapter.
//!
//! The cheapest adapter to write and among the most valuable to have. RESP is
//! a line-based, length-prefixed protocol that fits in a few hundred lines
//! against Postgres's typed message flow, it needs no new fork kinds and no new
//! faults, and it is *reactive*: nothing happens on a Redis connection that a
//! client did not ask for, so a proxy reaches every decision and no simulator
//! is ever needed here.
//!
//! # Where the forks are
//!
//! - [`PointKind::Connection`] on accept. Refusing here is the connection the
//!   client's pool has to rebuild.
//! - [`PointKind::Statement`] on every command about to go to the server.
//!   Reordering two of these is the fault this adapter exists for, and holding
//!   one is what forces an exact interleaving between two clients.
//! - [`PointKind::Response`] on every reply about to go back. Delaying one is
//!   how a client's own timeout fires while the command it gave up on is still
//!   going to be executed.
//!
//! # Why reordering is live here and mostly is not over HTTP
//!
//! [`Decision::Reorder`] means "let the fork after this one go first", and that
//! only has meaning when two things can be in flight at once. Over HTTP that
//! needs pipelining, which almost no real client does. Redis clients pipeline
//! as a matter of course — it is the standard way to avoid a round trip per
//! command — so a Redis scenario permitting `reorder` explores real orderings
//! rather than none.
//!
//! # The bug class this is for
//!
//! Distributed locks. A client takes a lock with `SET key token NX PX ttl`, the
//! lock expires while it is still working, another client takes it, and the
//! first client's `DEL key` releases a lock it no longer owns. That is a
//! delayed command and nothing else, which is one decision in this adapter's
//! vocabulary — and it is the failure the Redis documentation warns about and
//! that people implement anyway.
//!
//! [`lock_released_by_owner`](crate::invariant::builtin::redis) watches for it
//! with no user input.
//!
//! # What this does not do
//!
//! **Pub/Sub, and anything else that pushes.** After `SUBSCRIBE` the server
//! sends messages nobody asked for, which breaks the one-reply-per-command
//! pairing this adapter and its invariants are built on. Rather than forward
//! them and quietly mis-pair every later reply, the adapter refuses the command
//! and says so. `MONITOR` is refused for the same reason.
//!
//! **TLS.** The service under test is on loopback.
//!
//! **RESP3 attributes are read and discarded.** They are metadata about the
//! reply rather than part of it, and nothing here acts on them.

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinSet;

use crate::error::{Error, Result};
use crate::event::{ConnectionId, Event, RedisEvent};
use crate::proxy::{Adapter, Endpoint, ProxyContext};
use crate::trace::{Decision, PointKind};

const PROTOCOL: &str = "redis";

/// Bytes accepted in one line of a RESP frame.
///
/// A bound rather than a preference: the line is read from a socket that may be
/// a service mid-bug, and a peer that never sends `\r\n` would otherwise be an
/// unbounded allocation in the harness.
const MAX_LINE: usize = 64 * 1024;

/// Bytes accepted in one bulk string.
///
/// Redis's own limit is 512MB. Nothing a scenario drives at a service under
/// test needs to be near that, and a harness that will happily buffer half a
/// gigabyte because the length prefix said so is a harness that gets OOM-killed
/// rather than reporting a malformed frame.
const MAX_BULK: usize = 8 * 1024 * 1024;

/// How deeply a reply may nest before it is refused.
///
/// `*1\r\n*1\r\n*1\r\n...` is a small number of bytes and unbounded recursion.
const MAX_DEPTH: usize = 32;

/// Commands that make the server speak without being asked.
///
/// Refused rather than forwarded. Every reply this adapter reads is matched to
/// the command that caused it, and a pushed pub/sub message has no command —
/// forwarding one would shift every later reply onto the wrong command and turn
/// the invariants into a random-number generator.
const PUSHES: [&str; 6] = [
    "SUBSCRIBE",
    "UNSUBSCRIBE",
    "PSUBSCRIBE",
    "PUNSUBSCRIBE",
    "SSUBSCRIBE",
    "MONITOR",
];

/// Proxies a Redis connection.
#[derive(Debug, Default)]
pub struct RedisAdapter {
    listener: Option<TcpListener>,
}

impl RedisAdapter {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl Adapter for RedisAdapter {
    fn protocol(&self) -> &'static str {
        PROTOCOL
    }

    async fn bind(&mut self, upstream: &str) -> Result<Endpoint> {
        if upstream.trim().is_empty() {
            return Err(Error::Environment(
                "the redis adapter has no upstream to forward to".to_string(),
            ));
        }

        let listener = TcpListener::bind(("127.0.0.1", 0)).await?;
        let listen = listener.local_addr()?;

        self.listener = Some(listener);

        Ok(Endpoint {
            protocol: PROTOCOL,
            listen,
            // An egress placement: the service reaches Redis through the proxy
            // by reading an ordinary variable, which is the whole "no SDK"
            // stance in one mechanism.
            env: vec![("REDIS_URL".to_string(), format!("redis://{listen}"))],
        })
    }

    async fn serve(&mut self, context: ProxyContext) -> Result<()> {
        let listener = self.listener.take().ok_or_else(|| {
            Error::Internal("the redis adapter was served before it was bound".to_string())
        })?;

        let context = Arc::new(context);
        let mut connections = JoinSet::new();

        loop {
            // `biased` so the polling order is fixed rather than left to the
            // runtime. This select decides nothing the service can observe; it
            // ends the accept loop.
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

                context.observe(connection, Event::Redis(RedisEvent::ConnectionClosed));

                result
            });
        }

        let mut first_error = None;

        while let Some(joined) = connections.join_next().await {
            match joined {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    tracing::debug!(%error, "redis connection ended with an error");
                    first_error.get_or_insert(error);
                }
                Err(error) if error.is_cancelled() => {}
                Err(error) => {
                    first_error.get_or_insert(Error::Internal(format!(
                        "a redis connection task panicked: {error}"
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
/// Sequential on purpose, like the HTTP adapter and for the same reason: two
/// tasks serving one connection would race over the order commands reach the
/// server, and that order is the scheduler's to decide.
async fn serve_connection(
    context: &ProxyContext,
    connection: ConnectionId,
    client: TcpStream,
) -> Result<()> {
    let upstream = TcpStream::connect(&context.upstream)
        .await
        .map_err(|error| {
            Error::Environment(format!(
                "redis did not accept a connection on {}: {error}",
                context.upstream
            ))
        })?;

    let (client_read, mut client_write) = client.into_split();
    let (upstream_read, mut upstream_write) = upstream.into_split();

    let mut client_read = BufReader::new(client_read);
    let mut upstream_read = BufReader::new(upstream_read);

    // Commands the schedule deferred, most recently deferred first. `Reorder`
    // always names the fork immediately after itself, so releasing in reverse
    // is what "let the next one go first" composes to when it happens twice.
    let mut deferred: Vec<(u64, Command)> = Vec::new();
    let mut arrived: u64 = 0;

    loop {
        let command = tokio::select! {
            biased;
            () = context.cancel.cancelled() => break,
            command = read_command(&mut client_read) => command?,
        };

        let Some(command) = command else {
            break;
        };

        if PUSHES.contains(&command.name.as_str()) {
            return Err(Error::Unsupported(format!(
                "the redis adapter does not proxy `{}` yet: the server then sends messages no \
                 command asked for, and this adapter pairs every reply with the command that \
                 caused it",
                command.name
            )));
        }

        let order = arrived;
        arrived += 1;

        let decision = context.decide(PointKind::Statement, connection, command.name.clone());

        match decision {
            Decision::Reorder { .. } => {
                deferred.push((order, command));
                continue;
            }
            // Never written upstream and never observed, so the server is not
            // asked to answer something it was never sent.
            Decision::Drop => {
                tracing::debug!(%connection, command = %command.name, "command dropped by the schedule");
                continue;
            }
            Decision::CloseConnection => {
                tracing::debug!(%connection, "connection closed by the schedule");
                return Ok(());
            }
            Decision::Deliver { .. } | Decision::Corrupt { .. } | Decision::Hold { .. } => {}
        }

        let mut batch = vec![(order, command, decision)];
        while let Some((order, command)) = deferred.pop() {
            batch.push((order, command, Decision::NEUTRAL));
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
    // command. This is the release a reorder on the last command depends on.
    if !deferred.is_empty() {
        let batch = std::iter::from_fn(|| deferred.pop())
            .map(|(order, command)| (order, command, Decision::NEUTRAL))
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

/// Forwards a batch of commands, then answers the client in the order it asked.
///
/// Returns whether the connection survived.
///
/// Redis answers in the order it was sent, which is the reordered order.
/// Writing those replies straight back would leave a pipelining client matching
/// every reply to the wrong command, so they are restored to the client's order
/// first. The server still saw the ordering the scheduler chose, which is the
/// whole object of the exercise.
///
/// Each command is answered before the next is written. Two replies in flight
/// would complete in an order this adapter does not control, and an adapter
/// that let the runtime pick would have put nondeterminism somewhere the trace
/// cannot describe.
async fn exchange(
    context: &ProxyContext,
    connection: ConnectionId,
    batch: Vec<(u64, Command, Decision)>,
    upstream_write: &mut OwnedWriteHalf,
    upstream_read: &mut BufReader<OwnedReadHalf>,
    client_write: &mut OwnedWriteHalf,
) -> Result<bool> {
    let mut answers = Vec::with_capacity(batch.len());

    for (order, command, decision) in batch {
        if let Decision::Deliver { delay } = decision
            && !delay.is_zero()
        {
            tokio::time::sleep(delay).await;
        }

        let mut encoded = command.encode();

        if let Decision::Corrupt { offset } = decision {
            corrupt(&mut encoded, offset);
        }

        upstream_write.write_all(&encoded).await?;
        upstream_write.flush().await?;

        context.observe(
            connection,
            Event::Redis(RedisEvent::Command {
                name: command.name.clone(),
                args: command.args.clone(),
                order,
            }),
        );

        let reply = read_reply(upstream_read, 0).await?;

        context.observe(
            connection,
            Event::Redis(RedisEvent::Reply {
                error: reply.error,
                value: reply.value.clone(),
            }),
        );

        answers.push((order, reply));
    }

    answers.sort_by_key(|(order, _)| *order);

    for (_, reply) in answers {
        let decision = context.decide(
            PointKind::Response,
            connection,
            if reply.error { "error" } else { "reply" },
        );

        match decision {
            Decision::Deliver { delay } => {
                if !delay.is_zero() {
                    tokio::time::sleep(delay).await;
                }
            }
            Decision::Drop => continue,
            Decision::CloseConnection => return Ok(false),
            Decision::Corrupt { .. } => {}
            Decision::Reorder { .. } | Decision::Hold { .. } => {
                return Err(Error::Internal(format!(
                    "the schedule answered a redis response fork with {decision}, which no \
                     redis fork can carry out"
                )));
            }
        }

        let mut bytes = reply.raw.to_vec();

        if let Decision::Corrupt { offset } = decision {
            corrupt(&mut bytes, offset);
        }

        client_write.write_all(&bytes).await?;
        client_write.flush().await?;
    }

    Ok(true)
}

/// Flips a byte, so a corrupted frame is corrupted rather than truncated.
///
/// Modulo the length rather than bounds-checked away: a decision that quietly
/// did nothing would be a recorded fault the run never had, which is the one
/// outcome worse than missing a bug.
fn corrupt(bytes: &mut [u8], offset: usize) {
    if bytes.is_empty() {
        return;
    }

    let at = offset % bytes.len();
    bytes[at] ^= 0xff;
}

/// One command from a client.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Command {
    /// Upper-cased. Redis command names are case-insensitive and clients
    /// disagree about which case they send.
    pub name: String,
    pub args: Vec<Bytes>,
}

impl Command {
    /// Re-encoded as a RESP array of bulk strings.
    ///
    /// Re-encoded rather than forwarded verbatim, so an inline command becomes
    /// the array form the server prefers and there is one representation on the
    /// wire rather than two. That is a re-framing the protocol allows, and it
    /// keeps the bytes the server sees identical to the bytes the events
    /// describe.
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();

        out.extend_from_slice(format!("*{}\r\n", self.args.len() + 1).as_bytes());
        out.extend_from_slice(format!("${}\r\n", self.name.len()).as_bytes());
        out.extend_from_slice(self.name.as_bytes());
        out.extend_from_slice(b"\r\n");

        for arg in &self.args {
            out.extend_from_slice(format!("${}\r\n", arg.len()).as_bytes());
            out.extend_from_slice(arg);
            out.extend_from_slice(b"\r\n");
        }

        out
    }
}

/// One complete reply, kept whole.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Reply {
    /// Exactly the bytes the server sent, so forwarding is byte-for-byte.
    raw: Bytes,
    error: bool,
    /// The scalar payload, for the kinds that have one.
    value: Option<Bytes>,
}

/// Reads one command, or `None` at end of stream.
///
/// Both framings, because both turn up: a client library sends the array form,
/// and `redis-cli` in interactive mode sends inline commands.
async fn read_command(read: &mut BufReader<OwnedReadHalf>) -> Result<Option<Command>> {
    let Some(line) = read_line(read).await? else {
        return Ok(None);
    };

    if !line.starts_with(b"*") {
        return inline(&line).map(Some);
    }

    let count = parse_count(&line, b'*')?;

    if count <= 0 {
        return Err(Error::Internal(
            "a redis command array declared no elements".to_string(),
        ));
    }

    let mut parts = Vec::with_capacity(count as usize);

    for _ in 0..count {
        let Some(part) = read_bulk(read).await? else {
            return Err(Error::Internal(
                "a redis command contained a null argument".to_string(),
            ));
        };

        parts.push(part);
    }

    let name = String::from_utf8_lossy(&parts[0]).to_uppercase();

    Ok(Some(Command {
        name,
        args: parts[1..].to_vec(),
    }))
}

/// An inline command: a bare line of whitespace-separated words.
fn inline(line: &[u8]) -> Result<Command> {
    let text = String::from_utf8_lossy(line);
    let mut words = text.split_whitespace();

    let name = words
        .next()
        .ok_or_else(|| Error::Internal("an empty redis inline command".to_string()))?
        .to_uppercase();

    Ok(Command {
        name,
        args: words
            .map(|word| Bytes::copy_from_slice(word.as_bytes()))
            .collect(),
    })
}

/// Reads one bulk string body, given its `$` header has not been read yet.
async fn read_bulk(read: &mut BufReader<OwnedReadHalf>) -> Result<Option<Bytes>> {
    let Some(header) = read_line(read).await? else {
        return Err(Error::Internal(
            "a redis bulk string ended before its header".to_string(),
        ));
    };

    let length = parse_count(&header, b'$')?;

    if length < 0 {
        return Ok(None);
    }

    Ok(Some(read_exact_crlf(read, length as usize).await?))
}

/// Reads one complete RESP value and keeps its bytes.
///
/// Recursive, because an array of arrays is one value and forwarding half of it
/// would desynchronise the connection for good. Bounded by [`MAX_DEPTH`], since
/// deep nesting is a handful of bytes and unbounded recursion.
fn read_reply<'a>(
    read: &'a mut BufReader<OwnedReadHalf>,
    depth: usize,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Reply>> + Send + 'a>> {
    Box::pin(async move {
        if depth > MAX_DEPTH {
            return Err(Error::Internal(format!(
                "a redis reply nested deeper than {MAX_DEPTH} levels"
            )));
        }

        let Some(line) = read_line(read).await? else {
            return Err(Error::Internal(
                "the redis connection ended mid-reply".to_string(),
            ));
        };

        let tag = line[0];
        let mut raw = Vec::from(&line[..]);
        raw.extend_from_slice(b"\r\n");

        match tag {
            // Scalars: the line is the whole value. `_` is RESP3 null, `#`
            // boolean, `,` double, `(` big number.
            b'+' | b'-' | b':' | b'_' | b'#' | b',' | b'(' => Ok(Reply {
                error: tag == b'-',
                value: Some(Bytes::copy_from_slice(&line[1..])),
                raw: Bytes::from(raw),
            }),

            // Length-prefixed blobs. `!` is a RESP3 blob error, `=` a verbatim
            // string.
            b'$' | b'!' | b'=' => {
                let length = parse_count(&line, tag)?;

                if length < 0 {
                    return Ok(Reply {
                        raw: Bytes::from(raw),
                        error: false,
                        value: None,
                    });
                }

                let body = read_exact_crlf(read, length as usize).await?;

                raw.extend_from_slice(&body);
                raw.extend_from_slice(b"\r\n");

                Ok(Reply {
                    error: tag == b'!',
                    value: Some(body),
                    raw: Bytes::from(raw),
                })
            }

            // Aggregates. A map is `n` pairs, so `2n` values; a set, array and
            // push are `n`.
            b'*' | b'~' | b'>' | b'%' => {
                let declared = parse_count(&line, tag)?;

                if declared < 0 {
                    return Ok(Reply {
                        raw: Bytes::from(raw),
                        error: false,
                        value: None,
                    });
                }

                let elements = if tag == b'%' { declared * 2 } else { declared };

                for _ in 0..elements {
                    let element = read_reply(read, depth + 1).await?;
                    raw.extend_from_slice(&element.raw);
                }

                Ok(Reply {
                    raw: Bytes::from(raw),
                    error: false,
                    value: None,
                })
            }

            // A RESP3 attribute is metadata attached to the value that follows
            // it. Read and kept so the bytes forwarded stay identical; nothing
            // here acts on it.
            b'|' => {
                let pairs = parse_count(&line, b'|')?.max(0);

                for _ in 0..pairs * 2 {
                    let element = read_reply(read, depth + 1).await?;
                    raw.extend_from_slice(&element.raw);
                }

                let value = read_reply(read, depth + 1).await?;
                raw.extend_from_slice(&value.raw);

                Ok(Reply {
                    raw: Bytes::from(raw),
                    error: value.error,
                    value: value.value,
                })
            }

            other => Err(Error::Internal(format!(
                "unknown redis reply type `{}`",
                other as char
            ))),
        }
    })
}

/// Reads one `\r\n`-terminated line, without the terminator.
async fn read_line(read: &mut BufReader<OwnedReadHalf>) -> Result<Option<Vec<u8>>> {
    let mut line = Vec::new();

    let read_bytes = read
        .take(MAX_LINE as u64)
        .read_until(b'\n', &mut line)
        .await?;

    if read_bytes == 0 {
        return Ok(None);
    }

    if !line.ends_with(b"\n") {
        return Err(Error::Internal(format!(
            "a redis line exceeded {MAX_LINE} bytes without ending"
        )));
    }

    line.pop();

    if line.ends_with(b"\r") {
        line.pop();
    }

    if line.is_empty() {
        return Err(Error::Internal(
            "an empty line where a redis frame was expected".to_string(),
        ));
    }

    Ok(Some(line))
}

/// Reads exactly `length` bytes and the `\r\n` after them.
async fn read_exact_crlf(read: &mut BufReader<OwnedReadHalf>, length: usize) -> Result<Bytes> {
    if length > MAX_BULK {
        return Err(Error::Internal(format!(
            "a redis bulk string declared {length} bytes, over the {MAX_BULK} limit"
        )));
    }

    let mut body = vec![0u8; length + 2];
    read.read_exact(&mut body).await?;

    if !body.ends_with(b"\r\n") {
        return Err(Error::Internal(
            "a redis bulk string did not end where its length said it would".to_string(),
        ));
    }

    body.truncate(length);

    Ok(Bytes::from(body))
}

/// Parses the number after a type byte.
fn parse_count(line: &[u8], expected: u8) -> Result<i64> {
    if line.first() != Some(&expected) {
        return Err(Error::Internal(format!(
            "expected a redis `{}` frame",
            expected as char
        )));
    }

    std::str::from_utf8(&line[1..])
        .ok()
        .and_then(|text| text.trim().parse().ok())
        .ok_or_else(|| {
            Error::Internal(format!(
                "a redis `{}` frame had an unreadable length",
                expected as char
            ))
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::net::SocketAddr;
    use std::sync::Mutex;

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
                kind: PointKind::Statement,
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

    /// A Redis that answers `+OK` to everything and records what it was asked.
    ///
    /// Enough to test ordering, which is what this adapter decides. Real
    /// semantics are the container's job, and a fake that tried to have them
    /// would be a second implementation of Redis to keep correct.
    async fn server(listener: TcpListener, seen: Arc<Mutex<Vec<String>>>) {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };

            let seen = Arc::clone(&seen);

            tokio::spawn(async move {
                let (read, mut write) = stream.into_split();
                let mut read = BufReader::new(read);

                while let Ok(Some(command)) = read_command(&mut read).await {
                    let rendered = std::iter::once(command.name.clone())
                        .chain(
                            command
                                .args
                                .iter()
                                .map(|arg| String::from_utf8_lossy(arg).to_string()),
                        )
                        .collect::<Vec<_>>()
                        .join(" ");

                    seen.lock().expect("seen").push(rendered);

                    if write.write_all(b"+OK\r\n").await.is_err() {
                        return;
                    }
                }
            });
        }
    }

    struct Harness {
        proxy: SocketAddr,
        seen: Arc<Mutex<Vec<String>>>,
        events: mpsc::UnboundedReceiver<Observed>,
        cancel: CancellationToken,
        serving: tokio::task::JoinHandle<Result<()>>,
    }

    impl Harness {
        async fn start(source: Arc<dyn DecisionSource>) -> Self {
            let listener = TcpListener::bind(("127.0.0.1", 0))
                .await
                .expect("bind the server");
            let upstream = listener.local_addr().expect("address").to_string();

            let seen = Arc::new(Mutex::new(Vec::new()));

            tokio::spawn(server(listener, Arc::clone(&seen)));

            let mut adapter = RedisAdapter::new();
            let endpoint = adapter.bind(&upstream).await.expect("bind the proxy");

            let (events, receiver) = EventSink::new();
            let cancel = CancellationToken::new();
            let scheduler = Scheduler::new(source, Recorder::new(0, "redis_test"));
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

        /// Sends every command on one connection without waiting, then stops.
        ///
        /// The half-close is what releases a command the schedule deferred
        /// behind one that never arrived.
        async fn pipeline(&self, commands: &[&str]) -> Vec<u8> {
            let stream = TcpStream::connect(self.proxy)
                .await
                .expect("reach the proxy");
            let (mut read, mut write) = stream.into_split();

            for command in commands {
                let parts: Vec<&str> = command.split(' ').collect();
                let mut out = format!("*{}\r\n", parts.len());

                for part in parts {
                    out.push_str(&format!("${}\r\n{part}\r\n", part.len()));
                }

                write.write_all(out.as_bytes()).await.expect("send");
            }

            write.shutdown().await.expect("half close");

            let mut replies = Vec::new();
            let _ = read.read_to_end(&mut replies).await;

            replies
        }

        async fn finish(mut self) -> (Vec<String>, Vec<Observed>) {
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

    #[tokio::test]
    async fn commands_reach_the_server_in_order_when_nothing_is_perturbed() {
        let harness = Harness::start(At::nothing()).await;

        harness.pipeline(&["SET a 1", "GET a", "DEL a"]).await;

        let (seen, _) = harness.finish().await;

        assert_eq!(seen, vec!["SET a 1", "GET a", "DEL a"]);
    }

    /// The fault this adapter exists for. Redis clients pipeline, so two
    /// commands really can be in flight at once and a reorder really can swap
    /// them.
    #[tokio::test]
    async fn a_reorder_swaps_what_the_server_sees() {
        let harness = Harness::start(At::once(
            PointKind::Statement,
            1,
            Decision::Reorder { ahead_of: 2 },
        ))
        .await;

        harness.pipeline(&["SET k v", "DEL k", "GET k"]).await;

        let (seen, _) = harness.finish().await;

        assert_eq!(
            seen,
            vec!["SET k v", "GET k", "DEL k"],
            "the command at ordinal 1 should have been overtaken by the next"
        );
    }

    /// A reorder on the last command has nothing to swap with, so the release
    /// at half-close is what stops it being lost.
    #[tokio::test]
    async fn a_reorder_on_the_last_command_is_released_when_the_client_stops() {
        let harness = Harness::start(At::once(
            PointKind::Statement,
            1,
            Decision::Reorder { ahead_of: 2 },
        ))
        .await;

        harness.pipeline(&["SET k v", "DEL k"]).await;

        let (seen, _) = harness.finish().await;

        assert_eq!(seen, vec!["SET k v", "DEL k"]);
    }

    /// A dropped command is never written and never observed, so
    /// `every_command_gets_a_reply` reports the server's failures rather than
    /// the harness's.
    #[tokio::test]
    async fn a_dropped_command_is_neither_sent_nor_observed() {
        let harness = Harness::start(At::once(PointKind::Statement, 1, Decision::Drop)).await;

        harness.pipeline(&["SET a 1", "GET a", "DEL a"]).await;

        let (seen, events) = harness.finish().await;

        assert_eq!(seen, vec!["SET a 1", "DEL a"]);

        let commands: Vec<&String> = events
            .iter()
            .filter_map(|observed| match &observed.event {
                Event::Redis(RedisEvent::Command { name, .. }) => Some(name),
                _ => None,
            })
            .collect();

        assert_eq!(commands, vec!["SET", "DEL"], "got {commands:?}");
    }

    /// The reply the client gets has to match the command the client sent, even
    /// though the server saw them in a different order.
    #[tokio::test]
    async fn replies_go_back_in_the_order_the_client_asked() {
        let harness = Harness::start(At::once(
            PointKind::Statement,
            0,
            Decision::Reorder { ahead_of: 1 },
        ))
        .await;

        let replies = harness.pipeline(&["SET a 1", "GET a"]).await;

        let (seen, _) = harness.finish().await;

        assert_eq!(seen, vec!["GET a", "SET a 1"], "the server saw the swap");
        assert_eq!(
            replies,
            b"+OK\r\n+OK\r\n".to_vec(),
            "the client still gets one reply per command it sent"
        );
    }

    /// Pub/Sub breaks the one-reply-per-command pairing everything here rests
    /// on. Refused loudly rather than forwarded and quietly mis-paired.
    #[tokio::test]
    async fn subscribe_is_refused_rather_than_mis_paired() {
        let harness = Harness::start(At::nothing()).await;

        harness.pipeline(&["SUBSCRIBE news"]).await;

        harness.cancel.cancel();

        let error = harness
            .serving
            .await
            .expect("join")
            .expect_err("subscribe is refused");

        assert!(matches!(error, Error::Unsupported(_)), "got {error:?}");
        assert!(error.to_string().contains("SUBSCRIBE"), "got {error}");
    }

    /// An event carries the send position, so a reordering has two halves to
    /// disagree: what the client sent and what the server saw.
    #[tokio::test]
    async fn a_command_event_carries_the_order_the_client_sent_it() {
        let harness = Harness::start(At::once(
            PointKind::Statement,
            0,
            Decision::Reorder { ahead_of: 1 },
        ))
        .await;

        harness.pipeline(&["SET a 1", "GET a"]).await;

        let (_, events) = harness.finish().await;

        let orders: Vec<u64> = events
            .iter()
            .filter_map(|observed| match &observed.event {
                Event::Redis(RedisEvent::Command { order, .. }) => Some(*order),
                _ => None,
            })
            .collect();

        assert_eq!(
            orders,
            vec![1, 0],
            "arrival order is the emission order; the field is the send order"
        );
    }

    #[tokio::test]
    async fn an_inline_command_is_re_encoded_as_an_array() {
        let harness = Harness::start(At::nothing()).await;

        let stream = TcpStream::connect(harness.proxy)
            .await
            .expect("reach the proxy");
        let (mut read, mut write) = stream.into_split();

        write.write_all(b"PING\r\n").await.expect("send");
        write.shutdown().await.expect("half close");

        let mut replies = Vec::new();
        let _ = read.read_to_end(&mut replies).await;

        let (seen, _) = harness.finish().await;

        assert_eq!(seen, vec!["PING"]);
    }

    #[test]
    fn a_command_encodes_as_a_resp_array() {
        let command = Command {
            name: "SET".to_string(),
            args: vec![Bytes::from_static(b"k"), Bytes::from_static(b"v")],
        };

        assert_eq!(
            command.encode(),
            b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n".to_vec()
        );
    }

    #[test]
    fn a_length_that_is_not_a_number_is_refused() {
        assert!(parse_count(b"*abc", b'*').is_err());
        assert!(parse_count(b"$5", b'*').is_err());
        assert_eq!(parse_count(b"*-1", b'*').expect("null array"), -1);
    }

    #[test]
    fn corrupting_changes_a_byte_rather_than_doing_nothing() {
        let mut bytes = b"+OK\r\n".to_vec();
        corrupt(&mut bytes, 99);

        assert_ne!(bytes, b"+OK\r\n".to_vec());
    }
}
