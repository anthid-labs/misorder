//! The NATS and JetStream adapter.
//!
//! First alongside Postgres, and first among the message brokers for a reason
//! that is about the market rather than the protocol: NATS has real production
//! adoption and almost no testing tooling, so the gap between what teams run
//! and what they can test is widest here.
//!
//! # Protocol shape
//!
//! NATS core is a line protocol: `CONNECT`, `PUB`, `SUB`, `MSG`, `HMSG`, `PING`,
//! `PONG`, `+OK`, `-ERR`. Text commands with a length-prefixed payload, which
//! makes it the cheapest adapter to write and the right one to learn the seam
//! on. JetStream rides on top as request/reply to `$JS.API.>` subjects, plus
//! acks published back to a reply subject.
//!
//! # Where the forks are
//!
//! - [`PointKind::Connection`] on accept. Refusing here is the connection the
//!   client's pool has to rebuild.
//! - [`PointKind::Deliver`] on every `MSG`/`HMSG` heading to the service.
//! - [`PointKind::Ack`] on every `PUB` heading back to the server on a
//!   `$JS.ACK.>` subject. This is the valuable one: dropping it produces the
//!   redelivery, and delaying it past `ack_wait` produces the duplicate
//!   processing race where the ack lands at a server that has already given up.
//!
//! Note what is *not* a fork: an ordinary `PUB` on a subject of the service's
//! own. [`fork_kinds`](crate::schedule::fault::fork_kinds) gives NATS
//! `Connection`, `Deliver` and `Ack`, and this adapter reaches exactly those.
//! A service publishing its own outbox therefore has that publish observed but
//! never perturbed. Widening it means adding a fork kind to that table, which
//! is a decision about the vocabulary rather than about this file.
//!
//! # The two directions are two tasks, and that is safe
//!
//! Deliveries flow server to client and acks flow client to server, so each
//! direction is its own loop. They never contend for a fork ordinal: a fork is
//! numbered by `(kind, connection)`, deliveries are the only `Deliver` forks
//! and acks are the only `Ack` forks. Nothing is shared between the tasks
//! except the correlation map below, which carries data and decides nothing.
//!
//! # Correlating an ack with what it acknowledges
//!
//! `no_delivery_after_ack` keys on `(consumer, subject)`, so an ack has to name
//! the subject of the message it settles. The ack subject does not carry one:
//! `$JS.ACK.<stream>.<consumer>.<delivered>.<sseq>.<cseq>.<tm>.<pending>` names
//! the consumer and the sequence and stops there. So the delivery direction
//! records the subject against the reply-to it handed over, and the ack
//! direction reads it back. The ack subject is unique per delivery, which is
//! what makes that a lookup rather than a guess.
//!
//! # What this adapter cannot do, and why Phase 3 exists
//!
//! `ack_wait` expiry fires on the server's own timer with no frame crossing the
//! wire. There is no decision here to intercept, because nothing asked. Forging
//! a redelivery does not help: the real server keeps its own `num_delivered`
//! and will redeliver again later, leaving two state machines that disagree,
//! which is implementing JetStream badly rather than avoiding implementing it.
//! That is why the simulated JetStream in Phase 3 is the first sim written, and
//! why it is diffed against this adapter's real server rather than replacing
//! it.
//!
//! The practical consequence today: [`FaultKind::AckTimeout`] holds the ack for
//! a fixed span and the server's expiry is wall clock. The race is explored,
//! not commanded, until the virtual clock lands.
//!
//! [`FaultKind::AckTimeout`]: crate::schedule::FaultKind::AckTimeout

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinSet;

use crate::error::{Error, Result};
use crate::event::{ConnectionId, Event, NatsEvent, TerminalReason};
use crate::proxy::{Adapter, Endpoint, ProxyContext};
use crate::trace::{Decision, PointKind};

const PROTOCOL: &str = "nats";

/// The subject prefix every JetStream ack is published to.
const ACK_PREFIX: &str = "$JS.ACK.";

/// Bytes accepted in one protocol line.
///
/// A bound rather than a preference: the line is read from a socket that may be
/// a service mid-bug, and a peer that never sends `\r\n` would otherwise be an
/// unbounded allocation in the harness. NATS's own `max_control_line` defaults
/// to 4096; this is generous enough that a large `INFO` or `CONNECT` is never
/// the thing that fails.
const MAX_LINE: usize = 64 * 1024;

/// Bytes accepted in one message payload.
///
/// NATS's own `max_payload` defaults to 1MB and is configurable upward. This is
/// above any of that and still a bound, so a length prefix that says half a
/// gigabyte reports a malformed frame instead of getting the harness OOM
/// killed.
const MAX_PAYLOAD: usize = 8 * 1024 * 1024;

/// Deliveries whose subject is remembered for the ack that will settle them.
///
/// Bounded because the map is only ever inserted into on the delivery path and
/// removed from on the ack path, and a run where nothing acks would otherwise
/// grow it for the length of the run. Dropping the oldest costs an `Acked`
/// event its subject, which is visible in the report, rather than costing the
/// harness its memory.
const MAX_PENDING_ACKS: usize = 16 * 1024;

/// Proxies a NATS connection.
#[derive(Debug, Default)]
pub struct NatsAdapter {
    listener: Option<TcpListener>,
}

impl NatsAdapter {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl Adapter for NatsAdapter {
    fn protocol(&self) -> &'static str {
        PROTOCOL
    }

    async fn bind(&mut self, upstream: &str) -> Result<Endpoint> {
        if upstream.trim().is_empty() {
            return Err(Error::Environment(
                "the nats adapter has no upstream to forward to".to_string(),
            ));
        }

        let listener = TcpListener::bind(("127.0.0.1", 0)).await?;
        let listen = listener.local_addr()?;

        self.listener = Some(listener);

        Ok(Endpoint {
            protocol: PROTOCOL,
            listen,
            // An egress placement: the service reaches NATS through the proxy
            // by reading an ordinary variable, which is the whole "no SDK"
            // stance in one mechanism.
            env: vec![("NATS_URL".to_string(), format!("nats://{listen}"))],
        })
    }

    async fn serve(&mut self, context: ProxyContext) -> Result<()> {
        let listener = self.listener.take().ok_or_else(|| {
            Error::Internal("the nats adapter was served before it was bound".to_string())
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

            connections.spawn(async move { serve_connection(&context, connection, client).await });
        }

        let mut first_error = None;

        while let Some(joined) = connections.join_next().await {
            match joined {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    tracing::debug!(%error, "nats connection ended with an error");
                    first_error.get_or_insert(error);
                }
                Err(error) if error.is_cancelled() => {}
                Err(error) => {
                    first_error.get_or_insert(Error::Internal(format!(
                        "a nats connection task panicked: {error}"
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

/// What a delivery told the ack direction about itself.
///
/// Shared between the two directional tasks. Data only: nothing here is a fork
/// and nothing here changes a decision, so the tasks stay independent in the
/// only sense that matters for replay.
#[derive(Debug, Default)]
struct Pending {
    /// Ack subject to the subject that was delivered on it.
    subjects: Mutex<HashMap<String, String>>,
}

impl Pending {
    fn remember(&self, ack_subject: &str, delivered: &str) {
        let mut subjects = self.subjects.lock().expect("pending acks mutex poisoned");

        // Cleared wholesale rather than evicting one entry. Picking a victim
        // needs an insertion order this map does not keep, and a run that hits
        // this at all is far outside the shape the tool is for.
        if subjects.len() >= MAX_PENDING_ACKS {
            tracing::debug!(
                "more than {MAX_PENDING_ACKS} unacked deliveries; forgetting their subjects"
            );
            subjects.clear();
        }

        subjects.insert(ack_subject.to_string(), delivered.to_string());
    }

    /// The subject an ack settles, or the empty string if it was not seen.
    ///
    /// Not removed on read: a `+WPI` progress ack is followed by the real ack
    /// on the same subject, and forgetting on the first would leave the second
    /// unable to name what it settled.
    fn subject_for(&self, ack_subject: &str) -> String {
        self.subjects
            .lock()
            .expect("pending acks mutex poisoned")
            .get(ack_subject)
            .cloned()
            .unwrap_or_default()
    }
}

/// One client connection, from accept to close.
///
/// Both directions run at once because NATS is not request/reply: a delivery
/// arrives whenever the server has one, and holding it until the client next
/// spoke would be this adapter inventing a protocol the service does not have.
async fn serve_connection(
    context: &ProxyContext,
    connection: ConnectionId,
    client: TcpStream,
) -> Result<()> {
    let upstream = TcpStream::connect(&context.upstream)
        .await
        .map_err(|error| {
            Error::Environment(format!(
                "nats did not accept a connection on {}: {error}",
                context.upstream
            ))
        })?;

    let (client_read, client_write) = client.into_split();
    let (upstream_read, upstream_write) = upstream.into_split();

    let pending = Arc::new(Pending::default());

    // Cancelling this ends the other direction when either one finishes. A
    // half-open proxy would leave the service waiting on a socket nothing is
    // ever going to write to again.
    let closed = context.cancel.child_token();

    let to_server = pump_to_server(
        context,
        connection,
        BufReader::new(client_read),
        upstream_write,
        Arc::clone(&pending),
        closed.clone(),
    );

    let to_client = pump_to_client(
        context,
        connection,
        BufReader::new(upstream_read),
        client_write,
        Arc::clone(&pending),
        closed.clone(),
    );

    let (server_result, client_result) = tokio::join!(to_server, to_client);

    context.observe(connection, Event::Nats(NatsEvent::ConnectionClosed));

    server_result.and(client_result)
}

/// Client to server: publishes, subscriptions, acks.
async fn pump_to_server(
    context: &ProxyContext,
    connection: ConnectionId,
    mut read: BufReader<OwnedReadHalf>,
    mut write: OwnedWriteHalf,
    pending: Arc<Pending>,
    closed: tokio_util::sync::CancellationToken,
) -> Result<()> {
    let result = pump_to_server_inner(
        context, connection, &mut read, &mut write, &pending, &closed,
    )
    .await;

    closed.cancel();

    result
}

async fn pump_to_server_inner(
    context: &ProxyContext,
    connection: ConnectionId,
    read: &mut BufReader<OwnedReadHalf>,
    write: &mut OwnedWriteHalf,
    pending: &Pending,
    closed: &tokio_util::sync::CancellationToken,
) -> Result<()> {
    loop {
        let frame = tokio::select! {
            biased;
            () = closed.cancelled() => break,
            frame = read_frame(read) => frame?,
        };

        let Some(frame) = frame else {
            break;
        };

        let Frame::Publish(publish) = &frame else {
            // CONNECT, SUB, UNSUB, PING, PONG. Not a fork, so straight through.
            write_all(write, frame.raw()).await?;

            // `ready_when = "nats_subscription_active"` is this line. A service
            // whose subscription has crossed the proxy is one the workload can
            // publish at; one that has merely connected is not, and publishing
            // at that one produces a failure that is entirely the harness's.
            if frame.raw().len() >= 4 && frame.raw()[..4].eq_ignore_ascii_case(b"SUB ") {
                context.readiness().subscribed();
            }

            continue;
        };

        if !publish.subject.starts_with(ACK_PREFIX) {
            // An ordinary publish. Observed, never perturbed: see the module
            // docs on which forks this adapter reaches.
            write_all(write, frame.raw()).await?;

            context.observe(
                connection,
                Event::Nats(NatsEvent::Published {
                    subject: publish.subject.clone(),
                    payload: publish.payload.clone(),
                }),
            );

            continue;
        }

        let decision = context.decide(PointKind::Ack, connection, publish.subject.clone());

        match decision {
            Decision::Deliver { delay } => {
                if !delay.is_zero() {
                    tokio::time::sleep(delay).await;
                }
            }
            // The ack never reaches the server, and is never observed. The
            // server learns nothing and will redeliver, which is the fault
            // doing its job rather than something for an invariant to read as
            // the service's fault.
            Decision::Drop => {
                tracing::debug!(%connection, subject = %publish.subject, "ack swallowed by the schedule");
                continue;
            }
            Decision::CloseConnection => return Ok(()),
            Decision::Reorder { .. } | Decision::Corrupt { .. } | Decision::Hold { .. } => {
                return Err(Error::Internal(format!(
                    "the schedule answered a nats ack fork with {decision}, which no nats ack \
                     fork can carry out"
                )));
            }
        }

        write_all(write, frame.raw()).await?;

        let consumer = parse_ack_subject(&publish.subject)
            .map(|parsed| parsed.consumer)
            .unwrap_or_default();
        let subject = pending.subject_for(&publish.subject);

        context.observe(connection, ack_event(&publish.payload, consumer, subject));
    }

    Ok(())
}

/// Server to client: deliveries.
async fn pump_to_client(
    context: &ProxyContext,
    connection: ConnectionId,
    mut read: BufReader<OwnedReadHalf>,
    mut write: OwnedWriteHalf,
    pending: Arc<Pending>,
    closed: tokio_util::sync::CancellationToken,
) -> Result<()> {
    let result = pump_to_client_inner(
        context, connection, &mut read, &mut write, &pending, &closed,
    )
    .await;

    closed.cancel();

    result
}

async fn pump_to_client_inner(
    context: &ProxyContext,
    connection: ConnectionId,
    read: &mut BufReader<OwnedReadHalf>,
    write: &mut OwnedWriteHalf,
    pending: &Pending,
    closed: &tokio_util::sync::CancellationToken,
) -> Result<()> {
    // Deliveries the schedule deferred, most recently deferred first. `Reorder`
    // always names the fork immediately after itself, so releasing in reverse
    // is what "let the next one go first" composes to when it happens twice.
    let mut deferred: Vec<Frame> = Vec::new();

    loop {
        let frame = tokio::select! {
            biased;
            () = closed.cancelled() => break,
            frame = read_frame(read) => frame?,
        };

        let Some(frame) = frame else {
            break;
        };

        let Frame::Message(message) = &frame else {
            // INFO, PING, PONG, +OK, -ERR. Not a fork, so straight through.
            write_all(write, frame.raw()).await?;
            continue;
        };

        let decision = context.decide(PointKind::Deliver, connection, message.subject.clone());

        let mut batch = match decision {
            Decision::Deliver { delay } => {
                if !delay.is_zero() {
                    tokio::time::sleep(delay).await;
                }
                vec![(frame, decision)]
            }
            // Never written to the client and never observed, so the service is
            // not blamed for failing to handle a message it never saw. The
            // server's own redelivery is what follows.
            Decision::Drop => {
                tracing::debug!(%connection, subject = %message.subject, "delivery dropped by the schedule");
                continue;
            }
            Decision::CloseConnection => return Ok(()),
            Decision::Reorder { .. } => {
                deferred.push(frame);
                continue;
            }
            Decision::Corrupt { .. } => vec![(frame, decision)],
            Decision::Hold { .. } => {
                return Err(Error::Internal(format!(
                    "the schedule answered a nats delivery fork with {decision}, which no nats \
                     delivery fork can carry out"
                )));
            }
        };

        while let Some(held) = deferred.pop() {
            batch.push((held, Decision::NEUTRAL));
        }

        for (frame, decision) in batch {
            deliver(context, connection, pending, write, &frame, decision).await?;
        }
    }

    // The server stopped sending, so nothing is left to overtake a deferred
    // delivery. This is the release a reorder on the last delivery depends on.
    while let Some(held) = deferred.pop() {
        deliver(
            context,
            connection,
            pending,
            write,
            &held,
            Decision::NEUTRAL,
        )
        .await?;
    }

    Ok(())
}

/// Writes one delivery to the client and records what it was.
///
/// The correlation is remembered before the write rather than after: the ack
/// for this delivery can reach the other task as soon as the bytes land, and a
/// map written afterwards would lose that race and leave the ack unable to name
/// its subject.
async fn deliver(
    context: &ProxyContext,
    connection: ConnectionId,
    pending: &Pending,
    write: &mut OwnedWriteHalf,
    frame: &Frame,
    decision: Decision,
) -> Result<()> {
    let Frame::Message(message) = frame else {
        return Err(Error::Internal(
            "a nats delivery batch held something that was not a message".to_string(),
        ));
    };

    let parsed = message.reply_to.as_deref().and_then(parse_ack_subject);

    if let Some(reply_to) = &message.reply_to {
        pending.remember(reply_to, &message.subject);
    }

    let mut bytes = frame.raw().to_vec();

    if let Decision::Corrupt { offset } = decision {
        corrupt(&mut bytes, offset);
    }

    write_all(write, &bytes).await?;

    // A message with no `$JS.ACK.>` reply-to is core NATS rather than a
    // JetStream delivery: there is no consumer and no delivery count, and
    // reporting a made-up one would feed `max_deliver_respected` a number the
    // server never produced. The fork still happened, which is what replays.
    let Some(parsed) = parsed else {
        return Ok(());
    };

    context.observe(
        connection,
        Event::Nats(NatsEvent::Delivered {
            subject: message.subject.clone(),
            consumer: parsed.consumer,
            num_delivered: parsed.num_delivered,
            payload: message.payload.clone(),
        }),
    );

    Ok(())
}

/// Which event an ack payload is.
///
/// The payload is the whole of it: an empty body and `+ACK` both mean ack,
/// `-NAK` may carry JSON options after the token, and `+WPI` is a progress
/// signal rather than a settlement. Reading `+WPI` as an ack would have
/// `no_delivery_after_ack` fire on every service that reports progress on a
/// long-running handler, which is the correct thing to do and would look like a
/// bug.
fn ack_event(payload: &[u8], consumer: String, subject: String) -> Event {
    if payload.is_empty() || payload.starts_with(b"+ACK") {
        return Event::Nats(NatsEvent::Acked { consumer, subject });
    }

    if payload.starts_with(b"-NAK") {
        return Event::Nats(NatsEvent::Nacked { consumer, subject });
    }

    if payload.starts_with(b"+TERM") {
        return Event::Nats(NatsEvent::Terminated {
            consumer,
            subject,
            reason: TerminalReason::Terminated,
        });
    }

    // `+WPI`, and anything a future server version adds. Reported as a nack
    // rather than an ack: both mean "not settled", and guessing the safe way
    // round keeps a new token from silently satisfying an invariant.
    Event::Nats(NatsEvent::Nacked { consumer, subject })
}

/// What an ack subject names.
#[derive(Debug, Clone, PartialEq, Eq)]
struct AckSubject {
    consumer: String,
    num_delivered: u32,
}

/// Reads the consumer and delivery count out of a `$JS.ACK.>` subject.
///
/// Two layouts, and the token count is what tells them apart, which is how the
/// official clients do it too:
///
/// ```text
/// $JS.ACK.<stream>.<consumer>.<delivered>.<sseq>.<cseq>.<tm>.<pending>
/// $JS.ACK.<domain>.<hash>.<stream>.<consumer>.<delivered>.<sseq>.<cseq>.<tm>.<pending>.<rand>
/// ```
///
/// Anything else returns `None` rather than a guess. A misread consumer name
/// would key `no_delivery_after_ack` on a consumer that does not exist, and the
/// invariant would then never fire while looking as though it were checking.
fn parse_ack_subject(subject: &str) -> Option<AckSubject> {
    let tokens: Vec<&str> = subject.split('.').collect();

    if !subject.starts_with(ACK_PREFIX) {
        return None;
    }

    let (consumer, delivered) = match tokens.len() {
        9 => (tokens[3], tokens[4]),
        12 => (tokens[5], tokens[6]),
        _ => return None,
    };

    Some(AckSubject {
        consumer: consumer.to_string(),
        num_delivered: delivered.parse().ok()?,
    })
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

async fn write_all(write: &mut OwnedWriteHalf, bytes: &[u8]) -> Result<()> {
    write.write_all(bytes).await?;
    write.flush().await?;

    Ok(())
}

/// One protocol operation, kept whole.
///
/// The raw bytes are carried alongside the parsed view so forwarding is
/// byte-for-byte. Re-encoding would be a re-framing NATS does not need and
/// would put this adapter's idea of the protocol between two peers that already
/// agree on it.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Frame {
    /// `PUB` or `HPUB` from the client.
    Publish(Publish),
    /// `MSG` or `HMSG` from the server.
    Message(Message),
    /// `CONNECT`, `SUB`, `UNSUB`, `PING`, `PONG`, `INFO`, `+OK`, `-ERR`.
    Control(Bytes),
}

impl Frame {
    fn raw(&self) -> &[u8] {
        match self {
            Frame::Publish(publish) => &publish.raw,
            Frame::Message(message) => &message.raw,
            Frame::Control(raw) => raw,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Publish {
    subject: String,
    payload: Bytes,
    raw: Bytes,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Message {
    subject: String,
    /// The ack subject for a JetStream delivery, and the request's reply box
    /// for core NATS request/reply.
    reply_to: Option<String>,
    payload: Bytes,
    raw: Bytes,
}

/// Reads one operation, or `None` at end of stream.
///
/// Shared by both directions. The op name says whether a payload follows, so
/// one reader covers a client socket and a server socket without being told
/// which it has.
async fn read_frame(read: &mut BufReader<OwnedReadHalf>) -> Result<Option<Frame>> {
    let Some(line) = read_line(read).await? else {
        return Ok(None);
    };

    let text = String::from_utf8_lossy(&line).to_string();
    let mut parts = text.split_whitespace();

    // Uppercased because the protocol is case-insensitive on op names. Every
    // real client sends uppercase and the servers accept either, so a lenient
    // read here costs nothing and a strict one would reject a valid peer.
    let op = parts.next().unwrap_or_default().to_ascii_uppercase();
    let args: Vec<&str> = parts.collect();

    let mut raw = line.clone();
    raw.extend_from_slice(b"\r\n");

    match op.as_str() {
        "PUB" | "HPUB" => {
            let headed = op == "HPUB";
            let total = payload_length(&args, if headed { 2 } else { 1 })?;
            let body = read_exact_crlf(read, total).await?;

            raw.extend_from_slice(&body);
            raw.extend_from_slice(b"\r\n");

            let subject = args
                .first()
                .ok_or_else(|| Error::Internal("a nats publish named no subject".to_string()))?;

            // The header block is not part of the payload. Splitting it off
            // matters because `no_delivery_after_ack` keys on the payload, and
            // a redelivery carries different headers for the same body.
            let payload = split_headers(&args, headed, body)?;

            Ok(Some(Frame::Publish(Publish {
                subject: (*subject).to_string(),
                payload,
                raw: Bytes::from(raw),
            })))
        }

        "MSG" | "HMSG" => {
            let headed = op == "HMSG";
            let total = payload_length(&args, if headed { 2 } else { 1 })?;
            let body = read_exact_crlf(read, total).await?;

            raw.extend_from_slice(&body);
            raw.extend_from_slice(b"\r\n");

            let subject = args
                .first()
                .ok_or_else(|| Error::Internal("a nats message named no subject".to_string()))?;

            // `MSG <subject> <sid> [reply-to] <#bytes>`, so a reply-to is
            // present exactly when there is one argument more than the minimum.
            let minimum = if headed { 4 } else { 3 };
            let reply_to = (args.len() > minimum).then(|| args[2].to_string());

            let payload = split_headers(&args, headed, body)?;

            Ok(Some(Frame::Message(Message {
                subject: (*subject).to_string(),
                reply_to,
                payload,
                raw: Bytes::from(raw),
            })))
        }

        _ => Ok(Some(Frame::Control(Bytes::from(raw)))),
    }
}

/// The total byte count from the end of an op's arguments.
///
/// `trailing` is how many numbers sit at the end: one for `PUB`/`MSG`, two for
/// the headed forms, where the total is last and the header size is before it.
fn payload_length(args: &[&str], trailing: usize) -> Result<usize> {
    let total = args
        .last()
        .and_then(|value| value.parse::<usize>().ok())
        .ok_or_else(|| {
            Error::Internal("a nats frame had an unreadable payload length".to_string())
        })?;

    if args.len() < trailing {
        return Err(Error::Internal(
            "a nats frame had fewer arguments than its op requires".to_string(),
        ));
    }

    Ok(total)
}

/// Drops the header block from a headed frame's body.
///
/// The header size is the second-to-last argument, and it counts from the start
/// of the body, so the payload is everything after it.
fn split_headers(args: &[&str], headed: bool, body: Bytes) -> Result<Bytes> {
    if !headed {
        return Ok(body);
    }

    let header_bytes = args
        .get(args.len().wrapping_sub(2))
        .and_then(|value| value.parse::<usize>().ok())
        .ok_or_else(|| {
            Error::Internal("a nats headed frame had an unreadable header length".to_string())
        })?;

    if header_bytes > body.len() {
        return Err(Error::Internal(
            "a nats headed frame declared more header than body".to_string(),
        ));
    }

    Ok(body.slice(header_bytes..))
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
            "a nats line exceeded {MAX_LINE} bytes without ending"
        )));
    }

    line.pop();

    if line.ends_with(b"\r") {
        line.pop();
    }

    Ok(Some(line))
}

/// Reads exactly `length` bytes and the `\r\n` after them.
async fn read_exact_crlf(read: &mut BufReader<OwnedReadHalf>, length: usize) -> Result<Bytes> {
    if length > MAX_PAYLOAD {
        return Err(Error::Internal(format!(
            "a nats payload declared {length} bytes, over the {MAX_PAYLOAD} limit"
        )));
    }

    let mut body = vec![0u8; length + 2];
    read.read_exact(&mut body).await?;

    if !body.ends_with(b"\r\n") {
        return Err(Error::Internal(
            "a nats payload did not end where its length said it would".to_string(),
        ));
    }

    body.truncate(length);

    Ok(Bytes::from(body))
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::net::SocketAddr;
    use std::time::Duration;

    use tokio::sync::mpsc;
    use tokio_util::sync::CancellationToken;

    use crate::event::Observed;
    use crate::proxy::EventSink;
    use crate::schedule::{DecisionSource, Scheduler};
    use crate::trace::{DecisionPoint, Recorder};

    /// An ack subject in the layout a server without a domain produces.
    const ACK_V1: &str = "$JS.ACK.STRATEGY.QUOTER.1.2.3.1700000000000000000.0";

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

    /// A NATS that greets, records what it is sent, and pushes what it is told
    /// to push.
    ///
    /// Enough to test ordering, which is what this adapter decides. Real
    /// JetStream semantics are the container's job, and a fake that tried to
    /// have them would be a second implementation of a server to keep correct.
    async fn server(
        listener: TcpListener,
        seen: Arc<Mutex<Vec<Vec<u8>>>>,
        mut pushes: mpsc::Receiver<Vec<u8>>,
    ) {
        let Ok((stream, _)) = listener.accept().await else {
            return;
        };

        let (read, mut write) = stream.into_split();
        let mut read = BufReader::new(read);

        if write
            .write_all(b"INFO {\"server_id\":\"test\"}\r\n")
            .await
            .is_err()
        {
            return;
        }

        tokio::spawn(async move {
            while let Some(bytes) = pushes.recv().await {
                if write.write_all(&bytes).await.is_err() || write.flush().await.is_err() {
                    return;
                }
            }
        });

        while let Ok(Some(frame)) = read_frame(&mut read).await {
            seen.lock().expect("seen").push(frame.raw().to_vec());
        }
    }

    struct Harness {
        proxy: SocketAddr,
        seen: Arc<Mutex<Vec<Vec<u8>>>>,
        push: mpsc::Sender<Vec<u8>>,
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
            let (push, pushes) = mpsc::channel(16);

            tokio::spawn(server(listener, Arc::clone(&seen), pushes));

            let mut adapter = NatsAdapter::new();
            let endpoint = adapter.bind(&upstream).await.expect("bind the proxy");

            let (events, receiver) = EventSink::new();
            let cancel = CancellationToken::new();
            let scheduler = Scheduler::new(source, Recorder::new(0, "nats_test"));
            let context = ProxyContext::new(scheduler, upstream, events, cancel.clone());

            let serving = tokio::spawn(async move { adapter.serve(context).await });

            Self {
                proxy: endpoint.listen,
                seen,
                push,
                events: receiver,
                cancel,
                serving,
            }
        }

        /// Connects a client and completes the greeting, so the return is a
        /// connection both ends agree is open.
        async fn connect(&self) -> (BufReader<OwnedReadHalf>, OwnedWriteHalf) {
            let stream = TcpStream::connect(self.proxy)
                .await
                .expect("reach the proxy");
            let (read, mut write) = stream.into_split();
            let mut read = BufReader::new(read);

            let info = read_frame(&mut read).await.expect("read").expect("INFO");
            assert!(
                info.raw().starts_with(b"INFO "),
                "the server greets first, and the proxy has to pass that through untouched"
            );

            write
                .write_all(b"CONNECT {\"verbose\":false}\r\nSUB strategy.> 1\r\n")
                .await
                .expect("greet back");
            write.flush().await.expect("flush");

            (read, write)
        }

        /// Waits for the server to have received `count` frames.
        ///
        /// Polled rather than slept on: a fixed sleep is either flaky or slow,
        /// and this is a test of the adapter rather than of the machine it runs
        /// on.
        async fn seen_at_least(&self, count: usize) -> Vec<Vec<u8>> {
            for _ in 0..500 {
                {
                    let seen = self.seen.lock().expect("seen");
                    if seen.len() >= count {
                        return seen.clone();
                    }
                }

                tokio::time::sleep(Duration::from_millis(4)).await;
            }

            panic!(
                "the server saw {} frames, expected {count}",
                self.seen.lock().expect("seen").len()
            );
        }

        /// Every event emitted so far, once the run has settled.
        async fn events(&mut self) -> Vec<Event> {
            tokio::time::sleep(Duration::from_millis(60)).await;

            let mut out = Vec::new();
            while let Ok(observed) = self.events.try_recv() {
                out.push(observed.event);
            }

            out
        }

        async fn stop(self) {
            self.cancel.cancel();
            let _ = tokio::time::timeout(Duration::from_secs(5), self.serving).await;
        }
    }

    /// A JetStream delivery, as a server writes one.
    fn delivery(subject: &str, ack_subject: &str, payload: &str) -> Vec<u8> {
        format!(
            "MSG {subject} 1 {ack_subject} {}\r\n{payload}\r\n",
            payload.len()
        )
        .into_bytes()
    }

    #[tokio::test]
    async fn a_neutral_schedule_forwards_both_directions_untouched() {
        let harness = Harness::start(At::nothing()).await;
        let (mut read, mut write) = harness.connect().await;

        harness
            .push
            .send(delivery("strategy.schedule.q", ACK_V1, "hello"))
            .await
            .expect("push");

        let delivered = read_frame(&mut read).await.expect("read").expect("MSG");

        let Frame::Message(message) = &delivered else {
            panic!("expected a message, got {delivered:?}");
        };

        assert_eq!(message.subject, "strategy.schedule.q");
        assert_eq!(message.payload, Bytes::from_static(b"hello"));
        assert_eq!(message.reply_to.as_deref(), Some(ACK_V1));

        write
            .write_all(format!("PUB {ACK_V1} 0\r\n\r\n").as_bytes())
            .await
            .expect("ack");
        write.flush().await.expect("flush");

        // CONNECT, SUB, then the ack.
        let seen = harness.seen_at_least(3).await;
        assert!(
            seen[2].starts_with(format!("PUB {ACK_V1}").as_bytes()),
            "the ack has to reach the server byte for byte, got {:?}",
            String::from_utf8_lossy(&seen[2])
        );

        harness.stop().await;
    }

    #[tokio::test]
    async fn a_delivery_is_reported_with_the_consumer_that_got_it() {
        let mut harness = Harness::start(At::nothing()).await;
        let (mut read, _write) = harness.connect().await;

        harness
            .push
            .send(delivery("strategy.schedule.q", ACK_V1, "hello"))
            .await
            .expect("push");

        read_frame(&mut read).await.expect("read").expect("MSG");

        let events = harness.events().await;

        assert!(
            events.iter().any(|event| matches!(
                event,
                Event::Nats(NatsEvent::Delivered { subject, consumer, num_delivered, payload })
                    if subject == "strategy.schedule.q"
                        && consumer == "QUOTER"
                        && *num_delivered == 1
                        && payload == "hello"
            )),
            "the consumer and delivery count come out of the ack subject: {events:?}"
        );

        harness.stop().await;
    }

    /// The correlation the whole `Pending` map exists for.
    ///
    /// `no_delivery_after_ack` keys on `(consumer, subject)`, and the ack
    /// subject carries no subject of its own. Without this the ack arrives
    /// naming an empty subject, the invariant never matches a delivery, and it
    /// passes every run while looking as though it checked something.
    #[tokio::test]
    async fn an_ack_names_the_subject_it_settles() {
        let mut harness = Harness::start(At::nothing()).await;
        let (mut read, mut write) = harness.connect().await;

        harness
            .push
            .send(delivery("strategy.schedule.q", ACK_V1, "hello"))
            .await
            .expect("push");

        read_frame(&mut read).await.expect("read").expect("MSG");

        write
            .write_all(format!("PUB {ACK_V1} 0\r\n\r\n").as_bytes())
            .await
            .expect("ack");
        write.flush().await.expect("flush");

        harness.seen_at_least(3).await;

        let events = harness.events().await;

        assert!(
            events.iter().any(|event| matches!(
                event,
                Event::Nats(NatsEvent::Acked { consumer, subject })
                    if consumer == "QUOTER" && subject == "strategy.schedule.q"
            )),
            "an ack has to name what it acknowledged: {events:?}"
        );

        harness.stop().await;
    }

    #[tokio::test]
    async fn a_swallowed_ack_never_reaches_the_server() {
        let harness = Harness::start(At::once(PointKind::Ack, 0, Decision::Drop)).await;
        let (mut read, mut write) = harness.connect().await;

        harness
            .push
            .send(delivery("strategy.schedule.q", ACK_V1, "hello"))
            .await
            .expect("push");

        read_frame(&mut read).await.expect("read").expect("MSG");

        write
            .write_all(format!("PUB {ACK_V1} 0\r\n\r\nPUB strategy.done 2\r\nok\r\n").as_bytes())
            .await
            .expect("write");
        write.flush().await.expect("flush");

        // CONNECT, SUB, then the publish that followed the swallowed ack. The
        // ack itself must not be among them.
        let seen = harness.seen_at_least(3).await;

        assert!(
            !seen
                .iter()
                .any(|frame| frame.starts_with(format!("PUB {ACK_V1}").as_bytes())),
            "the swallowed ack reached the server anyway: {:?}",
            seen.iter()
                .map(|frame| String::from_utf8_lossy(frame).to_string())
                .collect::<Vec<_>>()
        );

        harness.stop().await;
    }

    #[tokio::test]
    async fn a_dropped_delivery_never_reaches_the_service() {
        let mut harness = Harness::start(At::once(PointKind::Deliver, 0, Decision::Drop)).await;
        let (mut read, _write) = harness.connect().await;

        harness
            .push
            .send(delivery("strategy.schedule.first", ACK_V1, "one"))
            .await
            .expect("push");
        harness
            .push
            .send(delivery("strategy.schedule.second", ACK_V1, "two"))
            .await
            .expect("push");

        let delivered = read_frame(&mut read).await.expect("read").expect("MSG");

        let Frame::Message(message) = &delivered else {
            panic!("expected a message, got {delivered:?}");
        };

        assert_eq!(
            message.subject, "strategy.schedule.second",
            "the first delivery was dropped, so the second is the one that arrives"
        );

        let events = harness.events().await;

        assert!(
            !events.iter().any(|event| matches!(
                event,
                Event::Nats(NatsEvent::Delivered { subject, .. })
                    if subject == "strategy.schedule.first"
            )),
            "a dropped delivery must not be observed, or the service is blamed for not \
             handling a message it never saw: {events:?}"
        );

        harness.stop().await;
    }

    #[tokio::test]
    async fn a_reordered_delivery_arrives_after_the_one_behind_it() {
        let harness = Harness::start(At::once(
            PointKind::Deliver,
            0,
            Decision::Reorder { ahead_of: 1 },
        ))
        .await;
        let (mut read, _write) = harness.connect().await;

        harness
            .push
            .send(delivery("strategy.schedule.first", ACK_V1, "one"))
            .await
            .expect("push");
        harness
            .push
            .send(delivery("strategy.schedule.second", ACK_V1, "two"))
            .await
            .expect("push");

        let mut arrived = Vec::new();
        for _ in 0..2 {
            let frame = read_frame(&mut read).await.expect("read").expect("MSG");
            let Frame::Message(message) = frame else {
                panic!("expected a message");
            };
            arrived.push(message.subject);
        }

        assert_eq!(
            arrived,
            vec![
                "strategy.schedule.second".to_string(),
                "strategy.schedule.first".to_string()
            ],
            "reorder means the fork after this one goes first"
        );

        harness.stop().await;
    }

    /// A publish on the service's own subject is observed and left alone.
    ///
    /// It is not a fork: `fork_kinds("nats")` gives this adapter `Connection`,
    /// `Deliver` and `Ack`, and reaching a fourth kind here would put decisions
    /// in a trace that the fault table says cannot exist.
    #[tokio::test]
    async fn an_ordinary_publish_is_observed_but_not_forked() {
        let mut harness = Harness::start(At::nothing()).await;
        let (_read, mut write) = harness.connect().await;

        write
            .write_all(b"PUB strategy.fill.abc 4\r\nfill\r\n")
            .await
            .expect("publish");
        write.flush().await.expect("flush");

        harness.seen_at_least(3).await;

        let events = harness.events().await;

        assert!(
            events.iter().any(|event| matches!(
                event,
                Event::Nats(NatsEvent::Published { subject, payload })
                    if subject == "strategy.fill.abc" && payload == "fill"
            )),
            "the service's own publish has to be visible in the report: {events:?}"
        );

        harness.stop().await;
    }

    #[test]
    fn an_ack_subject_names_its_consumer_in_both_layouts() {
        assert_eq!(
            parse_ack_subject(ACK_V1),
            Some(AckSubject {
                consumer: "QUOTER".to_string(),
                num_delivered: 1,
            })
        );

        assert_eq!(
            parse_ack_subject(
                "$JS.ACK.dom.acchash.STRATEGY.QUOTER.4.2.3.1700000000000000000.0.rand"
            ),
            Some(AckSubject {
                consumer: "QUOTER".to_string(),
                num_delivered: 4,
            }),
            "a server with a domain adds three tokens, and the consumer moves"
        );
    }

    /// A subject this does not understand returns nothing rather than a guess.
    ///
    /// A misread consumer name keys `no_delivery_after_ack` on a consumer that
    /// never existed, and the invariant then never fires while looking as
    /// though it were checking.
    #[test]
    fn an_unrecognised_ack_subject_is_not_guessed_at() {
        assert_eq!(parse_ack_subject("$JS.ACK.too.few.tokens"), None);
        assert_eq!(parse_ack_subject("strategy.schedule.q"), None);
        assert_eq!(
            parse_ack_subject("$JS.ACK.STRATEGY.QUOTER.not_a_number.2.3.170.0"),
            None
        );
    }

    /// Headers are not payload.
    ///
    /// `no_delivery_after_ack` keys on the payload, and a redelivery carries
    /// different headers for the same body. Counting the header block as
    /// payload would make every redelivery look like a different message and
    /// the invariant would never fire.
    #[tokio::test]
    async fn a_headed_delivery_reports_its_payload_without_the_headers() {
        let harness = Harness::start(At::nothing()).await;
        let (mut read, _write) = harness.connect().await;

        // "NATS/1.0\r\n\r\n" is 12 bytes, "payload" is 7, so 19 in total.
        harness
            .push
            .send(
                format!("HMSG strategy.schedule.q 1 {ACK_V1} 12 19\r\nNATS/1.0\r\n\r\npayload\r\n")
                    .into_bytes(),
            )
            .await
            .expect("push");

        let frame = read_frame(&mut read).await.expect("read").expect("HMSG");

        let Frame::Message(message) = &frame else {
            panic!("expected a message, got {frame:?}");
        };

        assert_eq!(message.payload, Bytes::from_static(b"payload"));
        assert_eq!(
            message.reply_to.as_deref(),
            Some(ACK_V1),
            "a headed frame has one more length than a plain one, and the reply-to must not \
             shift with it"
        );
        assert!(
            frame.raw().ends_with(b"NATS/1.0\r\n\r\npayload\r\n"),
            "the headers still have to reach the service"
        );

        harness.stop().await;
    }

    #[test]
    fn a_progress_ack_is_not_a_settlement() {
        let acked = ack_event(b"+ACK", "QUOTER".to_string(), "s".to_string());
        assert!(matches!(acked, Event::Nats(NatsEvent::Acked { .. })));

        let empty = ack_event(b"", "QUOTER".to_string(), "s".to_string());
        assert!(
            matches!(empty, Event::Nats(NatsEvent::Acked { .. })),
            "an empty body is the ordinary ack every client sends"
        );

        // The quoter's long-running handler reports progress to hold off
        // `ack_wait`. Reading that as a settlement would have
        // `no_delivery_after_ack` fire on the redelivery that correctly follows.
        let progress = ack_event(b"+WPI", "QUOTER".to_string(), "s".to_string());
        assert!(matches!(progress, Event::Nats(NatsEvent::Nacked { .. })));

        let nak = ack_event(
            b"-NAK {\"delay\":100}",
            "QUOTER".to_string(),
            "s".to_string(),
        );
        assert!(matches!(nak, Event::Nats(NatsEvent::Nacked { .. })));

        let term = ack_event(b"+TERM", "QUOTER".to_string(), "s".to_string());
        assert!(matches!(
            term,
            Event::Nats(NatsEvent::Terminated {
                reason: TerminalReason::Terminated,
                ..
            })
        ));
    }

    #[tokio::test]
    async fn binding_without_an_upstream_is_refused() {
        let error = NatsAdapter::new()
            .bind("  ")
            .await
            .expect_err("an empty upstream is not a proxy, it is a black hole");

        assert!(matches!(error, Error::Environment(_)), "{error}");
    }

    #[tokio::test]
    async fn the_service_is_pointed_at_the_proxy_through_nats_url() {
        let mut adapter = NatsAdapter::new();
        let endpoint = adapter.bind("127.0.0.1:4222").await.expect("bind");

        assert_eq!(endpoint.protocol, "nats");
        assert_eq!(
            endpoint.env,
            vec![(
                "NATS_URL".to_string(),
                format!("nats://{}", endpoint.listen)
            )],
            "the service reads an ordinary variable and is never told why"
        );
    }
}
