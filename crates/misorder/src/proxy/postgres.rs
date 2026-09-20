//! The Postgres adapter.
//!
//! On the critical path with NATS, and the one whose fidelity argument is
//! strongest: holding statements to force an exact interleaving against a real
//! server produces real serialization failures, real lock waits and real
//! isolation semantics. A simulator would have to reimplement the planner to
//! get any of that right, so this adapter is permanent.
//!
//! TimescaleDB is a flag on this adapter, not a separate thing: it speaks the
//! same wire protocol, and the differences that matter are in what the server
//! does with a statement rather than in how the statement arrives.
//!
//! # Protocol shape
//!
//! A length-prefixed binary protocol: a startup message, then typed messages in
//! both directions. Simple query (`Q`) and extended query
//! (`Parse`/`Bind`/`Execute`/`Sync`) both matter, because a service using
//! prepared statements has its statement boundaries in different places.
//!
//! # What counts as one statement
//!
//! The fork is per *group*, not per message. A simple query is one `Q`; an
//! extended query is every message up to and including the `Sync` that asks the
//! server to answer. Forking per message instead would put four forks in front
//! of one `SELECT` and let a schedule drop a `Bind` while delivering its
//! `Execute`, which is not a fault a real system can have: it is a corrupt
//! conversation, and every later reply on that connection would be answering
//! the wrong question.
//!
//! # Where the forks are
//!
//! - [`PointKind::Statement`] before a statement group goes upstream. Holding
//!   one here until another completes is the interleaving control, and it is
//!   the reason to write this adapter at all.
//! - [`PointKind::Response`] before a result goes back.
//! - [`PointKind::Connection`] on accept.
//!
//! # Reading the session, not just forwarding it
//!
//! The adapter tracks enough state to emit
//! [`PostgresEvent`](crate::event::PostgresEvent): transaction boundaries,
//! statement text, and the SQLSTATE on an `ErrorResponse`.
//!
//! Transaction boundaries come from what the *client asked for*, not from the
//! `ReadyForQuery` status byte the server sends back. The two disagree in
//! exactly the case the built-in invariant exists for: a `COMMIT` inside a
//! failed transaction is answered with the command tag `ROLLBACK`, so an
//! adapter that believed the server would report no commit at all, and
//! `no_commit_after_error` could never fire. Recognising three leading keywords
//! is not understanding SQL, and it is the only reading that makes the event
//! mean "the service believes it committed".
//!
//! # Where the trace stops and the report begins
//!
//! The fork detail carries the leading keyword, so a reproducer reads
//! `statement #4: INSERT`. The statement text goes in the event instead. A
//! trace is a document people attach to public issues, and a literal inside a
//! statement is the user's production shape.
//!
//! # What this does not do
//!
//! **TLS.** `SSLRequest` is answered with `N` rather than forwarded. A proxy
//! that let the connection negotiate TLS would be watching an opaque stream,
//! and the decisions this tool exists to make all live inside it. The service
//! under test is on loopback, so plaintext is not the exposure it would be
//! anywhere else.
//!
//! **`LISTEN`/`NOTIFY`.** A `NotificationResponse` arrives with no statement
//! asking for it, which breaks the one-answer-per-statement pairing this
//! adapter rests on. Refused when one appears, rather than forwarded and
//! quietly mis-paired. This is the one part of Postgres that fails the
//! "does anything happen without a client asking?" test.
//!
//! **`COPY`.** A copy switches the connection into a streaming mode with no
//! statement boundaries in it. Refused when the server announces one.
//!
//! **Pipelining by `Flush` rather than `Sync`.** libpq's pipeline mode ends a
//! statement with `Flush`, which asks for output without ending the group, so
//! there is no boundary to fork at. Refused rather than guessed at.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinSet;

use crate::error::{Error, Result};
use crate::event::{ConnectionId, Event, PostgresEvent};
use crate::proxy::{Adapter, Endpoint, ProxyContext};
use crate::trace::{Decision, PointKind};

const PROTOCOL: &str = "postgres";

/// Version 3.0, as a startup packet encodes it.
const VERSION_3: i32 = 196_608;

/// The three startup packets that are not a startup message.
const SSL_REQUEST: i32 = 80_877_103;
const GSSENC_REQUEST: i32 = 80_877_104;
const CANCEL_REQUEST: i32 = 80_877_102;

/// Bytes accepted in one message.
///
/// A bound rather than a preference: the length prefix arrives from a service
/// that may be mid-bug, and a harness that allocates whatever an `i32` said is
/// a harness that gets OOM-killed instead of reporting a malformed frame.
/// Postgres's own ceiling is 1GB, and nothing a scenario drives is near it.
const MAX_MESSAGE: usize = 64 * 1024 * 1024;

/// Bytes accepted in one startup packet. Postgres itself refuses over 10000.
const MAX_STARTUP: usize = 64 * 1024;

/// Bytes accepted in one statement group before it is refused.
///
/// A client that sends `Parse` forever without a `Sync` is not pipelining, it
/// is an unbounded buffer in the harness.
const MAX_GROUP: usize = MAX_MESSAGE;

/// Prepared statements remembered per connection.
///
/// The map exists so a `Bind`/`Execute` group can report the text it is
/// running. Capped because the text is the client's to choose how much of, and
/// a pool connection that prepares in a loop would otherwise grow this forever.
/// Past the cap the group reports its portal name instead, which is worse to
/// read and still correct.
const MAX_PREPARED: usize = 4096;

/// Proxies a Postgres connection.
///
/// Carries the session's identity because the whole point of the egress
/// placement is that the service is told where to connect through its ordinary
/// configuration, and for Postgres that configuration is a URL rather than an
/// address. The values are the scenario's: misorder creates a container with
/// them, or reaches an existing server with them, and either way the service
/// and the terminal SQL check are given the same answer.
#[derive(Debug, Default)]
pub struct PostgresAdapter {
    listener: Option<TcpListener>,
    database: String,
    user: String,
    password: String,
}

impl PostgresAdapter {
    pub fn new(
        database: impl Into<String>,
        user: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        Self {
            listener: None,
            database: database.into(),
            user: user.into(),
            password: password.into(),
        }
    }
}

#[async_trait]
impl Adapter for PostgresAdapter {
    fn protocol(&self) -> &'static str {
        PROTOCOL
    }

    async fn bind(&mut self, upstream: &str) -> Result<Endpoint> {
        if upstream.trim().is_empty() {
            return Err(Error::Environment(
                "the postgres adapter has no upstream to forward to".to_string(),
            ));
        }

        let listener = TcpListener::bind(("127.0.0.1", 0)).await?;
        let listen = listener.local_addr()?;

        self.listener = Some(listener);

        // An egress placement: the service reaches Postgres through the proxy
        // by reading its ordinary configuration, which is the whole "no SDK"
        // stance in one mechanism. Both spellings, because a service reads one
        // or the other and neither is unusual enough to make the other wrong.
        let Self {
            database,
            user,
            password,
            ..
        } = &self;

        Ok(Endpoint {
            protocol: PROTOCOL,
            listen,
            env: vec![
                ("PGHOST".to_string(), listen.ip().to_string()),
                ("PGPORT".to_string(), listen.port().to_string()),
                ("PGDATABASE".to_string(), database.clone()),
                ("PGUSER".to_string(), user.clone()),
                ("PGPASSWORD".to_string(), password.clone()),
                (
                    "DATABASE_URL".to_string(),
                    format!("postgres://{user}:{password}@{listen}/{database}"),
                ),
            ],
        })
    }

    async fn serve(&mut self, context: ProxyContext) -> Result<()> {
        let listener = self.listener.take().ok_or_else(|| {
            Error::Internal("the postgres adapter was served before it was bound".to_string())
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

                context.observe(connection, Event::Postgres(PostgresEvent::Disconnected));

                result
            });
        }

        let mut first_error = None;

        while let Some(joined) = connections.join_next().await {
            match joined {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    tracing::debug!(%error, "postgres connection ended with an error");
                    first_error.get_or_insert(error);
                }
                Err(error) if error.is_cancelled() => {}
                Err(error) => {
                    first_error.get_or_insert(Error::Internal(format!(
                        "a postgres connection task panicked: {error}"
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

/// One statement group the schedule deferred.
struct Deferred {
    /// Where it was in the order the client sent, so answers can be restored.
    order: u64,
    group: Group,
    /// The statement fork this waits on, or `None` to go with the next one.
    ///
    /// `None` is a reorder, which names the fork immediately after itself.
    /// `Some` is a hold, which names one that may be further off.
    until: Option<u64>,
}

/// One client connection, from accept to close.
///
/// Sequential on purpose, like the other adapters and for the same reason: two
/// tasks serving one connection would race over the order statements reach the
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
                "postgres did not accept a connection on {}: {error}",
                context.upstream
            ))
        })?;

    let (client_read, mut client_write) = client.into_split();
    let (upstream_read, mut upstream_write) = upstream.into_split();

    let mut client_read = BufReader::new(client_read);
    let mut upstream_read = BufReader::new(upstream_read);

    let Some(startup) = handshake(
        &mut client_read,
        &mut client_write,
        &mut upstream_read,
        &mut upstream_write,
    )
    .await?
    else {
        return Ok(());
    };

    context.observe(
        connection,
        Event::Postgres(PostgresEvent::Connected {
            database: startup.database.clone(),
        }),
    );

    // Reported once the server has answered, not when the socket was accepted.
    // A postmaster still running its own startup accepts TCP and then refuses
    // the session, and a scenario that drove its workload at that point would
    // be testing the harness's impatience.
    context.readiness().postgres_connected();

    let mut session = Session::default();
    let mut deferred: Vec<Deferred> = Vec::new();
    let mut arrived: u64 = 0;

    // Mirrors the ordinal `ProxyContext` hands out, because every statement
    // fork on this connection is taken right here and exactly once per group.
    // Kept because a `Hold` names another fork by ordinal, and an adapter that
    // could not say which one it was could not carry the decision out.
    let mut statements: u64 = 0;

    loop {
        let group = tokio::select! {
            biased;
            () = context.cancel.cancelled() => break,
            group = read_group(&mut client_read, &mut session) => group?,
        };

        let Some(group) = group else {
            break;
        };

        // No fork. A `Terminate` asks the server for nothing, so there is no
        // answer to delay, drop or reorder, and a fork whose every outcome is
        // the same is a decision in the trace that describes nothing.
        if group.terminate {
            upstream_write.write_all(&group.raw).await?;
            upstream_write.flush().await?;
            break;
        }

        let order = arrived;
        arrived += 1;

        let ordinal = statements;
        statements += 1;

        let decision = context.decide(PointKind::Statement, connection, group.keyword());

        match decision {
            Decision::Reorder { .. } => {
                deferred.push(Deferred {
                    order,
                    group,
                    until: None,
                });
                continue;
            }
            Decision::Hold { until } => {
                // A hold naming a fork that has already been reached is
                // already satisfied, so it releases with the next group rather
                // than waiting for something that cannot happen again.
                let until = (until > ordinal).then_some(until);

                deferred.push(Deferred {
                    order,
                    group,
                    until,
                });
                continue;
            }
            // Never written upstream and never observed, so the server is not
            // asked to answer something it was never sent. The client is left
            // waiting, which is what a lost statement does.
            Decision::Drop => {
                tracing::debug!(%connection, "statement dropped by the schedule");
                continue;
            }
            Decision::CloseConnection => {
                tracing::debug!(%connection, "connection closed by the schedule");
                return Ok(());
            }
            Decision::Deliver { .. } | Decision::Corrupt { .. } => {}
        }

        let mut batch = vec![(order, group, decision)];
        release(&mut deferred, ordinal, &mut batch);

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

    // The client stopped sending, so no later statement is coming to overtake
    // a deferred one, and no later fork is coming to release a held one. Sent
    // rather than discarded: the schedule said to hold this, not to lose it.
    if !deferred.is_empty() {
        let batch = std::iter::from_fn(|| deferred.pop())
            .map(|held| (held.order, held.group, Decision::NEUTRAL))
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

/// Moves every deferred group whose wait is over onto the end of the batch.
///
/// Most recently deferred first. `Reorder` always names the fork immediately
/// after itself, so releasing in reverse is what "let the next one go first"
/// composes to when it happens twice.
///
/// The batch is served in order and each group is answered before the next is
/// written, so a group appended here is sent after the one it was waiting on
/// has completed, which is what `Hold` means.
fn release(deferred: &mut Vec<Deferred>, ordinal: u64, batch: &mut Vec<(u64, Group, Decision)>) {
    let mut index = deferred.len();

    while index > 0 {
        index -= 1;

        let ready = match deferred[index].until {
            None => true,
            Some(until) => until <= ordinal,
        };

        if ready {
            let held = deferred.remove(index);
            batch.push((held.order, held.group, Decision::NEUTRAL));
        }
    }
}

/// Forwards a batch of statement groups, then answers in the order they were
/// asked.
///
/// Returns whether the connection survived.
///
/// Postgres answers in the order it was sent, which is the reordered order.
/// Writing those answers straight back would leave a pipelining client matching
/// every answer to the wrong statement, so they are restored to the client's
/// order first. The server still saw the ordering the scheduler chose, which is
/// the whole object of the exercise.
///
/// Each group is answered before the next is written. Two answers in flight
/// would complete in an order this adapter does not control, and an adapter
/// that let the runtime pick would have put nondeterminism somewhere the trace
/// cannot describe.
async fn exchange(
    context: &ProxyContext,
    connection: ConnectionId,
    batch: Vec<(u64, Group, Decision)>,
    upstream_write: &mut OwnedWriteHalf,
    upstream_read: &mut BufReader<OwnedReadHalf>,
    client_write: &mut OwnedWriteHalf,
) -> Result<bool> {
    let mut answers = Vec::with_capacity(batch.len());
    let mut alive = true;

    for (order, group, decision) in batch {
        if let Decision::Deliver { delay } = decision
            && !delay.is_zero()
        {
            tokio::time::sleep(delay).await;
        }

        let mut encoded = group.raw.clone();

        if let Decision::Corrupt { offset } = decision {
            corrupt(&mut encoded, offset);
        }

        upstream_write.write_all(&encoded).await?;
        upstream_write.flush().await?;

        for event in group.events() {
            context.observe(connection, Event::Postgres(event));
        }

        let answer = read_answer(upstream_read).await?;

        for failure in &answer.errors {
            context.observe(
                connection,
                Event::Postgres(PostgresEvent::Error {
                    code: failure.code.clone(),
                    message: failure.message.clone(),
                }),
            );
        }

        // The server ended the session mid-answer, which is what it does to a
        // frame it could not parse. Whatever arrived still goes back, so the
        // client sees the `ErrorResponse` rather than a bare disconnect, and
        // nothing further is written to a socket that is gone.
        alive &= answer.complete;

        answers.push((order, answer));

        if !alive {
            break;
        }
    }

    answers.sort_by_key(|(order, _)| *order);

    for (_, answer) in answers {
        let decision = context.decide(
            PointKind::Response,
            connection,
            if answer.errors.is_empty() {
                "answer"
            } else {
                "error"
            },
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
                    "the schedule answered a postgres response fork with {decision}, which no \
                     postgres response fork can carry out"
                )));
            }
        }

        let mut bytes = answer.raw;

        if let Decision::Corrupt { offset } = decision {
            corrupt(&mut bytes, offset);
        }

        client_write.write_all(&bytes).await?;
        client_write.flush().await?;
    }

    Ok(alive)
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

/// What the client asked for at startup.
struct Startup {
    database: String,
}

/// Relays the startup exchange and reports what the session is for.
///
/// `None` means the connection is over before any statement: a cancel request,
/// which is a one-shot packet on a connection of its own, or a server that
/// refused the session.
///
/// Authentication is forwarded byte for byte, SCRAM included. Both sides of
/// this proxy are plaintext, so there is no channel for a channel binding to
/// disagree about.
async fn handshake(
    client_read: &mut BufReader<OwnedReadHalf>,
    client_write: &mut OwnedWriteHalf,
    upstream_read: &mut BufReader<OwnedReadHalf>,
    upstream_write: &mut OwnedWriteHalf,
) -> Result<Option<Startup>> {
    let startup = loop {
        let Some(packet) = read_startup(client_read).await? else {
            return Ok(None);
        };

        match packet.code {
            // Refused rather than forwarded. Saying `S` would hand the rest of
            // the session to TLS, and a proxy watching an opaque stream can
            // make none of the decisions this tool exists to make. Clients
            // configured to prefer TLS fall back to plaintext on `N`.
            SSL_REQUEST | GSSENC_REQUEST => {
                client_write.write_all(b"N").await?;
                client_write.flush().await?;
            }

            // Its own connection, carrying a backend key rather than a
            // session. Forwarded and done: there is nothing here to fork on and
            // no answer to wait for.
            CANCEL_REQUEST => {
                upstream_write.write_all(&packet.raw).await?;
                upstream_write.flush().await?;

                return Ok(None);
            }

            _ => break packet,
        }
    };

    upstream_write.write_all(&startup.raw).await?;
    upstream_write.flush().await?;

    let database = parameters(&startup);

    if !authenticate(client_read, client_write, upstream_read, upstream_write).await? {
        return Ok(None);
    }

    Ok(Some(Startup { database }))
}

/// Relays authentication until the server is ready, and says whether it got
/// there.
async fn authenticate(
    client_read: &mut BufReader<OwnedReadHalf>,
    client_write: &mut OwnedWriteHalf,
    upstream_read: &mut BufReader<OwnedReadHalf>,
    upstream_write: &mut OwnedWriteHalf,
) -> Result<bool> {
    loop {
        let Some(message) = read_message(upstream_read).await? else {
            return Ok(false);
        };

        let tag = message.tag();

        client_write.write_all(&message.raw).await?;
        client_write.flush().await?;

        if tag == b'Z' {
            return Ok(true);
        }

        if tag != b'R' {
            continue;
        }

        let body = message.body();

        if body.len() < 4 {
            return Err(Error::protocol(
                PROTOCOL,
                "an authentication message carried no method",
            ));
        }

        let method = i32::from_be_bytes([body[0], body[1], body[2], body[3]]);

        // Listed rather than "anything but Ok", because `SASLFinal` is the one
        // that ends a round without asking for anything: waiting for a client
        // message after it would hang the connection on every SCRAM login,
        // which is every login against a default Postgres.
        let answers = matches!(method, 2 | 3 | 5 | 7 | 8 | 9 | 10 | 11);

        if !answers {
            continue;
        }

        let Some(answer) = read_message(client_read).await? else {
            return Ok(false);
        };

        upstream_write.write_all(&answer.raw).await?;
        upstream_write.flush().await?;
    }
}

/// The database this session is for.
///
/// Postgres defaults it to the user name when the parameter is absent, and so
/// does this: the event says which database the statements ran against, and
/// reporting an empty one would be a report that needs a second lookup to read.
fn parameters(packet: &Packet) -> String {
    let mut user = String::new();
    let mut database = String::new();

    if packet.code != VERSION_3 {
        return database;
    }

    let mut at = 4;

    while at < packet.body().len() {
        let Some((key, next)) = cstr(packet.body(), at) else {
            break;
        };

        if key.is_empty() {
            break;
        }

        let Some((value, next)) = cstr(packet.body(), next) else {
            break;
        };

        match key.as_str() {
            "user" => user = value,
            "database" => database = value,
            _ => {}
        }

        at = next;
    }

    if database.is_empty() { user } else { database }
}

/// What a connection has prepared, so a `Bind` can report what it runs.
#[derive(Debug, Default)]
struct Session {
    prepared: HashMap<String, String>,
    portals: HashMap<String, String>,
}

impl Session {
    fn remember(map: &mut HashMap<String, String>, name: String, sql: String) {
        if map.len() >= MAX_PREPARED && !map.contains_key(&name) {
            return;
        }

        map.insert(name, sql);
    }
}

/// One statement group: every frontend message up to the one that asks the
/// server to answer.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Group {
    raw: Vec<u8>,
    sql: Option<String>,
    terminate: bool,
}

impl Group {
    /// What goes in the trace: the leading keyword and nothing else.
    fn keyword(&self) -> String {
        let Some(sql) = &self.sql else {
            return "statement".to_string();
        };

        sql.split_whitespace()
            .next()
            .map(str::to_uppercase)
            .unwrap_or_else(|| "statement".to_string())
    }

    /// What goes in the report.
    ///
    /// A transaction boundary is reported as the boundary rather than as one
    /// more statement, because that is the whole of what it does and the
    /// invariants are written against it. Everything else carries its text.
    fn events(&self) -> Vec<PostgresEvent> {
        let Some(sql) = &self.sql else {
            return Vec::new();
        };

        match transaction(sql) {
            Some(event) => vec![event],
            None => vec![PostgresEvent::Statement { sql: sql.clone() }],
        }
    }
}

/// Which transaction boundary a statement is, if it is one.
///
/// The leading keyword, after whitespace. Deliberately no more than that: a
/// parser that went looking for a `COMMIT` inside a comment or a string literal
/// would be a SQL parser, and one that got it wrong would report a transaction
/// boundary that never happened.
fn transaction(sql: &str) -> Option<PostgresEvent> {
    let keyword = sql.split_whitespace().next()?.to_uppercase();

    match keyword.trim_end_matches(';') {
        "BEGIN" | "START" => Some(PostgresEvent::Begin),
        // `END` is `COMMIT` as a statement, and the service asking for either
        // is the event. Whether the server turned it into a rollback is what
        // `no_commit_after_error` exists to notice.
        "COMMIT" | "END" => Some(PostgresEvent::Commit),
        "ROLLBACK" | "ABORT" => Some(PostgresEvent::Rollback),
        _ => None,
    }
}

/// Reads one statement group, or `None` at end of stream.
async fn read_group(
    read: &mut BufReader<OwnedReadHalf>,
    session: &mut Session,
) -> Result<Option<Group>> {
    let mut raw = Vec::new();
    let mut sql = None;

    loop {
        let Some(message) = read_message(read).await? else {
            if raw.is_empty() {
                return Ok(None);
            }

            return Err(Error::protocol(
                PROTOCOL,
                "the connection ended part way through a statement",
            ));
        };

        let tag = message.tag();

        if raw.len().saturating_add(message.raw.len()) > MAX_GROUP {
            return Err(Error::protocol(
                PROTOCOL,
                format!("a statement group went past {MAX_GROUP} bytes without a Sync"),
            ));
        }

        match tag {
            // A simple query is the whole group.
            b'Q' => {
                sql = cstr(message.body(), 0).map(|(text, _)| text);
                raw.extend_from_slice(&message.raw);

                return Ok(Some(Group {
                    raw,
                    sql,
                    terminate: false,
                }));
            }

            b'X' => {
                raw.extend_from_slice(&message.raw);

                return Ok(Some(Group {
                    raw,
                    sql,
                    terminate: true,
                }));
            }

            // Sync ends an extended-query group and is what the server answers.
            b'S' => {
                raw.extend_from_slice(&message.raw);

                return Ok(Some(Group {
                    raw,
                    sql,
                    terminate: false,
                }));
            }

            b'P' => {
                if let Some((name, next)) = cstr(message.body(), 0)
                    && let Some((query, _)) = cstr(message.body(), next)
                {
                    sql.get_or_insert_with(|| query.clone());
                    Session::remember(&mut session.prepared, name, query);
                }

                raw.extend_from_slice(&message.raw);
            }

            b'B' => {
                if let Some((portal, next)) = cstr(message.body(), 0)
                    && let Some((statement, _)) = cstr(message.body(), next)
                    && let Some(known) = session.prepared.get(&statement).cloned()
                {
                    sql.get_or_insert_with(|| known.clone());
                    Session::remember(&mut session.portals, portal, known);
                }

                raw.extend_from_slice(&message.raw);
            }

            b'E' => {
                if let Some((portal, _)) = cstr(message.body(), 0)
                    && let Some(known) = session.portals.get(&portal)
                {
                    let known = known.clone();
                    sql.get_or_insert(known);
                }

                raw.extend_from_slice(&message.raw);
            }

            // Closing a statement or portal forgets it, which is also what
            // keeps the map from growing for the life of a pooled connection.
            b'C' => {
                if message.body().len() > 1
                    && let Some((name, _)) = cstr(message.body(), 1)
                {
                    match message.body()[0] {
                        b'S' => session.prepared.remove(&name),
                        b'P' => session.portals.remove(&name),
                        _ => None,
                    };
                }

                raw.extend_from_slice(&message.raw);
            }

            b'D' | b'p' => raw.extend_from_slice(&message.raw),

            b'H' => {
                return Err(Error::Unsupported(
                    "the postgres adapter does not proxy pipelining by `Flush` yet: it asks the \
                     server for output without ending the statement, so there is no boundary to \
                     decide at"
                        .to_string(),
                ));
            }

            other => {
                return Err(Error::protocol(
                    PROTOCOL,
                    format!(
                        "a frontend message tagged `{}` arrived where a statement was expected",
                        other as char
                    ),
                ));
            }
        }
    }
}

/// An `ErrorResponse`, reduced to the two fields anything reads.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Failure {
    code: String,
    message: String,
}

/// Everything the server sent in answer to one statement group.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Answer {
    raw: Vec<u8>,
    errors: Vec<Failure>,
    /// Whether the answer ended with `ReadyForQuery` rather than a closed
    /// socket. A corrupted frame is answered with an error and a disconnect,
    /// and that is a run the schedule asked for rather than a harness failure.
    complete: bool,
}

/// Reads one complete answer: every message up to and including
/// `ReadyForQuery`.
async fn read_answer(read: &mut BufReader<OwnedReadHalf>) -> Result<Answer> {
    let mut raw = Vec::new();
    let mut errors = Vec::new();

    loop {
        let Some(message) = read_message(read).await? else {
            return Ok(Answer {
                raw,
                errors,
                complete: false,
            });
        };

        let tag = message.tag();

        match tag {
            b'E' => errors.push(failure(message.body())),

            // Nothing asked for this. Refused rather than forwarded, because
            // every answer this adapter reads is paired with the statement
            // that caused it, and a notification has no statement: forwarding
            // one would shift every later answer onto the wrong statement and
            // turn the invariants into a random number generator.
            b'A' => {
                return Err(Error::Unsupported(
                    "the postgres adapter does not proxy `LISTEN`/`NOTIFY` yet: the server then \
                     sends messages no statement asked for, and this adapter pairs every answer \
                     with the statement that caused it"
                        .to_string(),
                ));
            }

            // CopyInResponse, CopyOutResponse, CopyBothResponse. The connection
            // switches to a stream with no statement boundaries in it.
            b'G' | b'H' | b'W' => {
                return Err(Error::Unsupported(
                    "the postgres adapter does not proxy `COPY` yet: it switches the connection \
                     into a stream with no statement boundaries to decide at"
                        .to_string(),
                ));
            }

            _ => {}
        }

        raw.extend_from_slice(&message.raw);

        if tag == b'Z' {
            return Ok(Answer {
                raw,
                errors,
                complete: true,
            });
        }
    }
}

/// Pulls the SQLSTATE and the message out of an `ErrorResponse` body.
///
/// A sequence of typed, NUL-terminated fields ending in a zero byte. Unknown
/// field types are skipped rather than refused: the set has grown between
/// server versions, and an adapter that insisted on knowing all of them would
/// break against a newer Postgres for no reason.
fn failure(body: &[u8]) -> Failure {
    let mut code = String::new();
    let mut message = String::new();
    let mut at = 0;

    while at < body.len() {
        let kind = body[at];

        if kind == 0 {
            break;
        }

        let Some((value, next)) = cstr(body, at + 1) else {
            break;
        };

        match kind {
            b'C' => code = value,
            b'M' => message = value,
            _ => {}
        }

        at = next;
    }

    Failure { code, message }
}

/// One message with its tag and length still on the front, ready to forward.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Message {
    raw: Vec<u8>,
}

impl Message {
    fn tag(&self) -> u8 {
        self.raw[0]
    }

    fn body(&self) -> &[u8] {
        &self.raw[5..]
    }
}

/// Reads one tagged message, or `None` at end of stream.
async fn read_message(read: &mut BufReader<OwnedReadHalf>) -> Result<Option<Message>> {
    let mut tag = [0u8; 1];

    match read.read_exact(&mut tag).await {
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(error) => return Err(Error::Io(error)),
    }

    let mut length = [0u8; 4];
    read.read_exact(&mut length).await?;

    let declared = i32::from_be_bytes(length);
    let body = usize::try_from(declared)
        .ok()
        .and_then(|declared| declared.checked_sub(4))
        .filter(|body| *body <= MAX_MESSAGE)
        .ok_or_else(|| {
            Error::protocol(
                PROTOCOL,
                format!(
                    "a message tagged `{}` declared {declared} bytes, which is not a length this \
                     adapter will buffer",
                    tag[0] as char
                ),
            )
        })?;

    let mut raw = vec![0u8; 5 + body];
    raw[0] = tag[0];
    raw[1..5].copy_from_slice(&length);
    read.read_exact(&mut raw[5..]).await?;

    Ok(Some(Message { raw }))
}

/// A startup packet, which carries a length and no tag.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Packet {
    raw: Vec<u8>,
    code: i32,
}

impl Packet {
    fn body(&self) -> &[u8] {
        &self.raw[4..]
    }
}

/// Reads one startup packet, or `None` at end of stream.
async fn read_startup(read: &mut BufReader<OwnedReadHalf>) -> Result<Option<Packet>> {
    let mut length = [0u8; 4];

    match read.read_exact(&mut length).await {
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(error) => return Err(Error::Io(error)),
    }

    let declared = i32::from_be_bytes(length);
    let body = usize::try_from(declared)
        .ok()
        .and_then(|declared| declared.checked_sub(4))
        .filter(|body| *body >= 4 && *body <= MAX_STARTUP)
        .ok_or_else(|| {
            Error::protocol(
                PROTOCOL,
                format!("a startup packet declared {declared} bytes"),
            )
        })?;

    let mut raw = vec![0u8; 4 + body];
    raw[..4].copy_from_slice(&length);
    read.read_exact(&mut raw[4..]).await?;

    let code = i32::from_be_bytes([raw[4], raw[5], raw[6], raw[7]]);

    Ok(Some(Packet { raw, code }))
}

/// Reads one NUL-terminated string, and where the next one starts.
///
/// Lossy rather than refusing: a client is free to put anything in a statement,
/// and a harness that failed the run over a byte sequence the server is about
/// to accept would be reporting its own strictness as a finding.
fn cstr(body: &[u8], from: usize) -> Option<(String, usize)> {
    let rest = body.get(from..)?;
    let end = rest.iter().position(|byte| *byte == 0)?;

    Some((
        String::from_utf8_lossy(&rest[..end]).into_owned(),
        from + end + 1,
    ))
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

    /// One message, tag and length included.
    fn message(tag: u8, body: &[u8]) -> Vec<u8> {
        let mut out = vec![tag];

        out.extend_from_slice(
            &i32::try_from(body.len() + 4)
                .expect("a test message fits")
                .to_be_bytes(),
        );
        out.extend_from_slice(body);

        out
    }

    fn cstring(text: &str) -> Vec<u8> {
        let mut out = text.as_bytes().to_vec();
        out.push(0);

        out
    }

    /// A `StartupMessage` for one database.
    fn startup(database: &str) -> Vec<u8> {
        let mut body = VERSION_3.to_be_bytes().to_vec();

        body.extend_from_slice(&cstring("user"));
        body.extend_from_slice(&cstring("misorder"));
        body.extend_from_slice(&cstring("database"));
        body.extend_from_slice(&cstring(database));
        body.push(0);

        let mut out = i32::try_from(body.len() + 4)
            .expect("a startup packet fits")
            .to_be_bytes()
            .to_vec();
        out.extend_from_slice(&body);

        out
    }

    fn simple_query(sql: &str) -> Vec<u8> {
        message(b'Q', &cstring(sql))
    }

    /// `Parse`/`Bind`/`Execute`/`Sync`, which is one statement in four
    /// messages and the shape any client using prepared statements sends.
    fn extended(name: &str, sql: &str) -> Vec<u8> {
        let mut parse = cstring(name);
        parse.extend_from_slice(&cstring(sql));
        parse.extend_from_slice(&0i16.to_be_bytes());

        let mut bind = cstring("");
        bind.extend_from_slice(&cstring(name));
        bind.extend_from_slice(&0i16.to_be_bytes());
        bind.extend_from_slice(&0i16.to_be_bytes());
        bind.extend_from_slice(&0i16.to_be_bytes());

        let mut execute = cstring("");
        execute.extend_from_slice(&0i32.to_be_bytes());

        let mut out = message(b'P', &parse);
        out.extend_from_slice(&message(b'B', &bind));
        out.extend_from_slice(&message(b'E', &execute));
        out.extend_from_slice(&message(b'S', &[]));

        out
    }

    /// How the fake server should answer one statement.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum Answers {
        /// `CommandComplete` and an idle `ReadyForQuery`.
        Ok,
        /// A serialization failure, then a failed-transaction
        /// `ReadyForQuery`.
        Serialization,
        /// What Postgres really does with a `COMMIT` in a failed transaction:
        /// reports the tag `ROLLBACK` and goes idle.
        RolledBack,
        /// A `NotificationResponse`, which no statement asked for.
        Notifies,
    }

    fn answer(kind: Answers) -> Vec<u8> {
        match kind {
            Answers::Ok => {
                let mut out = message(b'C', &cstring("SELECT 1"));
                out.extend_from_slice(&message(b'Z', b"I"));

                out
            }
            Answers::Serialization => {
                let mut body = vec![b'C'];
                body.extend_from_slice(&cstring("40001"));
                body.push(b'M');
                body.extend_from_slice(&cstring("could not serialize access"));
                body.push(0);

                let mut out = message(b'E', &body);
                out.extend_from_slice(&message(b'Z', b"E"));

                out
            }
            Answers::RolledBack => {
                let mut out = message(b'C', &cstring("ROLLBACK"));
                out.extend_from_slice(&message(b'Z', b"I"));

                out
            }
            Answers::Notifies => {
                let mut body = 42i32.to_be_bytes().to_vec();
                body.extend_from_slice(&cstring("ledger"));
                body.extend_from_slice(&cstring(""));

                message(b'A', &body)
            }
        }
    }

    /// A Postgres that authenticates anybody and answers every statement.
    ///
    /// Enough to test ordering, pairing and events, which is what this adapter
    /// decides. Real semantics are the container's job, and a fake that tried
    /// to have them would be a second implementation of Postgres to keep
    /// correct.
    async fn server(listener: TcpListener, seen: Arc<Mutex<Vec<String>>>, answers: Vec<Answers>) {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };

            let seen = Arc::clone(&seen);
            let answers = answers.clone();

            tokio::spawn(async move {
                let (read, mut write) = stream.into_split();
                let mut read = BufReader::new(read);

                // The startup packet, which carries no tag.
                let mut length = [0u8; 4];

                if read.read_exact(&mut length).await.is_err() {
                    return;
                }

                let declared = i32::from_be_bytes(length) as usize;
                let mut body = vec![0u8; declared - 4];

                if read.read_exact(&mut body).await.is_err() {
                    return;
                }

                let mut ready = message(b'R', &0i32.to_be_bytes());
                ready.extend_from_slice(&message(b'Z', b"I"));

                if write.write_all(&ready).await.is_err() {
                    return;
                }

                let mut statements = 0;

                while let Ok(Some(frame)) = read_message(&mut read).await {
                    let tag = frame.tag();

                    match tag {
                        b'Q' => {
                            if let Some((sql, _)) = cstr(frame.body(), 0) {
                                seen.lock().expect("seen").push(sql);
                            }
                        }
                        b'P' => {
                            if let Some((_, next)) = cstr(frame.body(), 0)
                                && let Some((sql, _)) = cstr(frame.body(), next)
                            {
                                seen.lock().expect("seen").push(sql);
                            }
                        }
                        b'X' => return,
                        _ => {}
                    }

                    if tag != b'Q' && tag != b'S' {
                        continue;
                    }

                    let kind = answers.get(statements).copied().unwrap_or(Answers::Ok);
                    statements += 1;

                    if write.write_all(&answer(kind)).await.is_err() {
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
        readiness: crate::proxy::Readiness,
        cancel: CancellationToken,
        serving: tokio::task::JoinHandle<Result<()>>,
    }

    impl Harness {
        async fn start(source: Arc<dyn DecisionSource>) -> Self {
            Self::answering(source, Vec::new()).await
        }

        async fn answering(source: Arc<dyn DecisionSource>, answers: Vec<Answers>) -> Self {
            let listener = TcpListener::bind(("127.0.0.1", 0))
                .await
                .expect("bind the server");
            let upstream = listener.local_addr().expect("address").to_string();

            let seen = Arc::new(Mutex::new(Vec::new()));

            tokio::spawn(server(listener, Arc::clone(&seen), answers));

            let mut adapter = PostgresAdapter::new("ledger", "misorder", "misorder");
            let endpoint = adapter.bind(&upstream).await.expect("bind the proxy");

            let (events, receiver) = EventSink::new();
            let cancel = CancellationToken::new();
            let readiness = crate::proxy::Readiness::new();
            let scheduler = Scheduler::new(source, Recorder::new(0, "postgres_test"));
            let context = ProxyContext::new(scheduler, upstream, events, cancel.clone())
                .with_readiness(readiness.clone());

            let serving = tokio::spawn(async move { adapter.serve(context).await });

            Self {
                proxy: endpoint.listen,
                seen,
                events: receiver,
                readiness,
                cancel,
                serving,
            }
        }

        /// Opens a session and leaves it at `ReadyForQuery`.
        async fn session(&self) -> (BufReader<OwnedReadHalf>, OwnedWriteHalf) {
            let stream = TcpStream::connect(self.proxy)
                .await
                .expect("reach the proxy");
            let (read, mut write) = stream.into_split();
            let mut read = BufReader::new(read);

            write.write_all(&startup("ledger")).await.expect("startup");
            write.flush().await.expect("flush");

            loop {
                let frame = read_message(&mut read)
                    .await
                    .expect("read")
                    .expect("the server answers a startup");

                if frame.tag() == b'Z' {
                    return (read, write);
                }
            }
        }

        /// Sends every statement without waiting, then half-closes.
        ///
        /// The half-close is what releases a statement the schedule deferred
        /// behind one that never arrived.
        async fn pipeline(&self, statements: &[&str]) -> Vec<u8> {
            let (mut read, mut write) = self.session().await;

            for sql in statements {
                write.write_all(&simple_query(sql)).await.expect("write");
            }

            write.flush().await.expect("flush");
            drop(write);

            let mut answers = Vec::new();
            let _ = read.read_to_end(&mut answers).await;

            answers
        }

        fn statements(&self) -> Vec<String> {
            self.seen.lock().expect("seen").clone()
        }

        async fn events(&mut self) -> Vec<Event> {
            self.cancel.cancel();
            let _ = (&mut self.serving).await;

            let mut collected = Vec::new();

            while let Ok(observed) = self.events.try_recv() {
                collected.push(observed.event);
            }

            collected
        }

        async fn finish(self) -> Result<()> {
            self.cancel.cancel();

            self.serving.await.expect("the adapter task")
        }
    }

    fn postgres_events(events: &[Event]) -> Vec<&PostgresEvent> {
        events
            .iter()
            .filter_map(|event| match event {
                Event::Postgres(event) => Some(event),
                _ => None,
            })
            .collect()
    }

    #[tokio::test]
    async fn a_statement_reaches_the_server_and_its_answer_comes_back() {
        let mut harness = Harness::start(At::nothing()).await;

        let answers = harness.pipeline(&["SELECT 1"]).await;

        assert_eq!(harness.statements(), vec!["SELECT 1".to_string()]);
        assert!(
            !answers.is_empty(),
            "the client is answered rather than left waiting"
        );

        let events = harness.events().await;
        let events = postgres_events(&events);

        assert!(
            events.contains(&&PostgresEvent::Connected {
                database: "ledger".to_string()
            }),
            "the session reports which database it is for: {events:?}"
        );
        assert!(
            events.contains(&&PostgresEvent::Statement {
                sql: "SELECT 1".to_string()
            }),
            "{events:?}"
        );
    }

    /// The fork is per statement, not per message. Four forks in front of one
    /// `SELECT` would let a schedule drop a `Bind` and deliver its `Execute`,
    /// which is not a fault a real system can have.
    #[tokio::test]
    async fn an_extended_query_is_one_fork_and_reports_the_text_it_parsed() {
        let harness = Harness::start(At::nothing()).await;

        let (mut read, mut write) = harness.session().await;

        write
            .write_all(&extended("s1", "INSERT INTO ledger VALUES (1)"))
            .await
            .expect("write");
        write.flush().await.expect("flush");
        drop(write);

        let mut answers = Vec::new();
        let _ = read.read_to_end(&mut answers).await;

        assert_eq!(
            harness.statements(),
            vec!["INSERT INTO ledger VALUES (1)".to_string()]
        );

        let mut harness = harness;
        let events = harness.events().await;

        assert!(
            postgres_events(&events).contains(&&PostgresEvent::Statement {
                sql: "INSERT INTO ledger VALUES (1)".to_string()
            }),
            "{events:?}"
        );
    }

    #[tokio::test]
    async fn a_statement_fork_is_taken_once_per_group() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.expect("bind");
        let upstream = listener.local_addr().expect("address").to_string();

        tokio::spawn(server(
            listener,
            Arc::new(Mutex::new(Vec::new())),
            Vec::new(),
        ));

        let mut adapter = PostgresAdapter::new("ledger", "misorder", "misorder");
        let endpoint = adapter.bind(&upstream).await.expect("bind the proxy");

        let (events, _receiver) = EventSink::new();
        let cancel = CancellationToken::new();
        let scheduler = Scheduler::new(At::nothing(), Recorder::new(0, "postgres_test"));
        let recorder = scheduler.recorder().clone();
        let context = ProxyContext::new(scheduler, upstream, events, cancel.clone());

        let serving = tokio::spawn(async move { adapter.serve(context).await });

        let stream = TcpStream::connect(endpoint.listen).await.expect("connect");
        let (read, mut write) = stream.into_split();
        let mut read = BufReader::new(read);

        write.write_all(&startup("ledger")).await.expect("startup");
        write
            .write_all(&extended("s1", "SELECT 1"))
            .await
            .expect("write");
        write.flush().await.expect("flush");
        drop(write);

        let mut answers = Vec::new();
        let _ = read.read_to_end(&mut answers).await;

        cancel.cancel();
        let _ = serving.await;

        let statements = recorder
            .snapshot()
            .records
            .into_iter()
            .filter(|record| record.point.key.kind == PointKind::Statement)
            .count();

        assert_eq!(
            statements, 1,
            "Parse, Bind, Execute and Sync are one statement"
        );
    }

    /// The fault this adapter exists for, in its cheapest form: the server saw
    /// the second statement first, and the client still got its answers in the
    /// order it asked.
    #[tokio::test]
    async fn the_schedule_can_let_a_later_statement_go_first() {
        let harness = Harness::start(At::once(
            PointKind::Statement,
            0,
            Decision::Reorder { ahead_of: 1 },
        ))
        .await;

        harness.pipeline(&["FIRST", "SECOND"]).await;

        assert_eq!(
            harness.statements(),
            vec!["SECOND".to_string(), "FIRST".to_string()],
            "the server saw the ordering the schedule chose"
        );
    }

    /// A hold names the fork it waits for, and the statement goes out after
    /// that fork has been answered rather than merely sent.
    #[tokio::test]
    async fn a_held_statement_waits_for_the_fork_it_names() {
        let harness = Harness::start(At::once(
            PointKind::Statement,
            0,
            Decision::Hold { until: 1 },
        ))
        .await;

        harness.pipeline(&["HELD", "OTHER"]).await;

        assert_eq!(
            harness.statements(),
            vec!["OTHER".to_string(), "HELD".to_string()]
        );
    }

    /// A hold whose fork never happens still sends the statement when the
    /// client stops talking. The schedule said to hold it, not to lose it, and
    /// a statement quietly dropped would be a recorded fault the run did not
    /// have.
    #[tokio::test]
    async fn a_hold_that_is_never_released_goes_out_at_the_end() {
        let harness = Harness::start(At::once(
            PointKind::Statement,
            0,
            Decision::Hold { until: 9 },
        ))
        .await;

        harness.pipeline(&["ONLY"]).await;

        assert_eq!(harness.statements(), vec!["ONLY".to_string()]);
    }

    #[tokio::test]
    async fn a_dropped_statement_never_reaches_the_server() {
        let harness = Harness::start(At::once(PointKind::Statement, 0, Decision::Drop)).await;

        harness.pipeline(&["LOST", "KEPT"]).await;

        assert_eq!(
            harness.statements(),
            vec!["KEPT".to_string()],
            "a dropped statement is never sent, so the server never answers it"
        );
    }

    #[tokio::test]
    async fn an_answer_the_schedule_drops_never_reaches_the_client() {
        let harness = Harness::start(At::once(PointKind::Response, 0, Decision::Drop)).await;

        let answers = harness.pipeline(&["SELECT 1"]).await;

        assert_eq!(
            harness.statements(),
            vec!["SELECT 1".to_string()],
            "the statement still ran; it is the answer that was lost"
        );
        assert!(
            answers.is_empty(),
            "the client is left waiting, which is what a lost answer does"
        );
    }

    #[tokio::test]
    async fn a_delayed_answer_still_arrives() {
        let harness = Harness::start(At::once(
            PointKind::Response,
            0,
            Decision::Deliver {
                delay: Duration::from_millis(20),
            },
        ))
        .await;

        let answers = harness.pipeline(&["SELECT 1"]).await;

        assert!(!answers.is_empty(), "a delay is not a loss");
    }

    #[tokio::test]
    async fn the_schedule_can_refuse_a_connection_before_it_starts() {
        let harness = Harness::start(At::once(
            PointKind::Connection,
            0,
            Decision::CloseConnection,
        ))
        .await;

        let stream = TcpStream::connect(harness.proxy).await.expect("connect");
        let (read, mut write) = stream.into_split();
        let mut read = BufReader::new(read);

        let _ = write.write_all(&startup("ledger")).await;
        let _ = write.flush().await;

        let mut answered = Vec::new();
        let _ = read.read_to_end(&mut answered).await;

        assert!(answered.is_empty(), "a refused connection answers nothing");
        assert!(harness.statements().is_empty());
    }

    /// The event that makes `no_commit_after_error` mean anything. Postgres
    /// answers a `COMMIT` inside a failed transaction with the tag `ROLLBACK`,
    /// so an adapter that reported what the server did would report no commit
    /// at all, and the invariant could never fire.
    #[tokio::test]
    async fn a_commit_is_reported_as_the_client_asked_for_it() {
        let mut harness = Harness::answering(
            At::nothing(),
            vec![Answers::Ok, Answers::Serialization, Answers::RolledBack],
        )
        .await;

        harness
            .pipeline(&["BEGIN", "UPDATE ledger SET n = 1", "COMMIT"])
            .await;

        let events = harness.events().await;
        let events = postgres_events(&events);

        assert!(events.contains(&&PostgresEvent::Begin), "{events:?}");
        assert!(
            events.contains(&&PostgresEvent::Error {
                code: "40001".to_string(),
                message: "could not serialize access".to_string(),
            }),
            "the SQLSTATE is read off the ErrorResponse: {events:?}"
        );
        assert!(
            events.contains(&&PostgresEvent::Commit),
            "the service believes it committed, which is the whole finding: {events:?}"
        );
    }

    /// The failure this closes: an adapter that forwarded a notification would
    /// pair it with whatever statement came next, and every later answer on the
    /// connection would be attributed to the wrong one.
    #[tokio::test]
    async fn a_notification_is_refused_rather_than_mis_paired() {
        let harness = Harness::answering(At::nothing(), vec![Answers::Notifies]).await;

        harness.pipeline(&["LISTEN ledger"]).await;

        let error = harness
            .finish()
            .await
            .expect_err("a pushed message has no statement to pair with");

        assert!(
            matches!(error, Error::Unsupported(_)),
            "the scenario is not wrong, the feature is missing: {error}"
        );
    }

    /// Answering `S` would hand the rest of the session to TLS, and every
    /// decision this tool makes lives inside it.
    #[tokio::test]
    async fn an_ssl_request_is_refused_so_the_session_stays_readable() {
        let harness = Harness::start(At::nothing()).await;

        let stream = TcpStream::connect(harness.proxy).await.expect("connect");
        let (read, mut write) = stream.into_split();
        let mut read = BufReader::new(read);

        write
            .write_all(&[0, 0, 0, 8, 4, 210, 22, 47])
            .await
            .expect("write");
        write.flush().await.expect("flush");

        let mut answer = [0u8; 1];
        read.read_exact(&mut answer).await.expect("one byte back");

        assert_eq!(answer[0], b'N', "plaintext, and the client falls back");

        // And the session carries on from there, which is the half that would
        // be easy to get wrong: a refusal that ate the next packet would break
        // every client configured to prefer TLS.
        write.write_all(&startup("ledger")).await.expect("startup");
        write
            .write_all(&simple_query("AFTER"))
            .await
            .expect("write");
        write.flush().await.expect("flush");
        drop(write);

        let mut answers = Vec::new();
        let _ = read.read_to_end(&mut answers).await;

        assert_eq!(harness.statements(), vec!["AFTER".to_string()]);
    }

    /// A workload driven at a socket that is open but not yet serving is the
    /// harness measuring its own impatience, so the signal is the answered
    /// startup rather than the accepted connection.
    #[tokio::test]
    async fn readiness_is_reported_once_the_server_has_answered() {
        let harness = Harness::start(At::nothing()).await;

        assert!(
            !harness.readiness.signals().postgres_ready,
            "nothing has connected yet"
        );

        let _session = harness.session().await;

        tokio::time::timeout(
            Duration::from_secs(2),
            harness.readiness.wait(
                crate::scenario::file::Ready::PostgresConnected,
                Duration::from_secs(2),
            ),
        )
        .await
        .expect("the signal arrives")
        .expect("a session that reached ReadyForQuery is the signal");
    }

    #[test]
    fn a_transaction_boundary_is_recognised_by_its_leading_keyword() {
        assert_eq!(transaction("BEGIN"), Some(PostgresEvent::Begin));
        assert_eq!(transaction("  begin;"), Some(PostgresEvent::Begin));
        assert_eq!(transaction("START TRANSACTION"), Some(PostgresEvent::Begin));
        assert_eq!(transaction("COMMIT"), Some(PostgresEvent::Commit));
        assert_eq!(transaction("END"), Some(PostgresEvent::Commit));
        assert_eq!(transaction("ROLLBACK"), Some(PostgresEvent::Rollback));
        assert_eq!(transaction("SELECT 1"), None);
        assert_eq!(
            transaction("SELECT 'COMMIT'"),
            None,
            "the keyword has to lead, or every query mentioning one is a boundary"
        );
    }

    /// What goes in the trace is the keyword, because a trace is a document
    /// people attach to public issues and a literal in a statement is the
    /// user's production shape.
    #[test]
    fn the_fork_detail_is_the_keyword_and_not_the_statement() {
        let group = Group {
            raw: Vec::new(),
            sql: Some("INSERT INTO ledger VALUES ('4111111111111111')".to_string()),
            terminate: false,
        };

        assert_eq!(group.keyword(), "INSERT");
    }

    #[test]
    fn corrupting_changes_a_byte_rather_than_doing_nothing() {
        let mut bytes = vec![1, 2, 3];

        corrupt(&mut bytes, 7);

        assert_ne!(bytes, vec![1, 2, 3], "an offset past the end still lands");
    }

    #[test]
    fn an_error_response_gives_up_its_sqlstate_and_message() {
        let mut body = vec![b'S'];
        body.extend_from_slice(&cstring("ERROR"));
        body.push(b'C');
        body.extend_from_slice(&cstring("25P02"));
        body.push(b'M');
        body.extend_from_slice(&cstring("current transaction is aborted"));
        body.push(0);

        let failure = failure(&body);

        assert_eq!(failure.code, "25P02");
        assert_eq!(failure.message, "current transaction is aborted");
    }

    /// Unknown fields are skipped rather than refused: the set has grown
    /// between server versions, and an adapter that insisted on knowing all of
    /// them would break against a newer Postgres for no reason.
    #[test]
    fn an_error_response_with_fields_this_build_does_not_know_still_reads() {
        let mut body = vec![b'!'];
        body.extend_from_slice(&cstring("something new"));
        body.push(b'C');
        body.extend_from_slice(&cstring("40001"));
        body.push(0);

        assert_eq!(failure(&body).code, "40001");
    }

    fn deferred(order: u64, until: Option<u64>) -> Deferred {
        Deferred {
            order,
            group: Group {
                raw: Vec::new(),
                sql: Some(format!("SELECT {order}")),
                terminate: false,
            },
            until,
        }
    }

    #[test]
    fn a_reorder_releases_with_the_next_statement() {
        let mut held = vec![deferred(0, None)];
        let mut batch = Vec::new();

        release(&mut held, 1, &mut batch);

        assert_eq!(batch.len(), 1);
        assert!(held.is_empty());
    }

    /// Reverse order, because `Reorder` names the fork immediately after
    /// itself: two of them compose to "let the next one go first", twice.
    #[test]
    fn two_reorders_release_most_recently_deferred_first() {
        let mut held = vec![deferred(0, None), deferred(1, None)];
        let mut batch = Vec::new();

        release(&mut held, 2, &mut batch);

        assert_eq!(
            batch.iter().map(|(order, _, _)| *order).collect::<Vec<_>>(),
            vec![1, 0]
        );
    }

    #[test]
    fn a_hold_waits_for_the_fork_it_names_and_no_earlier_one() {
        let mut held = vec![deferred(0, Some(3))];
        let mut batch = Vec::new();

        release(&mut held, 1, &mut batch);
        assert!(batch.is_empty(), "fork 3 has not happened yet");

        release(&mut held, 3, &mut batch);
        assert_eq!(batch.len(), 1, "fork 3 has now been answered");
    }
}
