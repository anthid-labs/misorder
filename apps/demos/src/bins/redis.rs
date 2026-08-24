//! Two workers, one lock, and the release everybody writes first.
//!
//! The system under test for
//! [`examples/redis_naive_lock.toml`](../../../examples/redis_naive_lock.toml).
//! It exists to be run by misorder, not to be depended on.
//!
//! # What is wrong with it
//!
//! The lock is textbook: `SET key token NX PX ttl` to acquire. The release is
//! `DEL key`, which is the line Redis's own documentation tells you not to
//! write, and which is safe exactly as long as the lock never expires early.
//! The whole reason it has a TTL is that it can:
//!
//! 1. worker A takes the lock,
//! 2. A's work runs long — a GC pause, a slow dependency, a delayed reply —
//!    and the TTL expires,
//! 3. worker B finds the lock free and takes it,
//! 4. A finishes and sends `DEL`, releasing **B's** lock,
//! 5. anyone can now take it while B still believes it holds it, and two
//!    workers are in the same critical section.
//!
//! Step 2 is the only unusual part, and it is one delayed reply.
//!
//! # Why it needs a fault to go wrong
//!
//! Unperturbed, A finishes its work well inside the TTL, B never gets the
//! lock, and the run is clean. The bug needs A's work to outlast the lock,
//! which is what a delayed reply through the proxy does — so this is not a
//! service that is simply broken, it is one that is correct until the ordering
//! stops being the one it was written against.
//!
//! # Configuration
//!
//! `REDIS_URL`, which misorder sets to its proxy. Nothing else.

use std::time::Duration;

use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};

/// How long the lock is held for.
///
/// A production lock is seconds to minutes; this one is milliseconds, so that a
/// delay the schedule injects can outlast it without the run taking real
/// seconds. The shape of the bug is identical either way.
///
/// The value is bounded on both sides and both bounds are real. Below about
/// 130ms the unperturbed run already loses the lock — process start, two
/// connections through the proxy and four round trips are not free — and a
/// scenario whose baseline fails is testing nothing. Above 250ms no single
/// injected delay can outlast it, because that is the profile's ceiling, and
/// the bug would need two delays to stack.
const TTL: Duration = Duration::from_millis(150);

/// One Redis connection, speaking just enough RESP to take a lock.
///
/// Hand-rolled for the same reason the rest of this repository's demos are:
/// nothing here should need a dependency to be understood, and the point of the
/// example is the ordering rather than the client.
struct Redis {
    write: OwnedWriteHalf,
    read: BufReader<OwnedReadHalf>,
}

impl Redis {
    async fn connect(address: &str) -> std::io::Result<Self> {
        let (read, write) = TcpStream::connect(address).await?.into_split();

        Ok(Self {
            write,
            read: BufReader::new(read),
        })
    }

    /// Sends one command and returns its reply as text.
    async fn call(&mut self, args: &[&str]) -> std::io::Result<String> {
        let mut out = format!("*{}\r\n", args.len());

        for arg in args {
            out.push_str(&format!("${}\r\n{arg}\r\n", arg.len()));
        }

        self.write.write_all(out.as_bytes()).await?;
        self.write.flush().await?;

        let mut line = String::new();

        // EOF is the schedule closing the connection, and it has to be an error
        // rather than an empty reply: a client that read "" and carried on would
        // print a blank line and look like it worked.
        if self.read.read_line(&mut line).await? == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "the connection closed before the reply arrived",
            ));
        }

        let line = line.trim_end().to_string();

        // A bulk string has its body on the next line. Nothing here sends a
        // command that returns an array, so one level is enough.
        if let Some(length) = line.strip_prefix('$')
            && let Ok(length) = length.parse::<i64>()
            && length >= 0
        {
            let mut body = vec![0u8; length as usize + 2];
            self.read.read_exact(&mut body).await?;
            body.truncate(length as usize);

            return Ok(String::from_utf8_lossy(&body).to_string());
        }

        Ok(line)
    }
}

/// The key this run's workers contend over.
///
/// Namespaced by `MISORDER_SEED`, which misorder sets on every run. Without it,
/// `mis fuzz --parallel 8` is eight of these fighting over one key on one Redis,
/// and the failures it reports are about the collision rather than about the
/// ordering — which is the most expensive kind of wrong answer a testing tool
/// can give.
///
/// A real service does this by pointing at its own database or by prefixing its
/// keys; the point is that misorder tells it which run it is and stays out of
/// the decision.
fn lock_key() -> String {
    match std::env::var("MISORDER_SEED") {
        Ok(seed) => format!("applock:{seed}"),
        Err(_) => "applock".to_string(),
    }
}

/// Where Redis is, as `host:port`.
///
/// misorder sets `REDIS_URL` to its proxy, and the service reads it the way it
/// would read the real one. That is the whole adoption story: a different value
/// in an ordinary variable.
fn address() -> String {
    let url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string());

    url.trim_start_matches("redis://")
        .trim_end_matches('/')
        .to_string()
}

#[tokio::main]
async fn main() {
    let address = address();

    eprintln!("redis-worker-demo talking to {address}");

    let mut worker_a = Redis::connect(&address)
        .await
        .unwrap_or_else(|error| panic!("worker A could not reach redis at {address}: {error}"));

    let mut worker_b = Redis::connect(&address)
        .await
        .unwrap_or_else(|error| panic!("worker B could not reach redis at {address}: {error}"));

    let key = lock_key();

    // A stale key from an earlier run would stop B ever acquiring, and the run
    // would pass for a reason that has nothing to do with the ordering.
    let _ = worker_a.call(&["DEL", &key]).await;

    let ttl = TTL.as_millis().to_string();

    // Every step reports rather than panics. A connection the schedule closed
    // is a fault this scenario permits, and a service that aborted on one would
    // turn "the ordering was hostile" into "the demo crashed" - which is a
    // different run than the one the invariants are judging.
    let step = |label: &str, result: std::io::Result<String>| match result {
        Ok(reply) => {
            eprintln!("  {label} -> {reply}");
            true
        }
        Err(error) => {
            eprintln!("  {label} -> gave up: {error}");
            false
        }
    };

    if !step(
        "A: acquire",
        worker_a
            .call(&["SET", &key, "worker-a", "NX", "PX", &ttl])
            .await,
    ) {
        return;
    }

    // A's work. One round trip, and the reply is what the schedule may hold:
    // held long enough, the lock above expires while A is still in here.
    step("A: work", worker_a.call(&["GET", &key]).await);

    // B tries to take it. It only succeeds if A's lock expired, which is the
    // whole question this scenario asks.
    step(
        "B: acquire",
        worker_b
            .call(&["SET", &key, "worker-b", "NX", "PX", "5000"])
            .await,
    );

    // A finishes and releases what it believes is its lock.
    step("A: release", worker_a.call(&["DEL", &key]).await);
}
