//! Redis semantics.
//!
//! Two invariants, and the second is why this adapter is worth turning on.
//!
//! Redis is where distributed locks live, the canonical implementation is four
//! lines, and the failure mode is documented by Redis itself and implemented
//! wrong anyway. `lock_released_by_owner` catches it with no user input: the
//! whole exchange is on the wire, so the proxy can see a client release a lock
//! that belongs to somebody else without knowing anything about the service.

use std::collections::{HashMap, VecDeque};

use async_trait::async_trait;
use bytes::Bytes;

use crate::event::{ConnectionId, Event, Observed, RedisEvent};
use crate::invariant::{CheckContext, Invariant, Violation};

/// Every command that reached the server got a reply.
///
/// The Redis counterpart of `every_request_reaches_terminal_state`, and it
/// matters for the same reason: a command that is neither answered nor
/// explicitly failed leaves the client unable to say whether it happened. For a
/// `DECRBY` on an inventory count, neither retrying nor not retrying is safe.
#[derive(Debug, Default)]
pub struct EveryCommandGetsAReply {
    /// Commands in flight, oldest first, per connection.
    pending: HashMap<ConnectionId, VecDeque<String>>,
}

#[async_trait]
impl Invariant for EveryCommandGetsAReply {
    fn name(&self) -> &str {
        "every_command_gets_a_reply"
    }

    fn describe(&self) -> &str {
        "every command that reached the server got a reply"
    }

    fn observe(&mut self, observed: &Observed) -> Option<Violation> {
        let connection = observed.connection?;

        match &observed.event {
            Event::Redis(RedisEvent::Command { name, .. }) => {
                self.pending
                    .entry(connection)
                    .or_default()
                    .push_back(name.clone());
                None
            }
            Event::Redis(RedisEvent::Reply { .. }) => {
                self.pending.get_mut(&connection)?.pop_front();
                None
            }
            // Reported here rather than at `finish` so the violation carries
            // the moment it happened rather than the end of the run.
            Event::Redis(RedisEvent::ConnectionClosed) => {
                let pending = self.pending.remove(&connection)?;
                let stranded = pending.front()?;

                Some(Violation {
                    invariant: self.name().to_string(),
                    detail: format!(
                        "{connection} closed with {} command(s) unanswered, starting with \
                         {stranded}",
                        pending.len()
                    ),
                    at: observed.at,
                })
            }
            _ => None,
        }
    }

    async fn finish(
        &mut self,
        _context: &CheckContext,
    ) -> Result<Option<Violation>, crate::error::Error> {
        Ok(None)
    }
}

/// A lock is released by the client that currently holds it.
///
/// # The bug
///
/// The canonical Redis lock is `SET key token NX PX ttl`, and the canonical
/// mistake is releasing it with `DEL key`. That is safe exactly as long as the
/// lock never expires early, and the whole reason it has a TTL is that it can:
///
/// 1. client A takes the lock with token `a1`,
/// 2. A is slow (a GC pause, a delayed reply, a long query) and the TTL expires,
/// 3. client B takes the same lock with token `b7`,
/// 4. A finishes and sends `DEL key`, releasing **B's** lock,
/// 5. C takes it while B still believes it holds it, and now two workers are in
///    the critical section.
///
/// Redis's own documentation says to release with a script that compares the
/// value first. This invariant fires when someone did not.
///
/// # Why it needs no user input
///
/// The entire exchange crosses the wire. `SET ... NX` returning `+OK` is an
/// acquisition and the token is right there in the command; a later `DEL` on
/// that key names the key. Nothing about the service under test has to be known
/// or configured.
///
/// # What it does not claim
///
/// A release that goes through `EVAL` is opaque: the compare-and-delete
/// happens inside Lua, which is exactly the correct implementation, so there is
/// nothing to flag and this stays quiet. That is the right asymmetry: the
/// invariant catches the naive release and says nothing about the careful one.
///
/// It also does not model expiry. It does not have to: what makes step 4 a bug
/// is that B acquired in between, and an acquisition is something this can see.
#[derive(Debug, Default)]
pub struct LockReleasedByOwner {
    /// Key to the token from the last successful `SET ... NX`.
    owner: HashMap<Bytes, Acquisition>,
    /// What each connection believes it holds, per key.
    ///
    /// Compared against [`Self::owner`] rather than comparing connections,
    /// because a pooled client reuses the same connection: A acquires as `a1`,
    /// the lock expires, A acquires again as `a2`, and a stale code path
    /// releases the first one. Same connection, different lock, and connection
    /// identity alone would call that legitimate.
    held: HashMap<(ConnectionId, Bytes), Bytes>,
    /// Commands awaiting a reply, oldest first, per connection.
    ///
    /// Needed because `SET ... NX` only acquires when it answers `+OK`, so a
    /// `$-1` null means somebody else holds it, and treating that as an
    /// acquisition would make every contended lock look like a violation.
    pending: HashMap<ConnectionId, VecDeque<Pending>>,
}

#[derive(Debug, Clone)]
struct Acquisition {
    token: Bytes,
    by: ConnectionId,
}

#[derive(Debug, Clone)]
enum Pending {
    /// A `SET key token NX ...` whose outcome is not known yet.
    Acquire { key: Bytes, token: Bytes },
    /// Anything else. Kept so the reply queue stays aligned with the command
    /// queue; dropping uninteresting commands would pair every later reply
    /// with the wrong one.
    Other,
}

/// Commands that release a key outright.
const RELEASES: [&str; 2] = ["DEL", "UNLINK"];

impl LockReleasedByOwner {
    /// Whether this is the `SET key token NX ...` form, and the token if so.
    ///
    /// `NX` is what makes it a lock rather than an assignment: without it the
    /// command overwrites whatever was there, which is not an acquisition of
    /// anything. Matched anywhere in the options because `SET k v NX PX 100`
    /// and `SET k v PX 100 NX` are the same command.
    fn acquisition(args: &[Bytes]) -> Option<(Bytes, Bytes)> {
        let key = args.first()?;
        let token = args.get(1)?;

        let exclusive = args[2..].iter().any(|arg| arg.eq_ignore_ascii_case(b"NX"));

        exclusive.then(|| (key.clone(), token.clone()))
    }
}

#[async_trait]
impl Invariant for LockReleasedByOwner {
    fn name(&self) -> &str {
        "lock_released_by_owner"
    }

    fn describe(&self) -> &str {
        "a key taken with SET NX is not deleted by a client that no longer holds it"
    }

    fn observe(&mut self, observed: &Observed) -> Option<Violation> {
        let connection = observed.connection?;

        match &observed.event {
            Event::Redis(RedisEvent::Command { name, args, .. }) => {
                if name == "SET"
                    && let Some((key, token)) = Self::acquisition(args)
                {
                    self.pending
                        .entry(connection)
                        .or_default()
                        .push_back(Pending::Acquire { key, token });

                    return None;
                }

                if RELEASES.contains(&name.as_str())
                    && let Some(key) = args.first()
                    && let Some(current) = self.owner.get(key)
                {
                    let believed = self.held.get(&(connection, key.clone()));
                    let stale = believed != Some(&current.token);

                    // Cleared either way: a legitimate release frees the key,
                    // and a late one is reported once rather than by every
                    // command that follows it.
                    let current = current.clone();
                    self.owner.remove(key);
                    self.held.remove(&(connection, key.clone()));

                    self.pending
                        .entry(connection)
                        .or_default()
                        .push_back(Pending::Other);

                    if stale {
                        return Some(Violation {
                            invariant: self.name().to_string(),
                            detail: format!(
                                "{connection} sent {name} on a key currently held by {} under a \
                                 different token; releasing a lock you no longer own lets two \
                                 clients into the same critical section. Release with a script \
                                 that compares the token before deleting.",
                                current.by
                            ),
                            at: observed.at,
                        });
                    }

                    return None;
                }

                self.pending
                    .entry(connection)
                    .or_default()
                    .push_back(Pending::Other);

                None
            }

            Event::Redis(RedisEvent::Reply { error, value }) => {
                let pending = self.pending.get_mut(&connection)?.pop_front()?;

                let Pending::Acquire { key, token } = pending else {
                    return None;
                };

                // `+OK` acquires. A null reply is the contended case - somebody
                // else holds it - and an error is not an acquisition either.
                let acquired = !error
                    && value
                        .as_ref()
                        .is_some_and(|value| value.eq_ignore_ascii_case(b"OK"));

                if acquired {
                    self.held.insert((connection, key.clone()), token.clone());
                    self.owner.insert(
                        key,
                        Acquisition {
                            token,
                            by: connection,
                        },
                    );
                }

                None
            }

            Event::Redis(RedisEvent::ConnectionClosed) => {
                self.pending.remove(&connection);
                self.held.retain(|(held_by, _), _| *held_by != connection);
                None
            }

            _ => None,
        }
    }

    async fn finish(
        &mut self,
        _context: &CheckContext,
    ) -> Result<Option<Violation>, crate::error::Error> {
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::time::Duration;

    fn at(millis: u64, connection: u64, event: RedisEvent) -> Observed {
        Observed::on(
            Duration::from_millis(millis),
            ConnectionId(connection),
            Event::Redis(event),
        )
    }

    fn command(name: &str, args: &[&str]) -> RedisEvent {
        RedisEvent::Command {
            name: name.to_string(),
            args: args
                .iter()
                .map(|arg| Bytes::copy_from_slice(arg.as_bytes()))
                .collect(),
            order: 0,
        }
    }

    fn ok() -> RedisEvent {
        RedisEvent::Reply {
            error: false,
            value: Some(Bytes::from_static(b"OK")),
        }
    }

    fn null() -> RedisEvent {
        RedisEvent::Reply {
            error: false,
            value: None,
        }
    }

    #[test]
    fn a_command_with_a_reply_is_not_stranded() {
        let mut invariant = EveryCommandGetsAReply::default();

        assert!(
            invariant
                .observe(&at(1, 1, command("GET", &["k"])))
                .is_none()
        );
        assert!(invariant.observe(&at(2, 1, ok())).is_none());
        assert!(
            invariant
                .observe(&at(3, 1, RedisEvent::ConnectionClosed))
                .is_none()
        );
    }

    #[test]
    fn closing_with_a_command_in_flight_is_the_violation() {
        let mut invariant = EveryCommandGetsAReply::default();

        invariant.observe(&at(1, 1, command("DECRBY", &["stock", "1"])));

        let violation = invariant
            .observe(&at(2, 1, RedisEvent::ConnectionClosed))
            .expect("a stranded command");

        assert!(violation.detail.contains("DECRBY"), "{}", violation.detail);
    }

    /// The whole scenario, in the order it happens in production.
    #[test]
    fn releasing_a_lock_another_client_took_is_the_violation() {
        let mut invariant = LockReleasedByOwner::default();

        // A takes the lock.
        invariant.observe(&at(
            1,
            1,
            command("SET", &["lock", "a1", "NX", "PX", "100"]),
        ));
        invariant.observe(&at(2, 1, ok()));

        // A is slow, the lock expires, B takes it.
        invariant.observe(&at(
            200,
            2,
            command("SET", &["lock", "b7", "NX", "PX", "100"]),
        ));
        invariant.observe(&at(201, 2, ok()));

        // A finishes and releases what it thinks is its lock.
        let violation = invariant
            .observe(&at(250, 1, command("DEL", &["lock"])))
            .expect("A released B's lock");

        assert_eq!(violation.invariant, "lock_released_by_owner");
        assert!(violation.detail.contains("DEL"), "{}", violation.detail);
    }

    #[test]
    fn releasing_your_own_lock_is_fine() {
        let mut invariant = LockReleasedByOwner::default();

        invariant.observe(&at(
            1,
            1,
            command("SET", &["lock", "a1", "NX", "PX", "100"]),
        ));
        invariant.observe(&at(2, 1, ok()));

        assert!(
            invariant
                .observe(&at(3, 1, command("DEL", &["lock"])))
                .is_none()
        );
    }

    /// A contended `SET NX` answers null, and treating that as an acquisition
    /// would make every lock that was ever contended look like a violation.
    #[test]
    fn a_failed_acquisition_does_not_take_ownership() {
        let mut invariant = LockReleasedByOwner::default();

        invariant.observe(&at(
            1,
            1,
            command("SET", &["lock", "a1", "NX", "PX", "100"]),
        ));
        invariant.observe(&at(2, 1, ok()));

        // B tries and loses.
        invariant.observe(&at(
            3,
            2,
            command("SET", &["lock", "b7", "NX", "PX", "100"]),
        ));
        invariant.observe(&at(4, 2, null()));

        // A releasing its own lock is still fine: B never owned it.
        assert!(
            invariant
                .observe(&at(5, 1, command("DEL", &["lock"])))
                .is_none()
        );
    }

    /// `SET k v` without `NX` overwrites rather than acquires. Treating it as a
    /// lock would flag ordinary cache writes.
    #[test]
    fn a_set_without_nx_is_not_a_lock() {
        let mut invariant = LockReleasedByOwner::default();

        invariant.observe(&at(1, 1, command("SET", &["k", "v"])));
        invariant.observe(&at(2, 1, ok()));

        assert!(
            invariant
                .observe(&at(3, 2, command("DEL", &["k"])))
                .is_none()
        );
    }

    /// The option order varies between clients and both forms mean the same
    /// thing.
    #[test]
    fn nx_is_found_wherever_it_appears_in_the_options() {
        let args: Vec<Bytes> = ["lock", "t1", "PX", "100", "nx"]
            .iter()
            .map(|arg| Bytes::copy_from_slice(arg.as_bytes()))
            .collect();

        assert!(LockReleasedByOwner::acquisition(&args).is_some());
    }

    /// A pooled client reuses its connection, so "did the same connection
    /// acquire it" is not the question. A acquires as `a1`, the lock expires,
    /// A acquires again as `a2`, and a stale code path releases the first one -
    /// same connection, different lock.
    #[test]
    fn a_stale_token_on_the_same_connection_is_still_a_late_release() {
        let mut invariant = LockReleasedByOwner::default();

        invariant.observe(&at(1, 1, command("SET", &["lock", "a1", "NX"])));
        invariant.observe(&at(2, 1, ok()));

        // The same connection takes it again under a new token.
        invariant.observe(&at(300, 1, command("SET", &["lock", "a2", "NX"])));
        invariant.observe(&at(301, 1, ok()));

        // And then something releases the one it no longer holds.
        assert!(
            invariant
                .observe(&at(350, 1, command("DEL", &["lock"])))
                .is_none(),
            "a release matching the current token is legitimate"
        );

        // Now the interesting direction: acquire, expire, re-acquire elsewhere,
        // and release under the first token.
        let mut invariant = LockReleasedByOwner::default();

        invariant.observe(&at(1, 1, command("SET", &["lock", "a1", "NX"])));
        invariant.observe(&at(2, 1, ok()));
        invariant.observe(&at(300, 1, command("SET", &["other", "a2", "NX"])));
        invariant.observe(&at(301, 1, ok()));
        invariant.observe(&at(400, 2, command("SET", &["lock", "b7", "NX"])));
        invariant.observe(&at(401, 2, ok()));

        assert!(
            invariant
                .observe(&at(500, 1, command("DEL", &["lock"])))
                .is_some(),
            "connection 1 released a lock connection 2 now holds"
        );
    }

    /// A key nobody acquired is an ordinary cache delete.
    #[test]
    fn deleting_a_key_no_lock_ever_took_is_not_a_finding() {
        let mut invariant = LockReleasedByOwner::default();

        assert!(
            invariant
                .observe(&at(1, 1, command("DEL", &["some:cache:key"])))
                .is_none()
        );
    }

    /// One late release is one finding. Reporting it again for every command
    /// that follows would bury the next real one.
    #[test]
    fn a_late_release_is_reported_once() {
        let mut invariant = LockReleasedByOwner::default();

        invariant.observe(&at(1, 1, command("SET", &["lock", "a1", "NX"])));
        invariant.observe(&at(2, 1, ok()));
        invariant.observe(&at(3, 2, command("SET", &["lock", "b7", "NX"])));
        invariant.observe(&at(4, 2, ok()));

        assert!(
            invariant
                .observe(&at(5, 1, command("DEL", &["lock"])))
                .is_some()
        );
        assert!(
            invariant
                .observe(&at(6, 1, command("DEL", &["lock"])))
                .is_none()
        );
    }

    /// The correct release goes through a script, so the compare-and-delete is
    /// inside Lua and there is nothing to flag.
    #[test]
    fn an_eval_release_is_not_flagged() {
        let mut invariant = LockReleasedByOwner::default();

        invariant.observe(&at(1, 1, command("SET", &["lock", "a1", "NX"])));
        invariant.observe(&at(2, 1, ok()));
        invariant.observe(&at(3, 2, command("SET", &["lock", "b7", "NX"])));
        invariant.observe(&at(4, 2, ok()));

        assert!(
            invariant
                .observe(&at(5, 1, command("EVAL", &["...", "1", "lock", "a1"])))
                .is_none(),
            "a compare-and-delete script is the correct implementation"
        );
    }
}
