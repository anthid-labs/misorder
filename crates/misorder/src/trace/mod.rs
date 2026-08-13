//! Recording a run so it can be replayed.
//!
//! Every draw the scheduler makes is appended here: what was decided, when, on
//! which connection, about which message. The result is a complete, replayable
//! description of one run, which is what turns a failure from an anecdote into
//! an artifact you can commit.
//!
//! # Format
//!
//! JSON Lines. One header line, then one line per decision:
//!
//! ```jsonl
//! {"t":"header","format":1,"seed":8837291,"scenario":"dead_letter_no_redelivery"}
//! {"t":"decision","seq":0,"at_ms":3,"kind":"deliver","connection":1,"ordinal":0,"detail":"ledger.order","do":"deliver","delay_ms":0}
//! {"t":"decision","seq":1,"at_ms":41,"kind":"ack","connection":1,"ordinal":0,"do":"drop"}
//! ```
//!
//! Line-oriented for a reason that outlives the format: a shrunk trace is
//! committed to a repository, and a diff of one is how a reviewer sees that a
//! reproducer got smaller. It also means an interrupted run leaves a valid
//! prefix rather than a truncated document.

pub mod decision;
pub mod replay;

pub use decision::{Decision, DecisionPoint, PointKey, PointKind};
pub use replay::Replay;

use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};

/// Bumped whenever an older misorder would misread a newer trace.
///
/// A trace is a committed artifact with a long life: someone's CI is running a
/// reproducer recorded a year ago. Refusing to read a format from the future is
/// the difference between a clear error and a silently different run.
pub const FORMAT_VERSION: u32 = 1;

/// One recorded decision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Record {
    /// Position in this trace. Diagnostic only: replay matches on
    /// [`PointKey`], never on `seq`. See [`PointKey`] for why.
    pub seq: u64,

    /// Since the run started.
    #[serde(rename = "at_ms", with = "duration_ms")]
    pub at: Duration,

    #[serde(flatten)]
    pub point: DecisionPoint,

    #[serde(flatten)]
    pub decision: Decision,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "t", rename_all = "snake_case")]
enum Line {
    Header {
        format: u32,
        seed: u64,
        scenario: String,
    },
    Decision(Record),
}

/// A complete description of one run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Trace {
    /// The seed that produced it. Kept even for a shrunk trace, because it says
    /// where the failure came from, but it is not what reproduces the run: the
    /// records are. A shrunk trace and its seed disagree by construction.
    pub seed: u64,
    pub scenario: String,
    pub records: Vec<Record>,
}

impl Trace {
    pub fn new(seed: u64, scenario: impl Into<String>) -> Self {
        Self {
            seed,
            scenario: scenario.into(),
            records: Vec::new(),
        }
    }

    /// Decisions that actually perturbed the run.
    ///
    /// What a reproducer prints, and what the shrinker works on. Neutral
    /// records are kept in the file because their absence and their neutrality
    /// are different facts: one says the fork never happened, the other says it
    /// happened and nothing was done to it.
    pub fn active(&self) -> impl Iterator<Item = &Record> {
        self.records
            .iter()
            .filter(|record| !record.decision.is_neutral())
    }

    pub fn active_count(&self) -> usize {
        self.active().count()
    }

    /// Reads a trace from a JSON Lines file.
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let file = std::fs::File::open(path).map_err(|error| match error.kind() {
            std::io::ErrorKind::NotFound => Error::NotFound(path.display().to_string()),
            _ => Error::Io(error),
        })?;

        let mut trace: Option<Trace> = None;

        for (number, line) in BufReader::new(file).lines().enumerate() {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }

            let parsed: Line = serde_json::from_str(&line).map_err(|error| {
                Error::Trace(format!(
                    "{}:{}: {error}",
                    path.display(),
                    number.saturating_add(1)
                ))
            })?;

            match (parsed, &mut trace) {
                (
                    Line::Header {
                        format,
                        seed,
                        scenario,
                    },
                    slot @ None,
                ) => {
                    if format > FORMAT_VERSION {
                        return Err(Error::Trace(format!(
                            "{} is format {format}; this build reads up to {FORMAT_VERSION}",
                            path.display()
                        )));
                    }
                    *slot = Some(Trace::new(seed, scenario));
                }
                (Line::Header { .. }, Some(_)) => {
                    return Err(Error::Trace(format!(
                        "{}:{}: a second header line",
                        path.display(),
                        number.saturating_add(1)
                    )));
                }
                (Line::Decision(record), Some(trace)) => trace.records.push(record),
                (Line::Decision(_), None) => {
                    return Err(Error::Trace(format!(
                        "{}: a decision before the header line",
                        path.display()
                    )));
                }
            }
        }

        trace.ok_or_else(|| Error::Trace(format!("{}: empty, no header line", path.display())))
    }

    /// Writes a trace as JSON Lines.
    pub fn save(&self, path: impl AsRef<Path>) -> Result<()> {
        let mut out = Vec::new();

        writeln!(
            out,
            "{}",
            serde_json::to_string(&Line::Header {
                format: FORMAT_VERSION,
                seed: self.seed,
                scenario: self.scenario.clone(),
            })
            .map_err(|error| Error::Internal(error.to_string()))?
        )?;

        for record in &self.records {
            writeln!(
                out,
                "{}",
                serde_json::to_string(&Line::Decision(record.clone()))
                    .map_err(|error| Error::Internal(error.to_string()))?
            )?;
        }

        std::fs::write(path, out)?;

        Ok(())
    }

    /// A stable identifier for the *shape* of this failure.
    ///
    /// Two runs that found the same bug produce the same signature; two runs
    /// that found different bugs do not. That is the whole contract, and it is
    /// what makes "ten failing seeds are usually two bugs" a computable
    /// statement rather than an observation.
    ///
    /// What is deliberately excluded, because it varies between two instances
    /// of one bug:
    ///
    /// - **Timestamps.** Wall-clock elapsed differs every run.
    /// - **Details.** Order ids and subjects carry the run's data, not its
    ///   shape. A signature that changed with an order id would never match
    ///   twice.
    /// - **Ordinals.** Dropping the third ack and dropping the fifth is the
    ///   same bug found two ways.
    /// - **Decision parameters.** A 40ms delay and a 90ms delay are one
    ///   failure, so only which *kind* of decision was taken counts.
    /// - **Connection numbering.** Connections are renumbered in order of first
    ///   appearance, so the same interleaving on conn 2 and conn 5 as on conn 1
    ///   and conn 2 signs identically.
    ///
    /// Computed on a *shrunk* trace. An unshrunk one signs its 847 incidental
    /// decisions along with the six that mattered, so two runs of the same bug
    /// would never agree.
    ///
    /// Grouping signatures into bugs, tracking them over time, and telling a
    /// team which pull request introduced one are hosted concerns. This is the
    /// key they are keyed on, and it is computed here so a local user gets the
    /// same identity without an account.
    pub fn signature(&self) -> String {
        let mut hasher = blake3::Hasher::new();

        hasher.update(b"misorder-trace-signature-v1\0");
        hasher.update(self.scenario.as_bytes());
        hasher.update(b"\0");

        // Renumbered in order of first appearance, so which connection happened
        // to be accepted first does not change the identity of the failure.
        let mut canonical: Vec<u64> = Vec::new();

        for record in self.active() {
            let raw = record.point.key.connection;
            let index = match canonical.iter().position(|seen| *seen == raw) {
                Some(index) => index,
                None => {
                    canonical.push(raw);
                    canonical.len() - 1
                }
            };

            hasher.update(&(index as u64).to_le_bytes());
            hasher.update(record.point.key.kind.as_str().as_bytes());
            hasher.update(record.decision.discriminant().as_bytes());
            hasher.update(b"\0");
        }

        hasher.finalize().to_hex()[..16].to_string()
    }

    /// The same trace with the decisions at `keys` replaced by
    /// [`Decision::NEUTRAL`].
    ///
    /// The shrinker's one mutation. Records are neutralised rather than deleted
    /// so the file still describes every fork the original run reached, which
    /// is what lets a reader see that a fault was available and not needed.
    pub fn without(&self, keys: &[PointKey]) -> Trace {
        let mut shrunk = self.clone();

        for record in &mut shrunk.records {
            if keys.contains(&record.point.key) {
                record.decision = Decision::NEUTRAL;
            }
        }

        shrunk
    }
}

/// Collects decisions during a run.
///
/// Shared by every proxy adapter, so it is behind a `Mutex` around a plain
/// `Vec`. The lock is held for a push and nothing else, and the alternative,
/// a channel to an owning task, would put the recorder's own scheduling between
/// a decision and its record.
#[derive(Debug, Clone)]
pub struct Recorder {
    inner: Arc<Mutex<Trace>>,
}

impl Recorder {
    pub fn new(seed: u64, scenario: impl Into<String>) -> Self {
        Self {
            inner: Arc::new(Mutex::new(Trace::new(seed, scenario))),
        }
    }

    /// Appends one decision. `at` is measured from the start of the run.
    pub fn record(&self, at: Duration, point: DecisionPoint, decision: Decision) {
        let mut trace = self.inner.lock().expect("recorder mutex poisoned");
        let seq = trace.records.len() as u64;

        trace.records.push(Record {
            seq,
            at,
            point,
            decision,
        });
    }

    /// The trace so far.
    pub fn snapshot(&self) -> Trace {
        self.inner.lock().expect("recorder mutex poisoned").clone()
    }
}

/// `Duration` as whole milliseconds.
///
/// Not `humantime` here, which is right for a scenario a human writes and wrong
/// for a trace a machine writes and diffs. Milliseconds because that is the
/// granularity the scheduler chooses delays at; sub-millisecond precision would
/// imply a control the proxy does not have.
pub(crate) mod duration_ms {
    use std::time::Duration;

    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(value: &Duration, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_u64(value.as_millis() as u64)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Duration, D::Error> {
        u64::deserialize(deserializer).map(Duration::from_millis)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::ConnectionId;

    fn record(seq: u64, ordinal: u64, decision: Decision) -> Record {
        Record {
            seq,
            at: Duration::from_millis(seq),
            point: DecisionPoint::new(PointKind::Deliver, ConnectionId(1), ordinal),
            decision,
        }
    }

    #[test]
    fn a_trace_round_trips_through_a_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("trace.jsonl");

        let mut trace = Trace::new(8_837_291, "dead_letter_no_redelivery");
        trace.records.push(record(0, 0, Decision::NEUTRAL));
        trace.records.push(record(1, 1, Decision::Drop));

        trace.save(&path).expect("save");

        assert_eq!(Trace::load(&path).expect("load"), trace);
    }

    #[test]
    fn only_perturbing_decisions_are_active() {
        let mut trace = Trace::new(1, "s");
        trace.records.push(record(0, 0, Decision::NEUTRAL));
        trace.records.push(record(1, 1, Decision::Drop));
        trace.records.push(record(2, 2, Decision::CloseConnection));

        assert_eq!(trace.active_count(), 2);
    }

    #[test]
    fn neutralising_keeps_the_record_and_drops_the_fault() {
        let mut trace = Trace::new(1, "s");
        trace.records.push(record(0, 0, Decision::Drop));
        trace.records.push(record(1, 1, Decision::Drop));

        let key = trace.records[0].point.key;
        let shrunk = trace.without(&[key]);

        assert_eq!(
            shrunk.records.len(),
            2,
            "records are neutralised, not removed"
        );
        assert_eq!(shrunk.active_count(), 1);
    }

    #[test]
    fn a_trace_from_the_future_is_refused_rather_than_guessed_at() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("trace.jsonl");

        std::fs::write(
            &path,
            r#"{"t":"header","format":99,"seed":1,"scenario":"s"}"#,
        )
        .expect("write");

        let error = Trace::load(&path).expect_err("should refuse");

        assert!(matches!(error, Error::Trace(_)), "got {error:?}");
    }

    #[test]
    fn a_missing_trace_reports_not_found() {
        let error = Trace::load("/nonexistent/trace.jsonl").expect_err("should fail");

        assert!(matches!(error, Error::NotFound(_)), "got {error:?}");
    }

    #[test]
    fn the_recorder_numbers_decisions_in_order() {
        let recorder = Recorder::new(7, "s");

        recorder.record(
            Duration::from_millis(1),
            DecisionPoint::new(PointKind::Ack, ConnectionId(1), 0),
            Decision::Drop,
        );
        recorder.record(
            Duration::from_millis(2),
            DecisionPoint::new(PointKind::Ack, ConnectionId(1), 1),
            Decision::Drop,
        );

        let trace = recorder.snapshot();

        assert_eq!(trace.seed, 7);
        assert_eq!(
            trace.records.iter().map(|r| r.seq).collect::<Vec<_>>(),
            vec![0, 1]
        );
    }
}

#[cfg(test)]
mod signature_tests {
    use super::*;
    use crate::event::ConnectionId;

    fn trace_of(scenario: &str, decisions: &[(u64, PointKind, u64, Decision)]) -> Trace {
        let mut trace = Trace::new(1, scenario);

        for (seq, (connection, kind, ordinal, decision)) in decisions.iter().enumerate() {
            trace.records.push(Record {
                seq: seq as u64,
                at: Duration::from_millis(seq as u64 * 13),
                point: DecisionPoint::new(*kind, ConnectionId(*connection), *ordinal)
                    .with_detail(format!("order-{seq}")),
                decision: *decision,
            });
        }

        trace
    }

    #[test]
    fn the_same_failure_shape_signs_identically() {
        let one = trace_of(
            "s",
            &[
                (1, PointKind::Ack, 0, Decision::Drop),
                (1, PointKind::Deliver, 4, Decision::CloseConnection),
            ],
        );

        // Different ordinals, different connection numbers, different details,
        // different timestamps. Same bug.
        let two = trace_of(
            "s",
            &[
                (7, PointKind::Ack, 91, Decision::Drop),
                (7, PointKind::Deliver, 92, Decision::CloseConnection),
            ],
        );

        assert_eq!(one.signature(), two.signature());
    }

    #[test]
    fn a_delay_of_a_different_length_is_the_same_failure() {
        let short = trace_of(
            "s",
            &[(
                1,
                PointKind::Deliver,
                0,
                Decision::Deliver {
                    delay: Duration::from_millis(40),
                },
            )],
        );
        let long = trace_of(
            "s",
            &[(
                1,
                PointKind::Deliver,
                0,
                Decision::Deliver {
                    delay: Duration::from_millis(900),
                },
            )],
        );

        assert_eq!(short.signature(), long.signature());
    }

    #[test]
    fn a_different_sequence_of_decisions_is_a_different_failure() {
        let dropped = trace_of("s", &[(1, PointKind::Ack, 0, Decision::Drop)]);
        let closed = trace_of("s", &[(1, PointKind::Ack, 0, Decision::CloseConnection)]);

        assert_ne!(dropped.signature(), closed.signature());
    }

    #[test]
    fn the_same_shape_in_a_different_scenario_is_a_different_failure() {
        let ledger = trace_of("ledger", &[(1, PointKind::Ack, 0, Decision::Drop)]);
        let billing = trace_of("billing", &[(1, PointKind::Ack, 0, Decision::Drop)]);

        assert_ne!(ledger.signature(), billing.signature());
    }

    #[test]
    fn interleaving_across_connections_is_part_of_the_shape() {
        let together = trace_of(
            "s",
            &[
                (1, PointKind::Ack, 0, Decision::Drop),
                (1, PointKind::Ack, 1, Decision::Drop),
            ],
        );
        let apart = trace_of(
            "s",
            &[
                (1, PointKind::Ack, 0, Decision::Drop),
                (2, PointKind::Ack, 0, Decision::Drop),
            ],
        );

        assert_ne!(
            together.signature(),
            apart.signature(),
            "two acks on one connection is not the same failure as one ack on each"
        );
    }

    #[test]
    fn neutral_decisions_do_not_participate() {
        let bare = trace_of("s", &[(1, PointKind::Ack, 0, Decision::Drop)]);

        let mut padded = bare.clone();
        padded.records.push(Record {
            seq: 9,
            at: Duration::from_millis(500),
            point: DecisionPoint::new(PointKind::Deliver, ConnectionId(3), 0),
            decision: Decision::NEUTRAL,
        });

        assert_eq!(bare.signature(), padded.signature());
    }

    #[test]
    fn a_signature_is_short_enough_to_read_aloud() {
        let trace = trace_of("s", &[(1, PointKind::Ack, 0, Decision::Drop)]);

        assert_eq!(trace.signature().len(), 16);
    }
}
