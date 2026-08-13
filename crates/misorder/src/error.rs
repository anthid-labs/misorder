use std::io;

use thiserror::Error as ThisError;

pub type Result<T> = std::result::Result<T, Error>;

/// Errors surfaced by misorder itself.
///
/// Variants are the categories a caller can act on, not a mirror of every
/// underlying failure. Anything a caller cannot branch on belongs in
/// [`Error::Internal`] or [`Error::Io`].
///
/// Note what is *not* here: a service under test violating an invariant. That
/// is not an error, it is the result, and it travels as
/// [`Outcome`](crate::runner::Outcome). Conflating the two is how a harness
/// ends up reporting its own bugs as the user's, and once a tool has cried
/// wolf about a failure that was never real, nobody trusts the failures that
/// are.
#[derive(Debug, ThisError)]
pub enum Error {
    /// A path that was expected to exist does not.
    #[error("not found: {0}")]
    NotFound(String),

    /// The scenario file is missing, malformed, or self-contradictory.
    #[error("invalid scenario: {0}")]
    Scenario(String),

    /// A dependency could not be started, reached, or given its topology.
    #[error("environment: {0}")]
    Environment(String),

    /// A frame could not be decoded, or arrived where the protocol does not
    /// allow it.
    ///
    /// Always about a connection misorder is proxying, never about a decision
    /// it made. A malformed frame from the service under test is a finding;
    /// one from the real dependency is a bug in the adapter.
    #[error("protocol {protocol}: {message}")]
    Protocol {
        protocol: &'static str,
        message: String,
    },

    /// A trace file is malformed, or its decisions do not line up with the run
    /// being replayed.
    ///
    /// The second case is the interesting one: it means the run diverged from
    /// what the trace describes, so whatever it reproduced is not the recorded
    /// failure. Reported rather than papered over, because a replay that
    /// silently drifts is worse than one that refuses.
    #[error("trace: {0}")]
    Trace(String),

    /// Something did not happen inside the time it was given.
    #[error("timed out after {elapsed:?}: {what}")]
    Timeout {
        what: String,
        elapsed: std::time::Duration,
    },

    /// An I/O failure with no more specific category. The source is preserved
    /// so the raw `io::ErrorKind` stays reachable.
    #[error("io error: {0}")]
    Io(#[source] io::Error),

    /// The scenario is valid but asks for something misorder cannot do yet.
    ///
    /// Distinct from [`Error::Scenario`]: the file is not wrong, the feature is
    /// missing. Says so plainly rather than failing as if the operator erred.
    /// Most of this crate currently answers with this variant.
    #[error("not supported yet: {0}")]
    Unsupported(String),

    /// An invariant was violated. Reaching this is a bug in misorder.
    #[error("internal error: {0}")]
    Internal(String),
}

impl Error {
    /// Convenience for the adapters, which all report the same shape.
    pub fn protocol(protocol: &'static str, message: impl Into<String>) -> Self {
        Error::Protocol {
            protocol,
            message: message.into(),
        }
    }
}

/// Maps the `io::ErrorKind`s that callers branch on into their own variants and
/// keeps the rest as [`Error::Io`], so a caller can match on `NotFound` without
/// reaching through to the source.
impl From<io::Error> for Error {
    fn from(error: io::Error) -> Self {
        match error.kind() {
            io::ErrorKind::NotFound => Error::NotFound(error.to_string()),
            _ => Error::Io(error),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn io_not_found_maps_to_not_found() {
        let err = Error::from(io::Error::new(io::ErrorKind::NotFound, "no such file"));

        assert!(matches!(err, Error::NotFound(_)));
    }

    #[test]
    fn unmapped_io_kind_stays_io_and_keeps_its_kind() {
        let err = Error::from(io::Error::new(io::ErrorKind::BrokenPipe, "pipe closed"));

        match err {
            Error::Io(source) => assert_eq!(source.kind(), io::ErrorKind::BrokenPipe),
            other => panic!("expected Error::Io, got {other:?}"),
        }
    }

    #[test]
    fn display_is_prefixed_by_category() {
        let err = Error::Scenario("no [[system]] block".to_string());

        assert_eq!(err.to_string(), "invalid scenario: no [[system]] block");
    }

    #[test]
    fn protocol_errors_name_their_protocol() {
        let err = Error::protocol("nats", "HMSG with no subject");

        assert_eq!(err.to_string(), "protocol nats: HMSG with no subject");
    }
}
