//! Deriving a decision from a seed and a fork.

use std::time::Duration;

use rand::RngExt;
use rand_chacha::ChaCha8Rng;
use rand_chacha::rand_core::SeedableRng;

use crate::schedule::{DecisionSource, FaultKind};
use crate::trace::{Decision, DecisionPoint, PointKey, PointKind};

/// How aggressive a schedule is.
///
/// Not in the scenario file yet, and that is deliberate: the file should say
/// *what may happen*, not how often, and a user tuning probabilities is a user
/// who has been handed the tool's problem. If it turns out that some scenarios
/// need a heavier hand, this becomes a named profile (`light`, `heavy`) rather
/// than a set of floats.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Profile {
    /// Chance that a given fork is perturbed at all.
    ///
    /// Low on purpose. A run where everything breaks finds nothing, because the
    /// service never gets far enough to reach the interesting state. The bugs
    /// this tool is for need a system that mostly works.
    pub fault_probability: f64,

    /// Upper bound on an injected delay.
    pub max_delay: Duration,

    /// How long an ack is held for [`FaultKind::AckTimeout`].
    ///
    /// Has to exceed the dependency's own `ack_wait` to mean anything. Until
    /// the virtual clock lands in Phase 3, that makes this the single most
    /// expensive fault in wall-clock terms, and the reason a scenario's
    /// `ack_wait` is usually compressed in test config.
    pub ack_hold: Duration,
}

impl Default for Profile {
    fn default() -> Self {
        Self {
            fault_probability: 0.15,
            max_delay: Duration::from_millis(250),
            ack_hold: Duration::from_secs(45),
        }
    }
}

/// Decisions drawn from a seed.
///
/// Stateless. Every fork derives its own generator, so `decide` is a pure
/// function of `(seed, fork)` and the order forks arrive in cannot affect the
/// answers. See the module docs on [`schedule`](crate::schedule) for why that
/// is a correctness requirement and not an optimisation.
#[derive(Debug, Clone)]
pub struct Seeded {
    seed: u64,
    faults: Vec<FaultKind>,
    profile: Profile,
}

impl Seeded {
    pub fn new(seed: u64, faults: Vec<FaultKind>, profile: Profile) -> Self {
        Self {
            seed,
            faults,
            profile,
        }
    }

    pub fn seed(&self) -> u64 {
        self.seed
    }

    /// An independent generator for one fork.
    ///
    /// The components are packed rather than hashed together, because ChaCha8
    /// is already a pseudorandom function of its key: two seeds differing in
    /// one bit produce unrelated streams, so there is nothing for a mixing step
    /// to improve. The trailing constant is a domain separator, so a future
    /// caller deriving something else from the same seed cannot collide with
    /// this.
    fn rng_for(&self, key: &PointKey) -> ChaCha8Rng {
        let mut material = [0u8; 32];

        material[0..8].copy_from_slice(&self.seed.to_le_bytes());
        material[8..16].copy_from_slice(&key.connection.to_le_bytes());
        material[16..24].copy_from_slice(&key.ordinal.to_le_bytes());
        material[24] = key.kind as u8;
        material[25..32].copy_from_slice(b"misordr");

        ChaCha8Rng::from_seed(material)
    }

    /// Which permitted faults could fire at this kind of fork.
    fn candidates(&self, kind: PointKind) -> Vec<FaultKind> {
        self.faults
            .iter()
            .copied()
            .filter(|fault| fault.applies_at(kind))
            .collect()
    }

    fn decision_for(&self, fault: FaultKind, key: &PointKey, rng: &mut ChaCha8Rng) -> Decision {
        match fault {
            FaultKind::Delay => Decision::Deliver {
                delay: Duration::from_millis(
                    rng.random_range(1..=self.profile.max_delay.as_millis().max(1) as u64),
                ),
            },
            FaultKind::AckTimeout => Decision::Deliver {
                delay: self.profile.ack_hold,
            },
            FaultKind::SwallowAck | FaultKind::Redelivery => Decision::Drop,
            FaultKind::ConnectionDrop => Decision::CloseConnection,
            // Ahead of the fork that follows this one on the same connection.
            // Expressed relative to this fork rather than as an absolute id so
            // it survives shrinking: neutralising a decision elsewhere does not
            // renumber it.
            FaultKind::Reorder => Decision::Reorder {
                ahead_of: key.ordinal.saturating_add(1),
            },
            FaultKind::CorruptFrame => Decision::Corrupt {
                offset: rng.random_range(0..64),
            },
            FaultKind::HoldStatement => Decision::Hold {
                until: key.ordinal.saturating_add(1),
            },
        }
    }
}

impl DecisionSource for Seeded {
    fn decide(&self, point: &DecisionPoint) -> Decision {
        let candidates = self.candidates(point.key.kind);

        if candidates.is_empty() {
            return Decision::NEUTRAL;
        }

        let mut rng = self.rng_for(&point.key);

        if !rng.random_bool(self.profile.fault_probability) {
            return Decision::NEUTRAL;
        }

        let fault = candidates[rng.random_range(0..candidates.len())];

        self.decision_for(fault, &point.key, &mut rng)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::ConnectionId;

    fn points() -> Vec<DecisionPoint> {
        (0..200)
            .map(|n| {
                let kind = match n % 3 {
                    0 => PointKind::Deliver,
                    1 => PointKind::Ack,
                    _ => PointKind::Statement,
                };
                DecisionPoint::new(kind, ConnectionId(n % 5), n)
            })
            .collect()
    }

    fn schedule_of(seed: u64) -> Vec<Decision> {
        let source = Seeded::new(seed, FaultKind::ALL.to_vec(), Profile::default());

        points().iter().map(|point| source.decide(point)).collect()
    }

    #[test]
    fn the_same_seed_gives_the_same_schedule() {
        assert_eq!(schedule_of(8_837_291), schedule_of(8_837_291));
    }

    #[test]
    fn a_different_seed_gives_a_different_schedule() {
        assert_ne!(schedule_of(8_837_291), schedule_of(8_837_292));
    }

    #[test]
    fn a_fork_is_answered_the_same_however_many_forks_came_first() {
        let source = Seeded::new(8_837_291, FaultKind::ALL.to_vec(), Profile::default());
        let target = DecisionPoint::new(PointKind::Deliver, ConnectionId(3), 17);

        let cold = source.decide(&target);

        for point in points() {
            source.decide(&point);
        }

        assert_eq!(
            source.decide(&target),
            cold,
            "decisions must not depend on draw order"
        );
    }

    #[test]
    fn permitting_no_faults_perturbs_nothing() {
        let source = Seeded::new(8_837_291, vec![], Profile::default());

        assert!(
            points()
                .iter()
                .all(|point| source.decide(point).is_neutral())
        );
    }

    #[test]
    fn only_permitted_faults_appear() {
        let source = Seeded::new(8_837_291, vec![FaultKind::SwallowAck], Profile::default());

        for point in points() {
            let decision = source.decide(&point);

            assert!(
                decision.is_neutral() || decision == Decision::Drop,
                "unpermitted decision {decision}"
            );
        }
    }

    #[test]
    fn a_permitted_fault_actually_fires_somewhere() {
        let source = Seeded::new(
            8_837_291,
            vec![FaultKind::ConnectionDrop],
            Profile::default(),
        );

        assert!(
            points()
                .iter()
                .any(|point| source.decide(point) == Decision::CloseConnection),
            "a fault that never fires is a scheduler that tests nothing"
        );
    }
}
