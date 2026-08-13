//! Reading and validating a scenario file.
//!
//! [`mod@file`] is the whole of it: the TOML shapes a user writes, and the
//! resolved form the running components take once defaults are applied.

pub mod file;

pub use file::{Resolved, Scenario};
