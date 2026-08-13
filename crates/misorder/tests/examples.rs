//! The scenario files shipped in this repository must parse.
//!
//! A broken example is a bad first five minutes, and the example is the
//! onboarding. This also pins the documented format against the parser: a key
//! renamed in `scenario::file` without updating `misorder.example.toml` fails
//! here rather than in someone's terminal.

use std::path::{Path, PathBuf};

use misorder::scenario::file::Scenario;

fn repository_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .canonicalize()
        .expect("the workspace root is two levels up from this crate")
}

fn resolves(path: &Path) {
    let scenario = Scenario::load(path)
        .unwrap_or_else(|error| panic!("{} does not parse: {error}", path.display()));

    scenario
        .resolve()
        .unwrap_or_else(|error| panic!("{} does not resolve: {error}", path.display()));
}

#[test]
fn the_documented_example_parses_and_resolves() {
    resolves(&repository_root().join("misorder.example.toml"));
}

#[test]
fn every_scenario_under_examples_parses_and_resolves() {
    let directory = repository_root().join("examples");

    let mut checked = 0;

    for entry in std::fs::read_dir(&directory).expect("examples/ exists") {
        let path = entry.expect("readable entry").path();

        if path
            .extension()
            .is_some_and(|extension| extension == "toml")
        {
            resolves(&path);
            checked += 1;
        }
    }

    assert!(checked > 0, "no scenarios found in {}", directory.display());
}

#[test]
fn the_documented_example_exercises_every_fault() {
    // Not a style rule. A fault absent from the reference file is a fault
    // nobody knows exists, and the list in `[faults] enabled` is the only place
    // a user learns the vocabulary.
    let text = std::fs::read_to_string(repository_root().join("misorder.example.toml"))
        .expect("read example");

    for fault in misorder::schedule::FaultKind::ALL {
        assert!(
            text.contains(fault.as_str()),
            "`{fault}` is not mentioned in misorder.example.toml"
        );
    }
}

#[test]
fn the_documented_example_mentions_every_builtin_invariant() {
    let text = std::fs::read_to_string(repository_root().join("misorder.example.toml"))
        .expect("read example");

    // The implemented ones only. A planned invariant in the reference file
    // would be a scenario a user could copy that then refuses to run.
    for entry in misorder::invariant::builtin::REGISTRY {
        if entry.status != misorder::invariant::builtin::Status::Implemented {
            continue;
        }

        assert!(
            text.contains(entry.name),
            "`{}` is implemented but not mentioned in misorder.example.toml",
            entry.name
        );
    }
}
