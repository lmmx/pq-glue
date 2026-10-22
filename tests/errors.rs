//! What `concat` refuses, and that it leaves no output when it does.

mod common;

use common::*;
use pq_glue::Error;

#[test]
fn no_inputs() {
    let dir = tempfile::tempdir().unwrap();
    let err = pq_glue::concat(&[], &dir.path().join("out.parquet")).unwrap_err();
    assert!(matches!(err, Error::NoInputs));
}

#[test]
fn output_is_an_input() {
    let dir = tempfile::tempdir().unwrap();
    let input = write(dir.path(), 0, 10, 10, true);
    let before = std::fs::read(&input).unwrap();
    let err = pq_glue::concat(std::slice::from_ref(&input), &input).unwrap_err();
    assert!(matches!(err, Error::OutputIsInput(_)));
    assert_eq!(
        std::fs::read(&input).unwrap(),
        before,
        "the input is untouched"
    );
}

#[test]
fn another_schema() {
    let dir = tempfile::tempdir().unwrap();
    let a = write(dir.path(), 0, 10, 10, true);
    let other = batch(1, 10, true).project(&[0, 1, 2]).unwrap();
    let b = write_with(dir.path(), "other.parquet", &other, 10, "test");
    let out = dir.path().join("out.parquet");
    let err = pq_glue::concat(&[a, b], &out).unwrap_err();
    assert!(
        matches!(err, Error::Incompatible { what: "schema", .. }),
        "{err}"
    );
    assert!(!out.exists());
}

#[test]
fn other_key_value_metadata() {
    let dir = tempfile::tempdir().unwrap();
    let a = write(dir.path(), 0, 10, 10, true);
    let b = write_with(
        dir.path(),
        "other.parquet",
        &batch(1, 10, true),
        10,
        "elsewhere",
    );
    let err = pq_glue::concat(&[a, b], &dir.path().join("out.parquet")).unwrap_err();
    assert!(
        matches!(
            err,
            Error::Incompatible {
                what: "key-value metadata",
                ..
            }
        ),
        "{err}"
    );
}

#[test]
fn a_missing_input() {
    let dir = tempfile::tempdir().unwrap();
    let a = write(dir.path(), 0, 10, 10, true);
    let missing = dir.path().join("missing.parquet");
    let err = pq_glue::concat(&[a, missing], &dir.path().join("out.parquet")).unwrap_err();
    assert!(matches!(err, Error::Io { .. }), "{err}");
}

#[test]
fn an_input_that_is_not_parquet() {
    let dir = tempfile::tempdir().unwrap();
    let a = write(dir.path(), 0, 10, 10, true);
    let bad = dir.path().join("bad.parquet");
    std::fs::write(&bad, b"not parquet at all").unwrap();
    let err = pq_glue::concat(&[a, bad], &dir.path().join("out.parquet")).unwrap_err();
    assert!(matches!(err, Error::Parquet { .. }), "{err}");
}
