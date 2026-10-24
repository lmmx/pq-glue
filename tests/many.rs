//! `concat_many`: each job's output is `concat`'s, and a failed job fails alone.

#![cfg(feature = "parallel")]

mod common;

use std::path::PathBuf;

use common::*;
use pq_glue::Error;

/// Jobs of one to four inputs each, of various row counts and row groups, in `dir`.
fn jobs(dir: &std::path::Path, n: usize) -> Vec<(Vec<PathBuf>, PathBuf)> {
    (0..n)
        .map(|j| {
            let sub = dir.join(format!("job_{j}"));
            std::fs::create_dir(&sub).unwrap();
            let inputs = (0..1 + j % 4)
                .map(|i| write(&sub, i, 1 + (j * 37 + i * 11) % 120, 16 + j % 3 * 8, true))
                .collect();
            (inputs, sub.join("out.parquet"))
        })
        .collect()
}

#[test]
fn each_output_is_concats() {
    let dir = tempfile::tempdir().unwrap();
    let jobs = jobs(dir.path(), 24);
    let rows: Vec<u64> = pq_glue::concat_many(&jobs)
        .into_iter()
        .map(Result::unwrap)
        .collect();
    for ((inputs, out), rows) in jobs.iter().zip(rows) {
        let alone = out.with_extension("alone.parquet");
        assert_eq!(pq_glue::concat(inputs, &alone).unwrap(), rows);
        assert_eq!(std::fs::read(out).unwrap(), std::fs::read(&alone).unwrap());
    }
}

#[test]
fn no_jobs() {
    assert!(pq_glue::concat_many(&[]).is_empty());
}

#[test]
fn a_failed_job_fails_alone() {
    let dir = tempfile::tempdir().unwrap();
    let mut jobs = jobs(dir.path(), 5);
    let other = batch(9, 10, true).project(&[0, 1, 2]).unwrap();
    let other = write_with(dir.path(), "other.parquet", &other, 10, "test");
    jobs[1].0.push(other);
    jobs[3].0.push(dir.path().join("missing.parquet"));
    let results = pq_glue::concat_many(&jobs);
    assert!(matches!(
        results[1],
        Err(Error::Incompatible { what: "schema", .. })
    ));
    assert!(matches!(results[3], Err(Error::Io { .. })));
    for (j, ((_, out), result)) in jobs.iter().zip(&results).enumerate() {
        assert_eq!(out.exists(), result.is_ok(), "job {j}");
        assert_eq!(result.is_ok(), j != 1 && j != 3, "job {j}");
    }
}
