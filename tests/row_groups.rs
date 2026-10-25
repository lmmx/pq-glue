//! `concat_sources` and `concat_many_sources`: chosen row groups of files, copied as
//! `concat` copies whole files.

mod common;

use std::path::PathBuf;

use common::*;
use pq_glue::{Error, Source};

fn num_row_groups(path: &std::path::Path) -> usize {
    metadata(path).num_row_groups()
}

#[test]
fn every_row_group_in_order_is_the_whole_file() {
    let dir = tempfile::tempdir().unwrap();
    let inputs: Vec<PathBuf> = (0..3)
        .map(|i| write(dir.path(), i, 50 + i * 7, 16, true))
        .collect();
    let whole = dir.path().join("whole.parquet");
    let picked = dir.path().join("picked.parquet");
    let rows = pq_glue::concat(&inputs, &whole).unwrap();
    let sources: Vec<Source> = inputs
        .iter()
        .map(|p| Source::row_groups(p.clone(), (0..num_row_groups(p)).collect()))
        .collect();
    assert_eq!(pq_glue::concat_sources(&sources, &picked).unwrap(), rows);
    assert_eq!(
        std::fs::read(&whole).unwrap(),
        std::fs::read(&picked).unwrap()
    );
}

#[test]
fn row_groups_split_off_glue_back_to_the_file() {
    // Each row group of each file copied to a file of its own, then those glued in
    // order, is the files glued whole
    let dir = tempfile::tempdir().unwrap();
    let inputs: Vec<PathBuf> = (0..2)
        .map(|i| write(dir.path(), i, 70 + i * 9, 16, true))
        .collect();
    let whole = dir.path().join("whole.parquet");
    pq_glue::concat(&inputs, &whole).unwrap();
    let mut parts = Vec::new();
    for (i, p) in inputs.iter().enumerate() {
        for r in 0..num_row_groups(p) {
            let part = dir.path().join(format!("part_{i}_{r}.parquet"));
            pq_glue::concat_sources(&[Source::row_groups(p.clone(), vec![r])], &part).unwrap();
            parts.push(part);
        }
    }
    let again = dir.path().join("again.parquet");
    pq_glue::concat(&parts, &again).unwrap();
    assert_eq!(
        std::fs::read(&whole).unwrap(),
        std::fs::read(&again).unwrap()
    );
    assert_eq!(read(&whole), read(&again));
}

#[test]
fn chosen_row_groups_have_their_rows_in_the_order_chosen() {
    let dir = tempfile::tempdir().unwrap();
    let a = write(dir.path(), 0, 64, 16, false);
    let out = dir.path().join("out.parquet");
    let rows = pq_glue::concat_sources(&[Source::row_groups(a.clone(), vec![3, 1])], &out).unwrap();
    assert_eq!(rows, 32);
    let m = metadata(&out);
    assert_eq!(m.num_row_groups(), 2);
    let src = metadata(&a);
    for (g, r) in [(0, 3), (1, 1)] {
        let (have, want) = (m.row_group(g), src.row_group(r));
        assert_eq!(have.num_rows(), want.num_rows());
        for c in 0..have.num_columns() {
            assert_eq!(have.column(c).statistics(), want.column(c).statistics());
        }
    }
}

#[test]
fn a_row_group_the_file_lacks_is_an_error() {
    let dir = tempfile::tempdir().unwrap();
    let a = write(dir.path(), 0, 40, 16, false);
    let out = dir.path().join("out.parquet");
    let result = pq_glue::concat_sources(&[Source::row_groups(a, vec![0, 3])], &out);
    assert!(matches!(
        result,
        Err(Error::RowGroup {
            row_group: 3,
            row_groups: 3,
            ..
        })
    ));
    assert!(!out.exists());
}

#[cfg(feature = "parallel")]
#[test]
fn jobs_sharing_files_write_what_each_writes_alone() {
    // Every job takes its row groups from the same two files, opened once for all
    let dir = tempfile::tempdir().unwrap();
    let a = write(dir.path(), 0, 100, 10, true);
    let b = write(dir.path(), 1, 60, 10, true);
    let jobs: Vec<(Vec<Source>, PathBuf)> = (0..6)
        .map(|j| {
            let sources = vec![
                Source::row_groups(a.clone(), vec![j, j + 4]),
                Source::row_groups(b.clone(), vec![j]),
            ];
            (sources, dir.path().join(format!("job_{j}.parquet")))
        })
        .collect();
    let rows: Vec<u64> = pq_glue::concat_many_sources(&jobs)
        .into_iter()
        .map(Result::unwrap)
        .collect();
    for ((sources, out), rows) in jobs.iter().zip(rows) {
        assert_eq!(rows, 30);
        let alone = out.with_extension("alone.parquet");
        assert_eq!(pq_glue::concat_sources(sources, &alone).unwrap(), rows);
        assert_eq!(std::fs::read(out).unwrap(), std::fs::read(&alone).unwrap());
    }
}

#[cfg(feature = "parallel")]
#[test]
fn a_shared_file_failing_fails_each_job_naming_it() {
    let dir = tempfile::tempdir().unwrap();
    let a = write(dir.path(), 0, 40, 10, false);
    let missing = dir.path().join("missing.parquet");
    let jobs: Vec<(Vec<Source>, PathBuf)> = vec![
        (
            vec![
                Source::row_groups(a.clone(), vec![0]),
                Source::whole(missing.clone()),
            ],
            dir.path().join("j0.parquet"),
        ),
        (
            vec![
                Source::row_groups(a.clone(), vec![1]),
                Source::whole(missing),
            ],
            dir.path().join("j1.parquet"),
        ),
        (
            vec![Source::row_groups(a.clone(), vec![9])],
            dir.path().join("j2.parquet"),
        ),
        (
            vec![Source::row_groups(a, vec![2, 3])],
            dir.path().join("j3.parquet"),
        ),
    ];
    let results = pq_glue::concat_many_sources(&jobs);
    assert!(matches!(results[0], Err(Error::Io { .. })));
    assert!(matches!(results[1], Err(Error::Io { .. })));
    assert!(matches!(
        results[2],
        Err(Error::RowGroup { row_group: 9, .. })
    ));
    assert_eq!(*results[3].as_ref().unwrap(), 20);
    for (j, ((_, out), result)) in jobs.iter().zip(&results).enumerate() {
        assert_eq!(out.exists(), result.is_ok(), "job {j}");
    }
}
