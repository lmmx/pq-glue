//! `concat_size`: the size of the file `concat` writes, without writing it.

mod common;

use std::path::{Path, PathBuf};

use common::*;
use pq_glue::Error;

fn size(path: &Path) -> u64 {
    std::fs::metadata(path).unwrap().len()
}

/// `n` sets of one to four inputs each, of various row counts and row groups, with page
/// indexes and bloom filters, in `dir`.
fn sets(dir: &Path, n: usize) -> Vec<Vec<PathBuf>> {
    (0..n)
        .map(|j| {
            let sub = dir.join(format!("set_{j}"));
            std::fs::create_dir(&sub).unwrap();
            (0..1 + j % 4)
                .map(|i| write(&sub, i, 1 + (j * 37 + i * 11) % 120, 16 + j % 3 * 8, true))
                .collect()
        })
        .collect()
}

#[test]
fn the_size_of_concats_file() {
    let dir = tempfile::tempdir().unwrap();
    for (j, inputs) in sets(dir.path(), 24).iter().enumerate() {
        let out = dir.path().join(format!("out_{j}.parquet"));
        pq_glue::concat(inputs, &out).unwrap();
        assert_eq!(pq_glue::concat_size(inputs).unwrap(), size(&out), "set {j}");
    }
}

/// Offsets in the footer and page indexes are varints, so their lengths change with
/// where the column chunks land: inputs of a few MB, glued past the 1 MiB and 2 MiB
/// marks, and many row groups (the row groups' ordinals and the list's header).
#[test]
fn the_size_with_offsets_of_more_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let inputs: Vec<_> = (0..3)
        .map(|i| write(dir.path(), i, 40_000 + 1_000 * i, 307, true))
        .collect();
    assert!(inputs.iter().map(|p| size(p)).sum::<u64>() > 2 << 20);
    for k in 1..=inputs.len() {
        let out = dir.path().join(format!("out_{k}.parquet"));
        pq_glue::concat(&inputs[..k], &out).unwrap();
        assert!(metadata(&out).num_row_groups() > 64);
        assert_eq!(
            pq_glue::concat_size(&inputs[..k]).unwrap(),
            size(&out),
            "{k} inputs"
        );
    }
}

#[test]
fn errors_as_concats() {
    let dir = tempfile::tempdir().unwrap();
    assert!(matches!(pq_glue::concat_size(&[]), Err(Error::NoInputs)));
    let a = write(dir.path(), 0, 10, 10, true);
    let other = batch(1, 10, true).project(&[0, 1, 2]).unwrap();
    let other = write_with(dir.path(), "other.parquet", &other, 10, "test");
    assert!(matches!(
        pq_glue::concat_size(&[a.clone(), other]),
        Err(Error::Incompatible { what: "schema", .. })
    ));
    let missing = dir.path().join("missing.parquet");
    assert!(matches!(
        pq_glue::concat_size(&[a, missing]),
        Err(Error::Io { .. })
    ));
}
