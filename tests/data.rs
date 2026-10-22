//! The output holds the inputs' rows, as their encoded bytes.

mod common;

use common::*;

#[test]
fn column_chunks_are_the_inputs_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let inputs: Vec<_> = [(100, 40), (37, 40), (1, 40), (250, 64)]
        .iter()
        .enumerate()
        .map(|(i, &(rows, rg))| write(dir.path(), i, rows, rg, true))
        .collect();
    let out = dir.path().join("out.parquet");
    assert_eq!(pq_glue::concat(&inputs, &out).unwrap(), 388);
    let want: Vec<Vec<u8>> = inputs.iter().flat_map(|p| chunk_bytes(p)).collect();
    assert_eq!(chunk_bytes(&out), want);
}

#[test]
fn rows_decode_as_the_inputs() {
    let dir = tempfile::tempdir().unwrap();
    let inputs: Vec<_> = (0..3)
        .map(|i| write(dir.path(), i, 90 + i, 32, false))
        .collect();
    let out = dir.path().join("out.parquet");
    pq_glue::concat(&inputs, &out).unwrap();
    let want: Vec<_> = inputs.iter().flat_map(|p| read(p)).collect();
    let want = arrow_select_concat(&want);
    assert_eq!(arrow_select_concat(&read(&out)), want);
}

#[test]
fn one_input_is_copied_whole() {
    let dir = tempfile::tempdir().unwrap();
    let input = write(dir.path(), 0, 75, 20, true);
    let out = dir.path().join("out.parquet");
    assert_eq!(
        pq_glue::concat(std::slice::from_ref(&input), &out).unwrap(),
        75
    );
    assert_eq!(chunk_bytes(&out), chunk_bytes(&input));
}

#[test]
fn an_input_without_row_groups_adds_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let empty = write_with(dir.path(), "empty.parquet", &batch(0, 0, true), 10, "test");
    let input = write(dir.path(), 1, 30, 10, true);
    let out = dir.path().join("out.parquet");
    assert_eq!(pq_glue::concat(&[empty, input.clone()], &out).unwrap(), 30);
    assert_eq!(chunk_bytes(&out), chunk_bytes(&input));
}

/// The batches as one, row for row (RecordBatch equality compares values).
fn arrow_select_concat(batches: &[arrow_array::RecordBatch]) -> Vec<String> {
    batches
        .iter()
        .flat_map(|b| (0..b.num_rows()).map(move |r| format!("{:?}", b.slice(r, 1))))
        .collect()
}
