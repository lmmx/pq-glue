//! The output's footer: schema, key-value metadata, column orders, row groups and their
//! statistics, page indexes and bloom filters, as the inputs have them.

mod common;

use common::*;
use parquet::basic::ColumnOrder;

fn glued(dir: &std::path::Path) -> (Vec<std::path::PathBuf>, std::path::PathBuf) {
    let inputs: Vec<_> = (0..3)
        .map(|i| write(dir, i, 120 + 7 * i, 50, true))
        .collect();
    let out = dir.join("out.parquet");
    pq_glue::concat(&inputs, &out).unwrap();
    (inputs, out)
}

#[test]
fn schema_and_key_value_metadata_are_the_first_inputs() {
    let dir = tempfile::tempdir().unwrap();
    let (inputs, out) = glued(dir.path());
    let (a, b) = (metadata(&inputs[0]), metadata(&out));
    assert_eq!(b.file_metadata().schema(), a.file_metadata().schema());
    assert_eq!(
        b.file_metadata().key_value_metadata(),
        a.file_metadata().key_value_metadata()
    );
    let keys: Vec<_> = b
        .file_metadata()
        .key_value_metadata()
        .unwrap()
        .iter()
        .map(|kv| kv.key.as_str())
        .collect();
    assert!(
        keys.contains(&"ARROW:schema") && keys.contains(&"origin"),
        "{keys:?}"
    );
}

#[test]
fn column_orders_are_the_inputs() {
    let dir = tempfile::tempdir().unwrap();
    let (inputs, out) = glued(dir.path());
    let want = metadata(&inputs[0])
        .file_metadata()
        .column_orders()
        .cloned();
    assert_eq!(
        metadata(&out).file_metadata().column_orders().cloned(),
        want
    );
    // arrow-rs declares float columns IEEE 754 ordered, so this checks they are kept as
    // the inputs have them, not merely as the writer would set them
    assert!(want.unwrap().contains(&ColumnOrder::IEEE_754_TOTAL_ORDER));
}

#[test]
fn row_groups_and_statistics_are_the_inputs() {
    let dir = tempfile::tempdir().unwrap();
    let (inputs, out) = glued(dir.path());
    let md = metadata(&out);
    let want: Vec<_> = inputs
        .iter()
        .flat_map(|p| metadata(p).row_groups().to_vec())
        .collect();
    assert_eq!(md.num_row_groups(), want.len());
    for (got, want) in md.row_groups().iter().zip(&want) {
        assert_eq!(got.num_rows(), want.num_rows());
        for (g, w) in got.columns().iter().zip(want.columns()) {
            assert_eq!(g.statistics(), w.statistics(), "{}", g.column_path());
            assert_eq!(g.encodings_mask(), w.encodings_mask());
            assert_eq!(g.compression(), w.compression());
            assert_eq!(g.compressed_size(), w.compressed_size());
        }
    }
}

#[test]
fn page_indexes_and_bloom_filters_are_copied() {
    let dir = tempfile::tempdir().unwrap();
    let (inputs, out) = glued(dir.path());
    let got = metadata(&out);
    let index = got.page_index().expect("the output has page indexes");
    let mut k = 0;
    for input in &inputs {
        let md = metadata(input);
        let want = md.page_index().unwrap();
        for r in 0..md.num_row_groups() {
            for c in 0..md.row_group(r).num_columns() {
                assert_eq!(index.column_index(k, c), want.column_index(r, c));
                assert_eq!(
                    index.offset_index(k, c).map(|o| o.page_locations().len()),
                    want.offset_index(r, c).map(|o| o.page_locations().len())
                );
                assert!(
                    got.row_group(k).column(c).bloom_filter_offset().is_some()
                        == md.row_group(r).column(c).bloom_filter_offset().is_some()
                );
            }
            k += 1;
        }
    }
}
