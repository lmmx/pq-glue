//! Input files for the integration tests, written with arrow-rs.

#![allow(dead_code)]

use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow_array::builder::{Float64Builder, ListBuilder, StringBuilder, StructBuilder};
use arrow_array::{ArrayRef, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Fields, Schema};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::file::metadata::{ParquetMetaData, ParquetMetaDataReader};
use parquet::file::properties::{EnabledStatistics, WriterProperties};

/// Floats that statistics and orders treat specially, cycled through the rows.
pub const SPECIAL: [f64; 7] = [
    f64::NAN,
    -0.0,
    0.0,
    1.5,
    -2.25,
    f64::INFINITY,
    f64::NEG_INFINITY,
];

fn snak_fields() -> Fields {
    Fields::from(vec![
        Field::new("p", DataType::Utf8, true),
        Field::new_list("v", Field::new_list_field(DataType::Float64, true), true),
    ])
}

pub fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("x", DataType::Float64, true),
        Field::new("n", DataType::Int64, true),
        Field::new_list(
            "refs",
            Field::new_list_field(DataType::Struct(snak_fields()), true),
            true,
        ),
    ]))
}

/// File `i`'s rows: nulls, null and empty lists, nested lists of structs of lists, and
/// the SPECIAL floats (`nan: false` leaves NaN out, for comparing decoded values).
pub fn batch(i: usize, rows: usize, nan: bool) -> RecordBatch {
    let x = |r: usize| {
        let v = SPECIAL[(r + i) % SPECIAL.len()];
        if v.is_nan() && !nan {
            7.0
        } else {
            v
        }
    };
    let ids = StringArray::from_iter_values((0..rows).map(|r| format!("Q{i}-{r:05}")));
    let xs = Float64Array::from_iter((0..rows).map(|r| (r % 4 != 0).then(|| x(r))));
    let ns = Int64Array::from_iter((0..rows).map(|r| (r % 6 != 0).then_some(r as i64 - 50)));
    let mut refs = ListBuilder::new(StructBuilder::new(
        snak_fields(),
        vec![
            Box::new(StringBuilder::new()),
            Box::new(ListBuilder::new(Float64Builder::new())),
        ],
    ))
    .with_field(Field::new_list_field(DataType::Struct(snak_fields()), true));
    for r in 0..rows {
        if r % 7 == 0 {
            refs.append(false);
            continue;
        }
        if r % 5 != 0 {
            let s = refs.values();
            s.field_builder::<StringBuilder>(0)
                .unwrap()
                .append_value(format!("P{}", r % 11));
            let v = s.field_builder::<ListBuilder<Float64Builder>>(1).unwrap();
            v.values().append_value(x(r));
            v.values().append_null();
            v.values().append_value(-0.0);
            v.append(true);
            s.append(true);
        }
        refs.append(true);
    }
    let columns: Vec<ArrayRef> = vec![
        Arc::new(ids),
        Arc::new(xs),
        Arc::new(ns),
        Arc::new(refs.finish()),
    ];
    RecordBatch::try_new(schema(), columns).unwrap()
}

/// Write `batch(i, rows, nan)` to `dir/in_{i}.parquet` in row groups of `rg` rows,
/// with page statistics (so page indexes) and an extra key-value pair.
pub fn write(dir: &Path, i: usize, rows: usize, rg: usize, nan: bool) -> PathBuf {
    write_with(
        dir,
        &format!("in_{i}.parquet"),
        &batch(i, rows, nan),
        rg,
        "test",
    )
}

pub fn write_with(dir: &Path, name: &str, batch: &RecordBatch, rg: usize, origin: &str) -> PathBuf {
    let path = dir.join(name);
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(rg))
        .set_statistics_enabled(EnabledStatistics::Page)
        .set_bloom_filter_enabled(true)
        .set_key_value_metadata(Some(vec![parquet::file::metadata::KeyValue::new(
            "origin".into(),
            origin.to_string(),
        )]))
        .build();
    let mut w =
        ArrowWriter::try_new(File::create(&path).unwrap(), batch.schema(), Some(props)).unwrap();
    w.write(batch).unwrap();
    w.close().unwrap();
    path
}

pub fn metadata(path: &Path) -> ParquetMetaData {
    ParquetMetaDataReader::new()
        .with_page_index_policy(parquet::file::metadata::PageIndexPolicy::Optional)
        .parse_and_finish(&File::open(path).unwrap())
        .unwrap()
}

/// The file's rows, decoded by arrow-rs.
pub fn read(path: &Path) -> Vec<RecordBatch> {
    ParquetRecordBatchReaderBuilder::try_new(File::open(path).unwrap())
        .unwrap()
        .build()
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap()
}

/// Every column chunk's bytes, row group by row group.
pub fn chunk_bytes(path: &Path) -> Vec<Vec<u8>> {
    let data = std::fs::read(path).unwrap();
    let md = metadata(path);
    md.row_groups()
        .iter()
        .flat_map(|rg| rg.columns().iter())
        .map(|c| {
            let (start, len) = c.byte_range();
            data[start as usize..(start + len) as usize].to_vec()
        })
        .collect()
}
