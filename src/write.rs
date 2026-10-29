//! Arrow record batches written to Parquet (feature `write`): polars-genson's writer,
//! moved here so it serves genson and Python callers alike.
//!
//! [`write_batches`] writes as genson writes its own output by default (see
//! [`WriteOptions`]): zstd at level 1, a dictionary for each leaf column but those whose
//! values are mostly distinct, data pages ended at the page size limit alone, and each
//! row group's leaf columns encoded in parallel. With content-defined chunking, whose
//! chunker parquet-rs keeps private to its own writer, row groups are encoded in
//! parallel instead, each by an `ArrowWriter` of its own into memory, and glued in order
//! (the [`concat`](crate::concat()) copy): each row group's chunking starts afresh, as
//! Arrow C++ (pyarrow) chunks.

use std::fs::File;
use std::path::{Path, PathBuf};

use arrow_array::cast::AsArray;
use arrow_array::{Array, ArrayRef, RecordBatch};
use arrow_schema::{DataType, SchemaRef};
use bytes::Bytes;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, Encoding, Type as PhysicalType, ZstdLevel};
use parquet::file::metadata::SortingColumn;
use parquet::file::properties::{CdcOptions, WriterProperties};
use rayon::prelude::*;

use crate::concat::{Footer, Glue};
use crate::error::{Error, Result};
use crate::input::{check_same, Input};

/// How [`write_batches`] writes. The default is how polars-genson writes its output.
#[derive(Clone, Debug)]
pub struct WriteOptions {
    /// Rows to a row group; arrow-rs's default (1,048,576) if `None`.
    pub max_row_group_rows: Option<usize>,
    /// The Parquet schema's list and map groups take the specification's names (`list`,
    /// `element`, `key_value`), as pyarrow and Polars write them, in place of the Arrow
    /// fields' (arrow-rs's `coerce_types`, which changes nothing else for Arrow types
    /// that Parquet has).
    pub spec_names: bool,
    /// zstd at level 1 by default: on Wikidata claims, typed output a third the size of
    /// uncompressed in the same write time, and normalised JSON an eighth.
    pub compression: Compression,
    /// Leaf columns whose values are mostly distinct are not dictionary encoded (see
    /// [`unrepeated_leaves`]); with `false`, every leaf column is.
    pub adaptive_dictionary: bool,
    /// The sorting columns each row group declares.
    pub sorting_columns: Option<Vec<SortingColumn>>,
    /// Content-defined chunking of each column's data pages, so pages of unchanged
    /// rows repeat from one file to the next (for deduplicating storage).
    pub content_defined_chunking: Option<CdcOptions>,
    /// At most this many threads encode at once, in a pool of the write's own; `None`
    /// uses the calling rayon pool (by default one thread per CPU). Each thread holds a
    /// row group's levels and encoded pages in flight, so memory goes with it. The
    /// output is the same whatever the threads.
    pub max_workers: Option<usize>,
    /// Leaf columns encoded with an encoding of their own, each named by its path in the
    /// Parquet schema (`"id"`, `"a.b"`, `"xs.list.element"` with `spec_names`), and
    /// written without a dictionary: `DELTA_BYTE_ARRAY` for strings that share a prefix
    /// with the value before (ids, or keys that start with their row's id), for one. Only
    /// the bytes change: the values read back, their statistics and the schema are the
    /// same. See [`check_encoding`] for the encodings each physical type takes.
    pub column_encodings: Vec<(String, Encoding)>,
}

impl Default for WriteOptions {
    fn default() -> Self {
        WriteOptions {
            max_row_group_rows: None,
            spec_names: false,
            compression: Compression::ZSTD(ZstdLevel::try_new(1).expect("1 is a zstd level")),
            adaptive_dictionary: true,
            sorting_columns: None,
            content_defined_chunking: None,
            max_workers: None,
            column_encodings: Vec::new(),
        }
    }
}

/// Write `batches`, of `schema`, to `path` (created or truncated) as `options` say, and
/// return the rows written. The output is removed if the write fails.
pub fn write_batches(
    path: &Path,
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
    options: &WriteOptions,
) -> Result<u64> {
    match options.max_workers {
        Some(n) => rayon::ThreadPoolBuilder::new()
            .num_threads(n.max(1))
            .build()
            .map_err(|e| Error::Pool(e.to_string()))?
            .install(|| write_in_pool(path, schema, batches, options)),
        None => write_in_pool(path, schema, batches, options),
    }
}

fn write_in_pool(
    path: &Path,
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
    options: &WriteOptions,
) -> Result<u64> {
    let props = properties(&schema, &batches, options)?;
    let rows = batches.iter().map(|b| b.num_rows() as u64).sum();
    let row_groups = row_groups(
        batches,
        props.max_row_group_row_count().unwrap_or(usize::MAX),
    );
    let result = if props.content_defined_chunking().is_some() && !row_groups.is_empty() {
        write_glued(path, &schema, row_groups, props)
    } else {
        write_columns(path, &schema, row_groups, props)
    };
    if result.is_err() {
        let _ = std::fs::remove_file(path);
    }
    result.map(|()| rows)
}

/// The writer properties for `batches`: with `spec_names`, the Parquet schema's list
/// and map groups take the specification's names, leaf columns whose values are mostly
/// distinct are not dictionary encoded (see [`unrepeated_leaves`]), and a data page ends
/// at arrow-rs's page size limit (1 MiB) alone, as Polars' do, rather than also at
/// 20,000 rows: zstd compresses each page on its own, and pages of 20,000 fixed-width
/// values (160 kB of Int64) compress worse. Each of `column_encodings`' columns takes its
/// encoding, with no dictionary (an error names a column the schema lacks, or an encoding
/// its physical type does not take).
fn properties(
    schema: &SchemaRef,
    batches: &[RecordBatch],
    options: &WriteOptions,
) -> Result<WriterProperties> {
    let mut props = WriterProperties::builder()
        .set_compression(options.compression)
        .set_coerce_types(options.spec_names)
        .set_data_page_row_count_limit(usize::MAX)
        .set_sorting_columns(options.sorting_columns.clone())
        .set_content_defined_chunking(options.content_defined_chunking);
    if let Some(rows) = options.max_row_group_rows {
        props = props.set_max_row_group_row_count(Some(rows));
    }
    if options.adaptive_dictionary {
        for column in unrepeated_leaves(schema, options.spec_names, batches) {
            props = props.set_column_dictionary_enabled(column, false);
        }
    }
    for (column, encoding) in leaf_encodings(schema, options.spec_names, &options.column_encodings)?
    {
        props = props
            .set_column_dictionary_enabled(column.clone(), false)
            .set_column_encoding(column, encoding);
    }
    Ok(props.build())
}

/// The leaf columns of `schema` (its Parquet schema, with `spec_names` as
/// [`WriteOptions::spec_names`]) that `column_encodings` names by path, each with its
/// encoding: an error names a column the schema lacks, or an encoding its physical type
/// does not take ([`check_encoding`]). So a caller can check a write's
/// [`WriteOptions::column_encodings`] before it writes.
pub fn leaf_encodings(
    schema: &SchemaRef,
    spec_names: bool,
    column_encodings: &[(String, Encoding)],
) -> Result<Vec<(parquet::schema::types::ColumnPath, Encoding)>> {
    if column_encodings.is_empty() {
        return Ok(Vec::new());
    }
    let descr = parquet::arrow::ArrowSchemaConverter::new()
        .with_coerce_types(spec_names)
        .convert(schema)
        .map_err(|e| Error::Option(format!("no Parquet schema for the columns to encode: {e}")))?;
    column_encodings
        .iter()
        .map(|(name, encoding)| {
            let column = (0..descr.num_columns())
                .map(|i| descr.column(i))
                .find(|c| c.path().string() == *name)
                .ok_or_else(|| Error::Option(format!("no leaf column {name:?} to encode")))?;
            check_encoding(name, column.physical_type(), *encoding)?;
            Ok((column.path().clone(), *encoding))
        })
        .collect()
}

/// Whether a leaf column of `physical` type can be written in `encoding` (an error
/// naming the `column` if not), as the Parquet format specifies them and arrow-rs writes
/// them: PLAIN any type; RLE booleans; DELTA_BINARY_PACKED 32 and 64-bit integers;
/// DELTA_LENGTH_BYTE_ARRAY byte arrays; DELTA_BYTE_ARRAY byte arrays and fixed-length
/// ones; BYTE_STREAM_SPLIT floats, doubles, 32 and 64-bit integers and fixed-length byte
/// arrays; ALP floats and doubles. A dictionary is not an encoding asked for here
/// (`adaptive_dictionary` decides it), nor the deprecated BIT_PACKED.
pub fn check_encoding(column: &str, physical: PhysicalType, encoding: Encoding) -> Result<()> {
    use PhysicalType as T;
    let ok = match encoding {
        Encoding::PLAIN => true,
        Encoding::RLE => physical == T::BOOLEAN,
        Encoding::DELTA_BINARY_PACKED => matches!(physical, T::INT32 | T::INT64),
        Encoding::DELTA_LENGTH_BYTE_ARRAY => physical == T::BYTE_ARRAY,
        Encoding::DELTA_BYTE_ARRAY => {
            matches!(physical, T::BYTE_ARRAY | T::FIXED_LEN_BYTE_ARRAY)
        }
        Encoding::BYTE_STREAM_SPLIT => matches!(
            physical,
            T::FLOAT | T::DOUBLE | T::INT32 | T::INT64 | T::FIXED_LEN_BYTE_ARRAY
        ),
        Encoding::ALP => matches!(physical, T::FLOAT | T::DOUBLE),
        _ => {
            return Err(Error::Option(format!(
                "{column:?}: {encoding} is not an encoding to ask for (a dictionary is \
                 adaptive_dictionary's)"
            )))
        }
    };
    if ok {
        Ok(())
    } else {
        Err(Error::Option(format!(
            "{column:?}: {encoding} does not encode {physical} values"
        )))
    }
}

/// Values sampled from each leaf column, from its first rows
const DICTIONARY_SAMPLE: usize = 16_384;

/// The Parquet leaf columns of `batches` whose sampled values are more than three
/// quarters distinct (of at least 128 sampled), for which a dictionary costs a hash per
/// value and grows the file until arrow-rs gives it up at its page size limit: Polars
/// skips dictionaries at the same ratio, and no other ratio from 0 to 1 measured better
/// across flat, nested and Wikidata tables (polars-genson's
/// docs/journal/2026-10-21-adaptive-dictionary.md). None when the Arrow leaves do not
/// line up with the Parquet leaf columns one to one.
fn unrepeated_leaves(
    schema: &SchemaRef,
    spec_names: bool,
    batches: &[RecordBatch],
) -> Vec<parquet::schema::types::ColumnPath> {
    let Ok(descr) = parquet::arrow::ArrowSchemaConverter::new()
        .with_coerce_types(spec_names)
        .convert(schema)
    else {
        return Vec::new();
    };
    // Each batch's leaf arrays, in Parquet leaf order
    let leaves: Vec<Vec<ArrayRef>> = batches
        .iter()
        .map(|batch| {
            let mut leaves = Vec::new();
            for column in batch.columns() {
                leaf_arrays(column, &mut leaves);
            }
            leaves
        })
        .collect();
    if leaves.iter().any(|l| l.len() != descr.num_columns()) {
        return Vec::new();
    }
    (0..descr.num_columns())
        .into_par_iter()
        .filter(|&i| {
            let mut seen = rustc_hash::FxHashSet::default();
            let mut sampled = 0;
            for batch_leaves in &leaves {
                if !sample(&batch_leaves[i], &mut seen, &mut sampled) {
                    return false;
                }
                if sampled >= DICTIONARY_SAMPLE {
                    break;
                }
            }
            sampled >= 128 && seen.len() * 4 > sampled * 3
        })
        .map(|i| descr.column(i).path().clone())
        .collect()
}

/// The leaf arrays under `array`, depth first, as Parquet's leaf columns are ordered.
fn leaf_arrays(array: &ArrayRef, out: &mut Vec<ArrayRef>) {
    match array.data_type() {
        DataType::Struct(_) => array
            .as_struct()
            .columns()
            .iter()
            .for_each(|c| leaf_arrays(c, out)),
        DataType::List(_) => leaf_arrays(array.as_list::<i32>().values(), out),
        DataType::LargeList(_) => leaf_arrays(array.as_list::<i64>().values(), out),
        DataType::FixedSizeList(..) => leaf_arrays(array.as_fixed_size_list().values(), out),
        DataType::Map(..) => {
            let entries = array.as_map().entries();
            entries.columns().iter().for_each(|c| leaf_arrays(c, out))
        }
        _ => out.push(array.clone()),
    }
}

/// Hash `array`'s non-null values into `seen` until `sampled` reaches the sample size;
/// false for a type not sampled (its dictionary setting is left as it is).
fn sample(array: &ArrayRef, seen: &mut rustc_hash::FxHashSet<u64>, sampled: &mut usize) -> bool {
    use arrow_array::types::*;
    use xxhash_rust::xxh64::xxh64;
    let mut take = |bytes: &[u8]| {
        seen.insert(xxh64(bytes, 0));
        *sampled += 1;
        *sampled < DICTIONARY_SAMPLE
    };
    macro_rules! each {
        ($iter:expr, $bytes:expr) => {{
            for v in $iter.flatten() {
                let bytes = $bytes(v);
                if !take(AsRef::<[u8]>::as_ref(&bytes)) {
                    break;
                }
            }
        }};
    }
    match array.data_type() {
        DataType::Utf8 => each!(array.as_string::<i32>().iter(), str::as_bytes),
        DataType::LargeUtf8 => each!(array.as_string::<i64>().iter(), str::as_bytes),
        DataType::Utf8View => each!(array.as_string_view().iter(), str::as_bytes),
        DataType::Binary => each!(array.as_binary::<i32>().iter(), |b| b),
        DataType::LargeBinary => each!(array.as_binary::<i64>().iter(), |b| b),
        DataType::Int32 => each!(array.as_primitive::<Int32Type>().iter(), |v: i32| v
            .to_le_bytes()),
        DataType::Int64 => each!(array.as_primitive::<Int64Type>().iter(), |v: i64| v
            .to_le_bytes()),
        DataType::UInt32 => each!(array.as_primitive::<UInt32Type>().iter(), |v: u32| v
            .to_le_bytes()),
        DataType::UInt64 => each!(array.as_primitive::<UInt64Type>().iter(), |v: u64| v
            .to_le_bytes()),
        DataType::Float32 => each!(array.as_primitive::<Float32Type>().iter(), |v: f32| v
            .to_le_bytes()),
        DataType::Float64 => each!(array.as_primitive::<Float64Type>().iter(), |v: f64| v
            .to_le_bytes()),
        _ => return false,
    }
    true
}

/// `batches` cut into row groups of `max_rows` rows (the last the rest), as
/// `ArrowWriter` cuts them.
fn row_groups(batches: Vec<RecordBatch>, max_rows: usize) -> Vec<Vec<RecordBatch>> {
    let mut row_groups: Vec<Vec<RecordBatch>> = Vec::new();
    let mut current: Vec<RecordBatch> = Vec::new();
    let mut buffered = 0;
    for batch in batches {
        let mut remaining = batch;
        while remaining.num_rows() > 0 {
            let rows = (max_rows - buffered).min(remaining.num_rows());
            current.push(remaining.slice(0, rows));
            remaining = remaining.slice(rows, remaining.num_rows() - rows);
            buffered += rows;
            if buffered >= max_rows {
                row_groups.push(std::mem::take(&mut current));
                buffered = 0;
            }
        }
    }
    if !current.is_empty() {
        row_groups.push(current);
    }
    row_groups
}

/// Write `row_groups` to `path`: the same file, byte for byte, as `ArrowWriter::write` of
/// each batch and `close` write, with the work done in parallel. Each column's writer is
/// handed its leaf of each batch in turn, as `ArrowWriter`'s are (`props` sets no row
/// group byte limit or content-defined chunking).
///
/// Row groups are encoded a few at a time, up to one per thread, each row group's leaf
/// columns (and each batch's levels) a task of their own: a wide table's columns fill the
/// threads within one row group, and a narrow table's row groups fill them side by side.
/// The encoded chunks are then written in row group order, so at most that many row
/// groups' encoded chunks are held at once.
///
/// Levels are computed for up to one batch per thread at a time, and written to the column
/// writers before the next batches': a batch's levels (two `i16` and a non-null index per
/// leaf value, for every leaf column) are much larger than its encoded chunks, so a wave
/// holds that many batches' levels at a time, not all of its row groups'.
fn write_columns(
    path: &Path,
    schema: &SchemaRef,
    row_groups: Vec<Vec<RecordBatch>>,
    props: WriterProperties,
) -> Result<()> {
    use parquet::arrow::arrow_writer::{
        compute_leaves, ArrowColumnChunk, ArrowColumnWriter, ArrowLeafColumn,
    };
    let failed = Error::parquet;
    let file = File::create(path).map_err(Error::io(path))?;
    let writer = ArrowWriter::try_new(file, schema.clone(), Some(props)).map_err(failed(path))?;
    let (mut file_writer, factory) = writer.into_serialized_writer().map_err(failed(path))?;
    let props = file_writer.properties();
    debug_assert!(props.max_row_group_bytes().is_none());
    debug_assert!(row_groups.is_empty() || props.content_defined_chunking().is_none());
    let in_flight = rayon::current_num_threads().max(1);
    for (wave, groups) in row_groups.chunks(in_flight).enumerate() {
        let first = wave * in_flight;
        let writers: Vec<Vec<ArrowColumnWriter>> = (0..groups.len())
            .map(|g| factory.create_column_writers(first + g))
            .collect::<std::result::Result<_, _>>()
            .map_err(failed(path))?;
        let columns = writers.first().map_or(0, Vec::len);
        // One task per (row group, leaf column), in row group then column order
        let mut tasks: Vec<(usize, usize, ArrowColumnWriter)> = writers
            .into_iter()
            .enumerate()
            .flat_map(|(g, ws)| ws.into_iter().enumerate().map(move |(i, w)| (g, i, w)))
            .collect();
        // Each step takes the next `per_group` batches of every row group in the wave, so
        // at most about one batch per thread has its levels held at once
        let per_group = (in_flight / groups.len()).max(1);
        let steps = groups
            .iter()
            .map(Vec::len)
            .max()
            .unwrap_or(0)
            .div_ceil(per_group);
        for step in 0..steps {
            // Each row group's step batches' leaf columns, in the order of the column writers
            let leaves: Vec<Vec<Vec<ArrowLeafColumn>>> = groups
                .par_iter()
                .map(|batches| {
                    let end = ((step + 1) * per_group).min(batches.len());
                    batches[(step * per_group).min(end)..end]
                        .par_iter()
                        .map(|batch| {
                            let mut leaves = Vec::with_capacity(columns);
                            for (field, column) in schema.fields().iter().zip(batch.columns()) {
                                leaves.extend(compute_leaves(field, column)?);
                            }
                            Ok(leaves)
                        })
                        .collect::<std::result::Result<_, parquet::errors::ParquetError>>()
                })
                .collect::<std::result::Result<_, _>>()
                .map_err(failed(path))?;
            tasks
                .par_iter_mut()
                .try_for_each(|(g, i, writer)| {
                    for batch_leaves in &leaves[*g] {
                        writer.write(&batch_leaves[*i])?;
                    }
                    Ok::<_, parquet::errors::ParquetError>(())
                })
                .map_err(failed(path))?;
        }
        let chunks: Vec<ArrowColumnChunk> = tasks
            .into_par_iter()
            .map(|(_, _, writer)| writer.close())
            .collect::<std::result::Result<_, _>>()
            .map_err(failed(path))?;
        let mut chunks = chunks.into_iter();
        for _ in groups {
            let mut row_group = file_writer.next_row_group().map_err(failed(path))?;
            for chunk in chunks.by_ref().take(columns) {
                chunk
                    .append_to_row_group(&mut row_group)
                    .map_err(failed(path))?;
            }
            row_group.close().map_err(failed(path))?;
        }
    }
    file_writer.close().map_err(failed(path))?;
    Ok(())
}

/// Write `row_groups` to `path` with content-defined chunking: up to one row group per
/// thread at a time, each encoded by an `ArrowWriter` of its own into memory (one row
/// group, its chunking started afresh), then glued in row group order, so at most that
/// many row groups' encoded bytes are held at once. The footer declares what each row
/// group's writer did: parquet-rs's `created_by`, the format version and the sorting
/// columns of `props`.
fn write_glued(
    path: &Path,
    schema: &SchemaRef,
    row_groups: Vec<Vec<RecordBatch>>,
    props: WriterProperties,
) -> Result<()> {
    let footer = Footer {
        created_by: Some(props.created_by().to_string()),
        version: Some(props.writer_version()),
        sorting_columns: props.sorting_columns().cloned(),
    };
    // Each row group is one writer's whole output
    let props = props
        .into_builder()
        .set_max_row_group_row_count(Some(usize::MAX))
        .build();
    let encode = |i: usize, batches: &[RecordBatch]| -> Result<Input<Bytes>> {
        let name = PathBuf::from(format!("{} (row group {i})", path.display()));
        let mut writer = ArrowWriter::try_new(Vec::new(), schema.clone(), Some(props.clone()))
            .map_err(Error::parquet(&name))?;
        for batch in batches {
            writer.write(batch).map_err(Error::parquet(&name))?;
        }
        let bytes = writer.into_inner().map_err(Error::parquet(&name))?;
        Input::new(name, Bytes::from(bytes))
    };
    let in_flight = rayon::current_num_threads().max(1);
    // The glue, and the first row group, which the others must match
    let mut glued: Option<(Glue, Input<Bytes>)> = None;
    for (wave, groups) in row_groups.chunks(in_flight).enumerate() {
        let parts: Vec<Input<Bytes>> = groups
            .par_iter()
            .enumerate()
            .map(|(g, batches)| encode(wave * in_flight + g, batches))
            .collect::<Result<_>>()?;
        for part in parts {
            match &mut glued {
                Some((glue, first)) => {
                    check_same(first, &part)?;
                    glue.append(&part)?;
                }
                None => {
                    let mut glue = Glue::create(path, &part, footer.clone())?;
                    glue.append(&part)?;
                    glued = Some((glue, part));
                }
            }
        }
    }
    let (glue, _) = glued.expect("write_batches glues only when there are row groups");
    glue.finish().map(|_| ())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow_array::{Float64Array, Int64Array, ListArray, StringArray, StructArray};
    use arrow_schema::{Field, Fields, Schema};
    use parquet::arrow::arrow_reader::{
        ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReaderBuilder,
    };
    use parquet::file::metadata::{PageIndexPolicy, ParquetMetaData, ParquetMetaDataReader};
    use parquet::file::properties::EnabledStatistics;
    use tempfile::NamedTempFile;

    fn zstd1() -> Compression {
        WriteOptions::default().compression
    }

    /// `rows` rows of an `id` column and a struct column, `claims`, of a string, a list of
    /// integers and a struct of a float, any of them null, in batches of uneven sizes (some
    /// of them empty).
    fn struct_batches(rows: usize, seed: u64) -> (SchemaRef, Vec<RecordBatch>) {
        use arrow_array::types::Int64Type;
        use arrow_buffer::NullBuffer;
        let mut state = seed;
        let mut next = move || {
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            (state >> 33) as usize
        };
        let draws: Vec<[usize; 4]> = (0..rows)
            .map(|_| [next(), next(), next(), next()])
            .collect();
        let s: ArrayRef = Arc::new(StringArray::from_iter(
            draws
                .iter()
                .map(|d| (d[0] % 5 != 0).then(|| "s".repeat(d[0] % 13))),
        ));
        let xs: ArrayRef = Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>(
            draws.iter().map(|d| {
                (d[1] % 7 != 0).then(|| {
                    (0..d[1] % 6)
                        .map(|j| (j != 3).then_some((d[1] * j) as i64))
                        .collect::<Vec<_>>()
                })
            }),
        ));
        let v: ArrayRef = Arc::new(Float64Array::from_iter(
            draws
                .iter()
                .map(|d| (d[2] % 4 != 0).then_some(d[2] as f64 / 7.0)),
        ));
        let inner_fields = Fields::from(vec![Field::new("v", DataType::Float64, true)]);
        let inner_nulls = NullBuffer::from(draws.iter().map(|d| d[3] % 6 != 0).collect::<Vec<_>>());
        let inner: ArrayRef = Arc::new(
            StructArray::try_new(inner_fields.clone(), vec![v], Some(inner_nulls)).unwrap(),
        );
        let claims_fields = Fields::from(vec![
            Field::new("s", DataType::Utf8, true),
            Field::new("xs", xs.data_type().clone(), true),
            Field::new("inner", DataType::Struct(inner_fields), true),
        ]);
        let claims_nulls =
            NullBuffer::from(draws.iter().map(|d| d[3] % 9 != 0).collect::<Vec<_>>());
        let claims: ArrayRef = Arc::new(
            StructArray::try_new(
                claims_fields.clone(),
                vec![s, xs, inner],
                Some(claims_nulls),
            )
            .unwrap(),
        );
        let ids: ArrayRef = Arc::new(StringArray::from_iter_values(
            (0..rows).map(|i| format!("Q{i:06}")),
        ));
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("claims", DataType::Struct(claims_fields), true),
        ]));
        let mut batches = vec![RecordBatch::new_empty(schema.clone())];
        let mut from = 0;
        while from < rows {
            let len = (next() % 23).min(rows - from);
            batches.push(
                RecordBatch::try_new(
                    schema.clone(),
                    vec![ids.slice(from, len), claims.slice(from, len)],
                )
                .unwrap(),
            );
            from += len;
        }
        (schema, batches)
    }

    fn arrow_writer(
        schema: &SchemaRef,
        batches: &[RecordBatch],
        props: WriterProperties,
    ) -> Vec<u8> {
        let mut writer = ArrowWriter::try_new(Vec::new(), schema.clone(), Some(props)).unwrap();
        for batch in batches {
            writer.write(batch).unwrap();
        }
        writer.into_inner().unwrap()
    }

    fn metadata(bytes: &[u8]) -> ParquetMetaData {
        ParquetMetaDataReader::new()
            .with_page_index_policy(PageIndexPolicy::Optional)
            .parse_and_finish(&Bytes::copy_from_slice(bytes))
            .unwrap()
    }

    /// Every column chunk's bytes, row group by row group.
    fn chunks(bytes: &[u8]) -> Vec<Vec<u8>> {
        metadata(bytes)
            .row_groups()
            .iter()
            .flat_map(|rg| rg.columns().iter())
            .map(|c| {
                let (start, len) = c.byte_range();
                bytes[start as usize..(start + len) as usize].to_vec()
            })
            .collect()
    }

    fn read(bytes: &[u8]) -> Vec<RecordBatch> {
        ParquetRecordBatchReaderBuilder::try_new(Bytes::copy_from_slice(bytes))
            .unwrap()
            .build()
            .unwrap()
            .map(|b| b.unwrap())
            .collect()
    }

    fn concat(schema: &SchemaRef, batches: &[RecordBatch]) -> RecordBatch {
        arrow_select::concat::concat_batches(schema, batches).unwrap()
    }

    fn pools() -> [rayon::ThreadPool; 3] {
        [1, 3, 8].map(|n| {
            rayon::ThreadPoolBuilder::new()
                .num_threads(n)
                .build()
                .unwrap()
        })
    }

    #[test]
    fn columns_in_parallel_match_arrow_writer() {
        let zstd = || WriterProperties::builder().set_compression(zstd1());
        let layouts = [
            ("as opened", zstd().build()),
            (
                "small pages, row groups of 37",
                zstd()
                    .set_write_batch_size(3)
                    .set_data_page_row_count_limit(4)
                    .set_max_row_group_row_count(Some(37))
                    .build(),
            ),
            (
                "row groups of 1, page statistics, bloom filters",
                WriterProperties::builder()
                    .set_max_row_group_row_count(Some(1))
                    .set_statistics_enabled(EnabledStatistics::Page)
                    .set_bloom_filter_enabled(true)
                    .build(),
            ),
            (
                "no dictionary, no offset index",
                zstd()
                    .set_dictionary_enabled(false)
                    .set_statistics_enabled(EnabledStatistics::Chunk)
                    .set_offset_index_disabled(true)
                    .set_max_row_group_row_count(Some(50))
                    .build(),
            ),
        ];
        // Row groups are encoded up to one per thread at a time: waves of one row group,
        // uneven waves, and waves wider than the row groups there are
        let pools = pools();
        for (layout, props) in layouts {
            for rows in [0, 1, 2, 37, 38, 74, 300] {
                let (schema, batches) = struct_batches(rows, rows as u64 + 1);
                let one = arrow_writer(&schema, &batches, props.clone());
                for pool in &pools {
                    let file = NamedTempFile::new().unwrap();
                    let groups = row_groups(
                        batches.clone(),
                        props.max_row_group_row_count().unwrap_or(usize::MAX),
                    );
                    pool.install(|| write_columns(file.path(), &schema, groups, props.clone()))
                        .unwrap();
                    let parallel = std::fs::read(file.path()).unwrap();
                    let threads = pool.current_num_threads();
                    assert!(
                        one == parallel,
                        "{layout}, {rows} rows, {threads} threads: {} and {} bytes",
                        one.len(),
                        parallel.len()
                    );
                }
            }
        }
    }

    fn cdc_options() -> WriteOptions {
        WriteOptions {
            max_row_group_rows: Some(64),
            spec_names: true,
            sorting_columns: Some(vec![SortingColumn {
                column_idx: 0,
                descending: false,
                nulls_first: false,
            }]),
            content_defined_chunking: Some(CdcOptions {
                min_chunk_size: 256,
                max_chunk_size: 1024,
                norm_level: 0,
            }),
            ..WriteOptions::default()
        }
    }

    /// With content-defined chunking, each row group's column chunks are those an
    /// `ArrowWriter` writes for that row group alone, whatever the threads, and the file
    /// reads back as the batches, declaring the writer's sorting columns.
    #[test]
    fn chunked_row_groups_are_their_own_writers() {
        let options = cdc_options();
        for rows in [1, 63, 64, 65, 300] {
            let (schema, batches) = struct_batches(rows, rows as u64 + 7);
            let props = properties(&schema, &batches, &options)
                .unwrap()
                .into_builder()
                .set_max_row_group_row_count(Some(usize::MAX))
                .build();
            let want: Vec<Vec<u8>> = row_groups(batches.clone(), 64)
                .iter()
                .flat_map(|g| chunks(&arrow_writer(&schema, g, props.clone())))
                .collect();
            let mut outputs = Vec::new();
            for pool in &pools() {
                let file = NamedTempFile::new().unwrap();
                let n = pool
                    .install(|| {
                        write_batches(file.path(), schema.clone(), batches.clone(), &options)
                    })
                    .unwrap();
                assert_eq!(n, rows as u64);
                outputs.push(std::fs::read(file.path()).unwrap());
            }
            assert!(
                outputs.iter().all(|o| o == &outputs[0]),
                "{rows} rows: threads differ"
            );
            let out = &outputs[0];
            assert_eq!(chunks(out), want, "{rows} rows");
            let md = metadata(out);
            assert_eq!(md.num_row_groups(), rows.div_ceil(64));
            for rg in md.row_groups() {
                assert_eq!(rg.sorting_columns(), options.sorting_columns.as_ref());
            }
            assert_eq!(md.file_metadata().created_by(), Some(props.created_by()));
            let leaf = md.file_metadata().schema_descr().column(2).path().string();
            assert_eq!(leaf, "claims.xs.list.element");
            let got = concat(&read(out)[0].schema(), &read(out));
            let written = concat(&schema, &batches);
            assert_eq!(got.num_rows(), written.num_rows());
            for (g, w) in got.columns().iter().zip(written.columns()) {
                assert_eq!(g, &arrow_cast::cast(w, g.data_type()).unwrap());
            }
        }
    }

    /// No rows, with content-defined chunking: a file of the schema with no row groups.
    #[test]
    fn chunked_without_rows() {
        let (schema, batches) = struct_batches(0, 1);
        let file = NamedTempFile::new().unwrap();
        assert_eq!(
            write_batches(file.path(), schema, batches, &cdc_options()).unwrap(),
            0
        );
        assert_eq!(
            metadata(&std::fs::read(file.path()).unwrap()).num_row_groups(),
            0
        );
    }

    fn dictionary_pages(bytes: &[u8]) -> HashMap<String, bool> {
        metadata(bytes)
            .row_group(0)
            .columns()
            .iter()
            .map(|c| {
                (
                    c.column_path().string(),
                    c.dictionary_page_offset().is_some(),
                )
            })
            .collect()
    }

    /// A dictionary is kept for a leaf whose sampled values repeat and left out for one
    /// whose values are mostly distinct, at any depth: a top-level string, and an integer
    /// and a string in a list of structs. With `adaptive_dictionary: false`, all are kept.
    #[test]
    fn mostly_distinct_leaves_are_not_dictionary_encoded() {
        use arrow_buffer::OffsetBuffer;
        let rows = 2_000;
        let unique: ArrayRef = Arc::new(StringArray::from_iter_values(
            (0..rows).map(|i| format!("Q{i}")),
        ));
        let repeated: ArrayRef = Arc::new(StringArray::from_iter_values(
            (0..rows).map(|i| format!("P{}", i % 7)),
        ));
        let n: ArrayRef = Arc::new(Int64Array::from_iter_values(
            (0..2 * rows as i64).map(|i| i * 7919),
        ));
        let kind: ArrayRef = Arc::new(StringArray::from_iter_values(
            (0..2 * rows).map(|i| ["a", "b", "c"][i % 3]),
        ));
        let item_fields = Fields::from(vec![
            Field::new("n", DataType::Int64, true),
            Field::new("kind", DataType::Utf8, true),
        ]);
        let items = StructArray::new(item_fields.clone(), vec![n, kind], None);
        let list: ArrayRef = Arc::new(ListArray::new(
            Arc::new(Field::new("item", DataType::Struct(item_fields), true)),
            OffsetBuffer::from_lengths(std::iter::repeat_n(2, rows)),
            Arc::new(items),
            None,
        ));
        let schema = Arc::new(Schema::new(vec![
            Field::new("unique", DataType::Utf8, true),
            Field::new("repeated", DataType::Utf8, true),
            Field::new("items", list.data_type().clone(), true),
        ]));
        let batch = RecordBatch::try_new(schema.clone(), vec![unique, repeated, list]).unwrap();
        let options = WriteOptions {
            spec_names: true,
            ..WriteOptions::default()
        };
        let file = NamedTempFile::new().unwrap();
        write_batches(file.path(), schema.clone(), vec![batch.clone()], &options).unwrap();
        let pages = dictionary_pages(&std::fs::read(file.path()).unwrap());
        assert!(!pages["unique"], "{pages:?}");
        assert!(pages["repeated"], "{pages:?}");
        assert!(!pages["items.list.element.n"], "{pages:?}");
        assert!(pages["items.list.element.kind"], "{pages:?}");

        let options = WriteOptions {
            adaptive_dictionary: false,
            ..options
        };
        write_batches(file.path(), schema, vec![batch], &options).unwrap();
        let pages = dictionary_pages(&std::fs::read(file.path()).unwrap());
        assert!(pages.values().all(|&d| d), "{pages:?}");
    }

    /// A flat column's data pages end at the page size limit (1 MiB), not every 20,000
    /// rows as arrow-rs's default would have them: 300,000 distinct Int64 values (2.4 MB,
    /// written plain) take three pages, and read back unchanged.
    #[test]
    fn flat_pages_end_at_the_page_size_limit() {
        let rows = 300_000;
        let values = Int64Array::from_iter_values((0..rows as i64).map(|i| i * 7919));
        let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, true)]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(values.clone())]).unwrap();
        let file = NamedTempFile::new().unwrap();
        write_batches(file.path(), schema, vec![batch], &WriteOptions::default()).unwrap();

        let options = ArrowReaderOptions::new().with_offset_index_policy(PageIndexPolicy::Required);
        let metadata =
            ArrowReaderMetadata::load(&File::open(file.path()).unwrap(), options).unwrap();
        let pages = metadata
            .metadata()
            .page_index_for_row_group(0)
            .page_locations(0)
            .unwrap()
            .clone();
        let starts: Vec<usize> = pages
            .iter()
            .map(|p| p.first_row_index as usize)
            .chain([rows])
            .collect();
        let page_rows: Vec<usize> = starts.windows(2).map(|w| w[1] - w[0]).collect();
        assert_eq!(page_rows.len(), 3, "{page_rows:?}");
        assert!(page_rows[..2].iter().all(|&n| n > 20_000), "{page_rows:?}");
        let read: Vec<i64> = read(&std::fs::read(file.path()).unwrap())
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_primitive::<arrow_array::types::Int64Type>()
                    .values()
                    .to_vec()
            })
            .collect();
        assert_eq!(read, values.values().to_vec());
    }

    /// Statement-id-like strings: each row's id, `$`, then a random UUID, a few rows to an
    /// id, so each shares its prefix with the row before.
    fn statement_ids(rows: usize) -> (Vec<String>, Vec<String>) {
        let mut state = 7u64;
        let mut hex = move || {
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            format!("{:08X}", state >> 32)
        };
        let ids: Vec<String> = (0..rows).map(|i| format!("Q{}", 1000 + i / 3)).collect();
        let statements = ids
            .iter()
            .map(|id| {
                let (a, b, c, d) = (hex(), hex(), hex(), hex());
                format!(
                    "{id}${a}-{}-{}-{}-{}{c}",
                    &b[..4],
                    &b[4..],
                    &d[..4],
                    &d[4..]
                )
            })
            .collect();
        (ids, statements)
    }

    /// Each column chunk's encodings, its path's, of the file's row groups.
    fn encodings(bytes: &[u8], column: &str) -> Vec<Vec<Encoding>> {
        metadata(bytes)
            .row_groups()
            .iter()
            .flat_map(|rg| rg.columns().iter())
            .filter(|c| c.column_path().string() == column)
            .map(|c| c.encodings().collect())
            .collect()
    }

    /// A column asked for in DELTA_BYTE_ARRAY is written in it, with no dictionary, on
    /// both paths (columns in parallel, and row groups glued with content-defined
    /// chunking), whatever the threads: the same values, the same statistics and
    /// smaller; the other columns as they were. A nested leaf takes its path.
    #[test]
    fn column_encodings_change_only_the_bytes() {
        use arrow_buffer::OffsetBuffer;
        let rows = 20_000;
        let (ids, statements) = statement_ids(rows);
        let id: ArrayRef = Arc::new(StringArray::from_iter_values(&ids));
        let statement: ArrayRef = Arc::new(StringArray::from_iter_values(&statements));
        let hashes: ArrayRef = Arc::new(ListArray::new(
            Arc::new(Field::new("item", DataType::Utf8, true)),
            OffsetBuffer::from_lengths(std::iter::repeat_n(1, rows)),
            statement.clone(),
            None,
        ));
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("statement_id", DataType::Utf8, true),
            Field::new("hashes", hashes.data_type().clone(), true),
        ]));
        let batch = RecordBatch::try_new(schema.clone(), vec![id, statement, hashes]).unwrap();
        let batches: Vec<RecordBatch> = (0..rows)
            .step_by(1_000)
            .map(|i| batch.slice(i, 1_000))
            .collect();
        for base in [
            WriteOptions {
                max_row_group_rows: Some(6_000),
                spec_names: true,
                ..WriteOptions::default()
            },
            // (as wikidata-pq writes its published files: the default chunk sizes)
            WriteOptions {
                max_row_group_rows: Some(6_000),
                compression: Compression::ZSTD(ZstdLevel::try_new(3).unwrap()),
                content_defined_chunking: Some(CdcOptions::default()),
                ..cdc_options()
            },
        ] {
            let cdc = base.content_defined_chunking.is_some();
            let file = NamedTempFile::new().unwrap();
            write_batches(file.path(), schema.clone(), batches.clone(), &base).unwrap();
            let plain = std::fs::read(file.path()).unwrap();
            let options = WriteOptions {
                column_encodings: vec![
                    ("statement_id".into(), Encoding::DELTA_BYTE_ARRAY),
                    ("hashes.list.element".into(), Encoding::DELTA_BYTE_ARRAY),
                ],
                ..base.clone()
            };
            let mut outputs = Vec::new();
            for pool in &pools() {
                pool.install(|| {
                    write_batches(file.path(), schema.clone(), batches.clone(), &options)
                })
                .unwrap();
                outputs.push(std::fs::read(file.path()).unwrap());
            }
            assert!(
                outputs.iter().all(|o| o == &outputs[0]),
                "cdc {cdc}: threads differ"
            );
            let delta = &outputs[0];
            for column in ["statement_id", "hashes.list.element"] {
                for e in encodings(delta, column) {
                    assert!(
                        e.contains(&Encoding::DELTA_BYTE_ARRAY),
                        "cdc {cdc}, {column}: {e:?}"
                    );
                    assert!(!e.contains(&Encoding::PLAIN), "cdc {cdc}, {column}: {e:?}");
                    assert!(
                        !e.contains(&Encoding::RLE_DICTIONARY),
                        "cdc {cdc}, {column}: {e:?}"
                    );
                }
                for e in encodings(&plain, column) {
                    assert!(
                        !e.contains(&Encoding::DELTA_BYTE_ARRAY),
                        "cdc {cdc}, {column}: {e:?}"
                    );
                }
            }
            assert_eq!(encodings(delta, "id"), encodings(&plain, "id"));
            // The same values, schema and statistics; fewer bytes
            let (got, want) = (read(delta), read(&plain));
            assert_eq!(got[0].schema(), want[0].schema());
            assert_eq!(
                concat(&got[0].schema(), &got),
                concat(&want[0].schema(), &want)
            );
            let (md, plain_md) = (metadata(delta), metadata(&plain));
            assert_eq!(
                md.file_metadata().schema_descr().columns(),
                plain_md.file_metadata().schema_descr().columns()
            );
            assert_eq!(
                md.file_metadata().key_value_metadata(),
                plain_md.file_metadata().key_value_metadata()
            );
            assert_eq!(md.num_row_groups(), plain_md.num_row_groups());
            for (a, b) in md.row_groups().iter().zip(plain_md.row_groups()) {
                assert_eq!(a.num_rows(), b.num_rows());
                for (x, y) in a.columns().iter().zip(b.columns()) {
                    assert_eq!(x.statistics(), y.statistics(), "cdc {cdc}");
                }
            }
            let size = |m: &ParquetMetaData, column: &str| -> i64 {
                m.row_groups()
                    .iter()
                    .flat_map(|rg| rg.columns().iter())
                    .filter(|c| c.column_path().string() == column)
                    .map(|c| c.compressed_size())
                    .sum()
            };
            assert!(
                size(&md, "statement_id") < size(&plain_md, "statement_id"),
                "cdc {cdc}: {} against {}",
                size(&md, "statement_id"),
                size(&plain_md, "statement_id")
            );
            assert!(delta.len() < plain.len());
        }
    }

    /// A column the schema lacks, an encoding its type does not take and a dictionary
    /// encoding are refused, and nothing is written.
    #[test]
    fn column_encodings_are_checked() {
        let (schema, batches) = struct_batches(10, 3);
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("out.parquet");
        for (column, encoding, says) in [
            ("nope", Encoding::DELTA_BYTE_ARRAY, "no leaf column"),
            (
                "claims.inner.v",
                Encoding::DELTA_BYTE_ARRAY,
                "does not encode DOUBLE",
            ),
            (
                "claims.s",
                Encoding::DELTA_BINARY_PACKED,
                "does not encode BYTE_ARRAY",
            ),
            ("id", Encoding::RLE_DICTIONARY, "not an encoding to ask for"),
        ] {
            let options = WriteOptions {
                column_encodings: vec![(column.into(), encoding)],
                ..WriteOptions::default()
            };
            let e = write_batches(&out, schema.clone(), batches.clone(), &options).unwrap_err();
            assert!(e.to_string().contains(says), "{column}: {e}");
            assert!(!out.exists());
        }
        let options = WriteOptions {
            column_encodings: vec![
                ("claims.inner.v".into(), Encoding::BYTE_STREAM_SPLIT),
                ("claims.xs.list.item".into(), Encoding::DELTA_BINARY_PACKED),
                ("claims.s".into(), Encoding::DELTA_LENGTH_BYTE_ARRAY),
            ],
            ..WriteOptions::default()
        };
        write_batches(&out, schema.clone(), batches.clone(), &options).unwrap();
        let bytes = std::fs::read(&out).unwrap();
        assert!(
            encodings(&bytes, "claims.xs.list.item")[0].contains(&Encoding::DELTA_BINARY_PACKED)
        );
        let got = read(&bytes);
        let written = concat(&schema, &batches);
        assert_eq!(
            concat(&got[0].schema(), &got).num_rows(),
            written.num_rows()
        );
    }

    /// A failed write leaves no output.
    #[test]
    fn a_failed_write_is_removed() {
        let (schema, batches) = struct_batches(10, 3);
        let other = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, true)]));
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("out.parquet");
        for options in [WriteOptions::default(), cdc_options()] {
            assert!(write_batches(&out, other.clone(), batches.clone(), &options).is_err());
            assert!(!out.exists());
        }
        let _ = schema;
    }
}
