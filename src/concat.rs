//! Concatenation by copying encoded row groups.

use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use bytes::Bytes;
use parquet::basic::{ColumnOrder, SortOrder};
use parquet::bloom_filter::Sbbf;
use parquet::column::writer::ColumnCloseResult;
use parquet::file::metadata::SortingColumn;
use parquet::file::properties::{WriterProperties, WriterVersion};
use parquet::file::reader::ChunkReader;
use parquet::file::writer::SerializedFileWriter;

use crate::error::{Error, Result};
use crate::footer::restore_column_orders;
use crate::input::{check_same, Input};
use crate::source::{At, Reader, Source};

/// Concatenate `inputs`, in order, into `output`, returning the rows written.
///
/// Every input must have the first one's Parquet schema, column orders and key-value
/// metadata, or nothing is written. `output` is created (or truncated) and is removed
/// if the copy fails part way; it must not be one of the inputs. When no input is larger
/// than 16 MiB, the inputs are read whole into memory, in one read each.
pub fn concat(inputs: &[PathBuf], output: &Path) -> Result<u64> {
    let sources: Vec<Source> = inputs.iter().cloned().map(Source::whole).collect();
    concat_reading(&sources, output, READ_WHOLE_BYTES)
}

/// [`concat`](concat()) of `sources`: each a file's row groups, all of them or those it
/// lists, in the order it lists them. The output is the selected row groups in order,
/// with the footer [`concat`](concat()) writes for the files they come from; a source
/// listing every row group in order is its whole file. A row group the file does not
/// have is an error ([`Error::RowGroup`]), and nothing is written.
pub fn concat_sources(sources: &[Source], output: &Path) -> Result<u64> {
    concat_reading(sources, output, READ_WHOLE_BYTES)
}

/// [`concat_sources`], the inputs read whole if none is larger than `read_whole` bytes.
fn concat_reading(sources: &[Source], output: &Path, read_whole: u64) -> Result<u64> {
    check_paths(sources, output)?;
    let files = sources
        .iter()
        .map(|s| {
            let p = &s.path;
            let file = File::open(p).map_err(Error::io(p))?;
            let len = file.metadata().map_err(Error::io(p))?.len();
            Ok((p, file, len))
        })
        .collect::<Result<Vec<_>>>()?;
    let whole = files.iter().all(|&(_, _, len)| len <= read_whole);
    let inputs = files
        .into_iter()
        .map(|(p, mut file, len)| {
            let reader = if whole {
                let mut bytes = vec![0; len as usize];
                file.read_exact(&mut bytes).map_err(Error::io(p))?;
                Reader::Bytes(Bytes::from(bytes))
            } else {
                Reader::File(file)
            };
            Input::new(p.clone(), reader)
        })
        .collect::<Result<Vec<_>>>()?;
    let selected: Vec<_> = inputs
        .iter()
        .zip(sources)
        .map(|(input, s)| (input, s.row_groups.as_deref()))
        .collect();
    glue(&selected, output)
}

/// There are sources, and `output` is not one of them.
fn check_paths(sources: &[Source], output: &Path) -> Result<()> {
    if sources.is_empty() {
        return Err(Error::NoInputs);
    }
    if sources.iter().any(|s| s.path.as_path() == output) {
        return Err(Error::OutputIsInput(output.to_path_buf()));
    }
    Ok(())
}

/// Inputs read whole, in one read each, when none is larger than this: read from the
/// file, each footer, page index, bloom filter and column chunk takes its own seek and
/// read on a duplicate of the file's handle (parquet-rs's `ChunkReader` for `File`),
/// and for a small file those system calls take longer than the bytes copied.
const READ_WHOLE_BYTES: u64 = 16 << 20;

/// An input's row groups to copy: all of them, in order, if None.
type Selected<'a, R> = (&'a Input<R>, Option<&'a [usize]>);

/// Glue the `inputs`' selected row groups, open, into `output` (see
/// [`concat_sources`]).
fn glue<R: ChunkReader>(inputs: &[Selected<'_, R>], output: &Path) -> Result<u64> {
    for &(input, rgs) in inputs {
        let n = input.metadata.num_row_groups();
        if let Some(&r) = rgs.into_iter().flatten().find(|&&r| r >= n) {
            return Err(Error::RowGroup {
                path: input.path.clone(),
                row_group: r,
                row_groups: n,
            });
        }
    }
    let first = inputs[0].0;
    for &(input, _) in &inputs[1..] {
        check_same(first, input)?;
    }
    let footer = Footer {
        created_by: created_by(inputs),
        version: writer_version(inputs),
        sorting_columns: sorting_columns(inputs),
    };
    let result = Glue::create(output, first, footer).and_then(|mut glue| {
        for &(input, rgs) in inputs {
            match rgs {
                None => glue.append(input)?,
                Some(rgs) => glue.append_row_groups(input, rgs)?,
            }
        }
        glue.finish()
    });
    if result.is_err() {
        let _ = std::fs::remove_file(output);
    }
    result
}

/// Concatenate each job's inputs, in order, into its output, as [`concat`](concat())
/// does, the jobs run in parallel on the calling rayon pool (by default one thread per
/// CPU), and return each job's rows written or error, in the jobs' order (feature
/// `parallel`).
///
/// For many small concatenations (a dataset's partitions, each a few files), whose time
/// goes on opening files and writing footers more than on bytes copied. Each job's output
/// is the same as `concat`'s, and the jobs are independent: one that fails leaves no
/// output of its own, and the others are written all the same. No two jobs may have the
/// same output, nor one job's output be another's input.
#[cfg(feature = "parallel")]
pub fn concat_many(jobs: &[(Vec<PathBuf>, PathBuf)]) -> Vec<Result<u64>> {
    let jobs: Vec<(Vec<Source>, PathBuf)> = jobs
        .iter()
        .map(|(inputs, output)| {
            (
                inputs.iter().cloned().map(Source::whole).collect(),
                output.clone(),
            )
        })
        .collect();
    concat_many_sources(&jobs)
}

/// [`concat_many`] of [`concat_sources`] jobs: each job's output is `concat_sources`'s.
///
/// A file that more than one job copies from (a file of many partitions' row groups,
/// each job one partition's) is opened once, and its footer read once, for every job:
/// it is read at offsets, which the jobs' threads can do at once, and not read whole. A
/// file only one job copies from is opened by that job, as `concat_sources` opens it.
#[cfg(feature = "parallel")]
pub fn concat_many_sources(jobs: &[(Vec<Source>, PathBuf)]) -> Vec<Result<u64>> {
    use rayon::prelude::*;
    use std::collections::HashMap;
    use std::sync::Arc;

    let mut named: HashMap<&Path, usize> = HashMap::new();
    for (sources, _) in jobs {
        let mut seen: Vec<&Path> = Vec::new();
        for s in sources {
            if !seen.contains(&s.path.as_path()) {
                seen.push(&s.path);
                *named.entry(&s.path).or_default() += 1;
            }
        }
    }
    let shared: Vec<&Path> = named
        .into_iter()
        .filter(|&(_, n)| n > 1)
        .map(|(p, _)| p)
        .collect();
    // A shared file that cannot be opened is opened again by each job naming it, which
    // fails with its own error
    let opened: HashMap<&Path, Arc<Input<Reader>>> = shared
        .par_iter()
        .filter_map(|&p| {
            let file = File::open(p).ok()?;
            let input = Input::new(p.to_path_buf(), Reader::At(At::new(file).ok()?)).ok()?;
            Some((p, Arc::new(input)))
        })
        .collect();
    jobs.par_iter()
        .map(|(sources, output)| {
            if !sources
                .iter()
                .any(|s| opened.contains_key(s.path.as_path()))
            {
                return concat_sources(sources, output);
            }
            check_paths(sources, output)?;
            let own = sources
                .iter()
                .filter(|s| !opened.contains_key(s.path.as_path()))
                .map(|s| {
                    let file = File::open(&s.path).map_err(Error::io(&s.path))?;
                    Input::new(s.path.clone(), Reader::File(file))
                })
                .collect::<Result<Vec<_>>>()?;
            let mut own = own.iter();
            let selected: Vec<Selected<'_, Reader>> = sources
                .iter()
                .map(|s| {
                    let input = match opened.get(s.path.as_path()) {
                        Some(input) => input.as_ref(),
                        None => own
                            .next()
                            .expect("an input opened for each unshared source"),
                    };
                    (input, s.row_groups.as_deref())
                })
                .collect();
            glue(&selected, output)
        })
        .collect()
}

/// What a glued file's footer declares besides the first input's schema, key-value
/// metadata and column orders: where the inputs agree on them, their `created_by`,
/// format version and row groups' sorting columns; parquet-rs's own `created_by` and
/// version, and no sorting columns, where they don't.
#[derive(Clone)]
pub(crate) struct Footer {
    pub created_by: Option<String>,
    pub version: Option<WriterVersion>,
    pub sorting_columns: Option<Vec<SortingColumn>>,
}

/// A file being glued: inputs' row groups appended in turn, each column chunk as it is
/// encoded, with its bloom filter and page index entries where it has them.
pub(crate) struct Glue {
    writer: SerializedFileWriter<File>,
    output: PathBuf,
    orders: Vec<ColumnOrder>,
    rows: u64,
}

impl Glue {
    /// `output`, created (or truncated), to take inputs with `first`'s schema, key-value
    /// metadata and column orders.
    pub fn create<R>(output: &Path, first: &Input<R>, footer: Footer) -> Result<Self> {
        let fm = first.metadata.file_metadata();
        let mut props = WriterProperties::builder()
            .set_key_value_metadata(fm.key_value_metadata().cloned())
            .set_sorting_columns(footer.sorting_columns);
        if let Some(created_by) = footer.created_by {
            props = props.set_created_by(created_by);
        }
        if let Some(version) = footer.version {
            props = props.set_writer_version(version);
        }
        let file = File::create(output).map_err(Error::io(output))?;
        let writer = SerializedFileWriter::new(
            file,
            fm.schema_descr().root_schema_ptr(),
            Arc::new(props.build()),
        )
        .map_err(Error::parquet(output))?;
        Ok(Glue {
            writer,
            output: output.to_path_buf(),
            orders: column_orders(first),
            rows: 0,
        })
    }

    /// Append `input`'s row groups, which must have the first input's schema, column
    /// orders and key-value metadata (see [`check_same`](crate::input::check_same)).
    pub fn append<R: ChunkReader>(&mut self, input: &Input<R>) -> Result<()> {
        let all: Vec<usize> = (0..input.metadata.num_row_groups()).collect();
        self.append_row_groups(input, &all)
    }

    /// Append `input`'s row groups `rgs`, in that order (each one the file has), as
    /// [`append`](Self::append) appends them all.
    pub fn append_row_groups<R: ChunkReader>(
        &mut self,
        input: &Input<R>,
        rgs: &[usize],
    ) -> Result<()> {
        let output = &self.output;
        let page_index = input.metadata.page_index();
        for &r in rgs {
            let rg = input.metadata.row_group(r);
            let mut rg_out = self
                .writer
                .next_row_group()
                .map_err(Error::parquet(output))?;
            for (c, column) in rg.columns().iter().enumerate() {
                let chunk = ColumnCloseResult {
                    bytes_written: column.compressed_size() as _,
                    rows_written: rg.num_rows() as _,
                    metadata: column.clone(),
                    bloom_filter: Sbbf::read_from_column_chunk(column, &input.file)
                        .ok()
                        .flatten(),
                    column_index: page_index.and_then(|pi| pi.column_index(r, c)).cloned(),
                    offset_index: page_index.and_then(|pi| pi.offset_index(r, c)).cloned(),
                };
                rg_out
                    .append_column(&input.file, chunk)
                    .map_err(Error::parquet(&input.path))?;
            }
            rg_out.close().map_err(Error::parquet(output))?;
            self.rows += rg.num_rows() as u64;
        }
        Ok(())
    }

    /// Write the footer, with the inputs' column orders, and return the rows written.
    pub fn finish(self) -> Result<u64> {
        self.writer.close().map_err(Error::parquet(&self.output))?;
        restore_column_orders(&self.output, &self.orders)?;
        Ok(self.rows)
    }
}

/// The inputs' column orders. Files written without them (before parquet-format 2.4)
/// have statistics in each type's legacy (signed) order, which is the type-defined
/// order of floats.
fn column_orders<R>(first: &Input<R>) -> Vec<ColumnOrder> {
    let fm = first.metadata.file_metadata();
    fm.column_orders().cloned().unwrap_or_else(|| {
        let n = fm.schema_descr().num_columns();
        vec![ColumnOrder::TYPE_DEFINED_ORDER(SortOrder::SIGNED); n]
    })
}

/// The value `get` gives every input, if they all give the same one.
fn shared<R, T: PartialEq>(inputs: &[Selected<'_, R>], get: impl Fn(&Input<R>) -> T) -> Option<T> {
    let first = get(inputs[0].0);
    inputs[1..]
        .iter()
        .all(|&(i, _)| get(i) == first)
        .then_some(first)
}

/// The inputs' `created_by`, if they all have the same one: it names the writer that
/// encoded every column chunk, which readers use to work around known writers' bugs.
fn created_by<R>(inputs: &[Selected<'_, R>]) -> Option<String> {
    shared(inputs, |i| {
        i.metadata.file_metadata().created_by().map(str::to_owned)
    })
    .flatten()
}

/// The inputs' format version, if they all have the same one.
fn writer_version<R>(inputs: &[Selected<'_, R>]) -> Option<WriterVersion> {
    match shared(inputs, |i| i.metadata.file_metadata().version())? {
        1 => Some(WriterVersion::PARQUET_1_0),
        2 => Some(WriterVersion::PARQUET_2_0),
        _ => None,
    }
}

/// The sorting columns every row group copied declares, if they all declare the same. A
/// row group's sorting columns say how its own rows are sorted, and the writer sets one
/// value for every row group, so row groups that differ declare none.
fn sorting_columns<R>(inputs: &[Selected<'_, R>]) -> Option<Vec<SortingColumn>> {
    let mut row_groups = inputs.iter().flat_map(|&(i, rgs)| {
        let all = i.metadata.row_groups();
        let picked: Vec<_> = match rgs {
            None => all.iter().collect(),
            Some(rgs) => rgs.iter().map(|&r| &all[r]).collect(),
        };
        picked
    });
    let first = row_groups.next()?.sorting_columns().cloned();
    row_groups
        .all(|rg| rg.sorting_columns() == first.as_ref())
        .then_some(first)
        .flatten()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{Int64Array, RecordBatch, StringArray};
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::{EnabledStatistics, WriterProperties};

    use super::*;

    fn write(path: &Path, i: i64) {
        let batch = RecordBatch::try_from_iter([
            (
                "id",
                Arc::new(StringArray::from_iter_values(
                    (0..50).map(|r| format!("Q{r}")),
                )) as arrow_array::ArrayRef,
            ),
            ("n", Arc::new(Int64Array::from_iter_values(i..i + 50))),
        ])
        .unwrap();
        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(20))
            .set_statistics_enabled(EnabledStatistics::Page)
            .set_bloom_filter_enabled(true)
            .build();
        let mut w =
            ArrowWriter::try_new(File::create(path).unwrap(), batch.schema(), Some(props)).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
    }

    #[test]
    fn inputs_read_whole_or_from_the_file_glue_the_same() {
        let dir = tempfile::tempdir().unwrap();
        let inputs: Vec<_> = (0..3)
            .map(|i| {
                let p = dir.path().join(format!("in_{i}.parquet"));
                write(&p, i * 100);
                p
            })
            .collect();
        let inputs: Vec<Source> = inputs.into_iter().map(Source::whole).collect();
        let whole = dir.path().join("whole.parquet");
        let from_file = dir.path().join("from_file.parquet");
        assert_eq!(concat_reading(&inputs, &whole, u64::MAX).unwrap(), 150);
        assert_eq!(concat_reading(&inputs, &from_file, 0).unwrap(), 150);
        assert_eq!(
            std::fs::read(&whole).unwrap(),
            std::fs::read(&from_file).unwrap()
        );
    }
}
