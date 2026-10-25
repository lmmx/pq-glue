//! What a concatenation copies: a file's row groups, all of them or some, and the
//! readers it copies them through.

use std::fs::File;
use std::io::{self, Read};
use std::path::PathBuf;
use std::sync::Arc;

use bytes::Bytes;
use parquet::file::reader::{ChunkReader, Length};

/// A source of a concatenation: a Parquet file's row groups, all of them in order
/// (`row_groups` None), or those listed, in the order listed.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Source {
    /// The file.
    pub path: PathBuf,
    /// The row groups to copy, by index, in the order to copy them; all if None.
    pub row_groups: Option<Vec<usize>>,
}

impl Source {
    /// All of the file's row groups, in order.
    pub fn whole(path: impl Into<PathBuf>) -> Self {
        Source {
            path: path.into(),
            row_groups: None,
        }
    }

    /// The file's row groups `row_groups`, in that order.
    pub fn row_groups(path: impl Into<PathBuf>, row_groups: Vec<usize>) -> Self {
        Source {
            path: path.into(),
            row_groups: Some(row_groups),
        }
    }
}

impl From<PathBuf> for Source {
    fn from(path: PathBuf) -> Self {
        Source::whole(path)
    }
}

/// An input's bytes: read whole into memory, the file itself (parquet-rs's reader for
/// `File`: a seek and a read on a duplicate of its handle for each read), or the file
/// read at offsets (`At`), which many threads can read at once.
pub(crate) enum Reader {
    Bytes(Bytes),
    File(File),
    At(At),
}

impl Length for Reader {
    fn len(&self) -> u64 {
        match self {
            Reader::Bytes(b) => b.len() as u64,
            Reader::File(f) => Length::len(f),
            Reader::At(a) => a.len,
        }
    }
}

impl ChunkReader for Reader {
    type T = Box<dyn Read + Send>;

    fn get_read(&self, start: u64) -> parquet::errors::Result<Self::T> {
        Ok(match self {
            Reader::Bytes(b) => Box::new(b.get_read(start)?),
            Reader::File(f) => Box::new(f.get_read(start)?),
            Reader::At(a) => Box::new(AtRead {
                file: a.file.clone(),
                pos: start,
            }),
        })
    }

    fn get_bytes(&self, start: u64, length: usize) -> parquet::errors::Result<Bytes> {
        match self {
            Reader::Bytes(b) => b.get_bytes(start, length),
            Reader::File(f) => f.get_bytes(start, length),
            Reader::At(a) => {
                let mut buf = vec![0; length];
                let mut done = 0;
                while done < length {
                    match read_at(&a.file, &mut buf[done..], start + done as u64)? {
                        0 => return Err(io::Error::from(io::ErrorKind::UnexpectedEof).into()),
                        n => done += n,
                    }
                }
                Ok(buf.into())
            }
        }
    }
}

/// A file read at offsets, shared: each read names its offset, so threads reading it
/// at once do not move each other's position.
pub(crate) struct At {
    file: Arc<File>,
    len: u64,
}

impl At {
    pub fn new(file: File) -> io::Result<Self> {
        let len = file.metadata()?.len();
        Ok(At {
            file: Arc::new(file),
            len,
        })
    }
}

/// A read of an `At` from an offset onwards.
struct AtRead {
    file: Arc<File>,
    pos: u64,
}

impl Read for AtRead {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let n = read_at(&self.file, buf, self.pos)?;
        self.pos += n as u64;
        Ok(n)
    }
}

#[cfg(unix)]
fn read_at(file: &File, buf: &mut [u8], offset: u64) -> io::Result<usize> {
    std::os::unix::fs::FileExt::read_at(file, buf, offset)
}

#[cfg(windows)]
fn read_at(file: &File, buf: &mut [u8], offset: u64) -> io::Result<usize> {
    std::os::windows::fs::FileExt::seek_read(file, buf, offset)
}
