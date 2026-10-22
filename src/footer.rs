//! The output footer's column orders.
//!
//! parquet-rs writes every float column's order as IEEE 754 total order, whatever the
//! order its statistics were computed in. Statistics copied from files that declare
//! another order (pyarrow declares the type-defined order) are then read under the
//! wrong one: Arrow C++ ignores IEEE 754 float min/max without the NaN counts that
//! order requires. [`restore_column_orders`] puts the inputs' orders back.

use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

use parquet::basic::ColumnOrder;

use crate::error::{Error, Result};
use crate::input::read_metadata;

/// A `ColumnOrder` union in Thrift's compact encoding: a field header (field 1,
/// TYPE_ORDER, or field 2, IEEE_754_TOTAL_ORDER, each an empty struct), the empty
/// struct's stop byte, and the union's.
const TYPE_DEFINED: [u8; 3] = [0x1C, 0x00, 0x00];
const IEEE_754: [u8; 3] = [0x2C, 0x00, 0x00];

/// Put the inputs' column orders back in the footer parquet-rs wrote to `path`.
///
/// parquet-rs writes every float column's order as IEEE 754 total order, whatever the
/// order its statistics were computed in; the copied statistics were computed in the
/// inputs' (for pyarrow's, type-defined) order, and Arrow C++ ignores IEEE 754 float
/// min/max without the NaN counts that order requires. The writer's API has no way to
/// set the orders, and the two encodings are the same length, so the footer is patched
/// in place: its `column_orders` list is its last field, each element checked to be
/// one of the two before it is rewritten, and the footer is read back to check it has
/// exactly `orders`. Nothing else in the file changes.
pub fn restore_column_orders(path: &Path, orders: &[ColumnOrder]) -> Result<()> {
    let mut want = Vec::with_capacity(orders.len());
    for order in orders {
        want.push(match order {
            ColumnOrder::TYPE_DEFINED_ORDER(_) => TYPE_DEFINED,
            ColumnOrder::IEEE_754_TOTAL_ORDER => IEEE_754,
            other => {
                return Err(Error::footer(
                    path,
                    format!("cannot write column order {other:?}"),
                ))
            }
        });
    }
    let io = Error::io;
    let mut f = OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .map_err(io(path))?;
    let len = f.seek(SeekFrom::End(0)).map_err(io(path))?;
    let mut tail = [0u8; 8];
    f.seek(SeekFrom::End(-8)).map_err(io(path))?;
    f.read_exact(&mut tail).map_err(io(path))?;
    if &tail[4..] != b"PAR1" {
        return Err(Error::footer(path, "no Parquet footer"));
    }
    let footer_len = u32::from_le_bytes(tail[..4].try_into().unwrap()) as u64;
    let footer_start = len - 8 - footer_len;
    let mut footer = vec![0u8; footer_len as usize];
    f.seek(SeekFrom::Start(footer_start)).map_err(io(path))?;
    f.read_exact(&mut footer).map_err(io(path))?;
    // [list header][n x 3 bytes][FileMetaData's stop byte]
    let n = want.len();
    let unexpected = || Error::footer(path, "unexpected column_orders encoding in the footer");
    let elems_end = footer.len().checked_sub(1).ok_or_else(unexpected)?;
    let elems_start = elems_end.checked_sub(3 * n).ok_or_else(unexpected)?;
    if footer[elems_end] != 0x00 {
        return Err(unexpected());
    }
    let header = list_header(n);
    if elems_start < header.len() || footer[elems_start - header.len()..elems_start] != header[..] {
        return Err(unexpected());
    }
    for (i, w) in want.iter().enumerate() {
        let e = &mut footer[elems_start + 3 * i..elems_start + 3 * i + 3];
        if *e != TYPE_DEFINED && *e != IEEE_754 {
            return Err(unexpected());
        }
        e.copy_from_slice(w);
    }
    f.seek(SeekFrom::Start(footer_start + elems_start as u64))
        .map_err(io(path))?;
    f.write_all(&footer[elems_start..elems_end])
        .map_err(io(path))?;
    drop(f);
    let file = std::fs::File::open(path).map_err(io(path))?;
    let got = read_metadata(path, &file)?;
    if got.file_metadata().column_orders().map(Vec::as_slice) != Some(orders) {
        return Err(Error::footer(
            path,
            "the footer read back has other column orders",
        ));
    }
    Ok(())
}

/// A compact-protocol list header for `n` structs: the size in the high nibble below
/// 15, else 0xF and the size as a varint.
fn list_header(n: usize) -> Vec<u8> {
    if n < 15 {
        return vec![((n as u8) << 4) | 0x0C];
    }
    let mut out = vec![0xFC];
    let mut v = n as u64;
    loop {
        let b = (v & 0x7F) as u8;
        v >>= 7;
        if v == 0 {
            out.push(b);
            return out;
        }
        out.push(b | 0x80);
    }
}

#[cfg(test)]
mod tests {
    use super::list_header;

    #[test]
    fn list_headers() {
        assert_eq!(list_header(0), vec![0x0C]);
        assert_eq!(list_header(14), vec![0xEC]);
        assert_eq!(list_header(15), vec![0xFC, 15]);
        assert_eq!(list_header(93), vec![0xFC, 93]);
        assert_eq!(list_header(300), vec![0xFC, 0xAC, 0x02]);
    }
}
