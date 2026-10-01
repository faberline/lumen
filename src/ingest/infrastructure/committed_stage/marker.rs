//! The marker file: the magic, format version, source, sequence, epoch, length
//! and digest a committed pair is recovered by, written once and read back with
//! every field checked.

use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::Path;

use crate::ingest::infrastructure::committed_stage::files::open_regular_readonly;
use crate::ingest::infrastructure::committed_stage::{
    invalid, Marker, SourceIdentity, SourceKind, FORMAT_MAGIC, FORMAT_VERSION, MAX_SOURCE_ID_BYTES,
};

pub(super) fn write_marker(path: &Path, marker: &Marker) -> io::Result<()> {
    let origin = marker.source.origin.as_bytes();
    let origin_len =
        u32::try_from(origin.len()).map_err(|_| invalid("source identity too long"))?;
    let mut file = OpenOptions::new().write(true).create_new(true).open(path)?;
    file.write_all(FORMAT_MAGIC)?;
    file.write_all(&[FORMAT_VERSION, marker.source.kind as u8])?;
    file.write_all(&marker.sequence.to_le_bytes())?;
    file.write_all(&marker.engine_epoch.to_le_bytes())?;
    file.write_all(&marker.byte_len.to_le_bytes())?;
    file.write_all(&origin_len.to_le_bytes())?;
    file.write_all(origin)?;
    file.write_all(&marker.digest)?;
    Ok(())
}

pub(super) fn read_marker(path: &Path) -> io::Result<Marker> {
    let mut file = open_regular_readonly(path)?;
    let mut magic = [0; 8];
    file.read_exact(&mut magic)?;
    if &magic != FORMAT_MAGIC {
        return Err(invalid("committed-stage marker has unknown format"));
    }
    let version = read_u8(&mut file)?;
    if version != FORMAT_VERSION {
        return Err(invalid("committed-stage marker has unknown version"));
    }
    let kind = SourceKind::decode(read_u8(&mut file)?)?;
    let sequence = read_u64(&mut file)?;
    let engine_epoch = read_u64(&mut file)?;
    let byte_len = read_u64(&mut file)?;
    let origin_len =
        usize::try_from(read_u32(&mut file)?).map_err(|_| invalid("invalid source length"))?;
    if origin_len == 0 || origin_len > MAX_SOURCE_ID_BYTES {
        return Err(invalid("committed-stage marker has invalid source length"));
    }
    let mut origin = vec![0; origin_len];
    file.read_exact(&mut origin)?;
    let origin = String::from_utf8(origin)
        .map_err(|_| invalid("committed-stage marker source is not UTF-8"))?;
    let mut digest = [0; 32];
    file.read_exact(&mut digest)?;
    if file.read(&mut [0; 1])? != 0 {
        return Err(invalid("committed-stage marker has trailing bytes"));
    }
    Ok(Marker {
        sequence,
        engine_epoch,
        source: SourceIdentity::new(kind, origin)?,
        byte_len,
        digest,
    })
}

fn read_u8(file: &mut File) -> io::Result<u8> {
    let mut value = [0; 1];
    file.read_exact(&mut value)?;
    Ok(value[0])
}

fn read_u32(file: &mut File) -> io::Result<u32> {
    let mut value = [0; 4];
    file.read_exact(&mut value)?;
    Ok(u32::from_le_bytes(value))
}

fn read_u64(file: &mut File) -> io::Result<u64> {
    let mut value = [0; 8];
    file.read_exact(&mut value)?;
    Ok(u64::from_le_bytes(value))
}
