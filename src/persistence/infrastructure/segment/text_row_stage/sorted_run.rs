//! Sorted, deduplicated term runs on the staging filesystem: flushing one,
//! reading one back with bounds on every record, and merging two at fan-in two.

use std::cmp::Ordering;
use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use anyhow::{anyhow, Context, Result};

use crate::persistence::infrastructure::segment::text_row_stage::{fresh_path, RowWorkspace};

pub(super) fn flush_run(
    workspace: &mut RowWorkspace,
    terms: &mut Vec<String>,
    used: &mut usize,
) -> Result<()> {
    if terms.is_empty() {
        return Ok(());
    }
    terms.sort_unstable();
    let path = fresh_path(workspace.dir(), "run");
    let mut out = BufWriter::new(
        OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .with_context(|| format!("create text term run {}", path.display()))?,
    );
    let mut index = 0usize;
    while index < terms.len() {
        let first = index;
        index += 1;
        while index < terms.len() && terms[index] == terms[first] {
            index += 1;
        }
        let count: u32 = (index - first)
            .try_into()
            .context("text term frequency exceeds u32")?;
        write_run_record(&mut out, terms[first].as_bytes(), count)?;
    }
    out.flush()
        .with_context(|| format!("flush text term run {}", path.display()))?;
    out.get_ref()
        .sync_all()
        .with_context(|| format!("fsync text term run {}", path.display()))?;
    terms.clear();
    *used = 0;
    workspace.push_run(path)?;
    Ok(())
}

pub(super) fn write_run_record(out: &mut impl Write, term: &[u8], count: u32) -> Result<()> {
    let length: u32 = term
        .len()
        .try_into()
        .context("text term exceeds u32 record capacity")?;
    let length = length.to_le_bytes();
    let count = count.to_le_bytes();
    let mut crc = crc32fast::Hasher::new();
    crc.update(&length);
    crc.update(term);
    crc.update(&count);
    out.write_all(&length)?;
    out.write_all(term)?;
    out.write_all(&count)?;
    out.write_all(&crc.finalize().to_le_bytes())?;
    Ok(())
}

pub(super) struct RunReader {
    input: BufReader<File>,
    pub(super) current: Option<(Vec<u8>, u32)>,
    max_term: usize,
}
impl RunReader {
    pub(super) fn open(path: &Path, max_term: usize) -> Result<Self> {
        let mut reader = Self {
            input: BufReader::new(File::open(path)?),
            current: None,
            max_term,
        };
        reader.advance()?;
        Ok(reader)
    }
    pub(super) fn advance(&mut self) -> Result<()> {
        // Release the old buffer before allocating the next. A clean EOF is
        // valid only before the first length byte; a short prefix is corruption.
        self.current = None;
        let mut length = [0u8; 4];
        match self.input.read(&mut length[..1]) {
            Ok(0) => {
                self.current = None;
                return Ok(());
            }
            Ok(1) => {}
            Ok(_) => unreachable!("one-byte run prefix read"),
            Err(error) => return Err(error.into()),
        }
        self.input
            .read_exact(&mut length[1..])
            .context("truncated text term run length prefix")?;
        let length = usize::try_from(u32::from_le_bytes(length))
            .context("run term length exceeds platform")?;
        anyhow::ensure!(
            length <= self.max_term,
            "text run length exceeds reserved term buffer"
        );
        let mut term = vec![0; length];
        self.input.read_exact(&mut term)?;
        let mut count = [0u8; 4];
        self.input
            .read_exact(&mut count)
            .context("truncated text term run count")?;
        let mut checksum = [0u8; 4];
        self.input
            .read_exact(&mut checksum)
            .context("truncated text term run checksum")?;
        let mut crc = crc32fast::Hasher::new();
        crc.update(&(length as u32).to_le_bytes());
        crc.update(&term);
        crc.update(&count);
        anyhow::ensure!(
            crc.finalize() == u32::from_le_bytes(checksum),
            "text term run checksum mismatch"
        );
        let count = u32::from_le_bytes(count);
        anyhow::ensure!(
            !term.is_empty() && count != 0 && std::str::from_utf8(&term).is_ok(),
            "invalid normalized text term run record"
        );
        self.current = Some((term, count));
        Ok(())
    }
}

pub(super) fn merge_runs(
    dir: &Path,
    left: &Path,
    right: &Path,
    max_term: usize,
) -> Result<PathBuf> {
    let path = fresh_path(dir, "merge");
    let result = (|| {
        let mut a = RunReader::open(left, max_term)?;
        let mut b = RunReader::open(right, max_term)?;
        let mut out = BufWriter::new(
            OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&path)?,
        );
        loop {
            let pick = match (&a.current, &b.current) {
                (None, None) => break,
                (Some(_), None) => Ordering::Less,
                (None, Some(_)) => Ordering::Greater,
                (Some((a_term, _)), Some((b_term, _))) => a_term.cmp(b_term),
            };
            match pick {
                Ordering::Less => {
                    let (term, count) = a.current.as_ref().unwrap();
                    write_run_record(&mut out, term, *count)?;
                    a.advance()?;
                }
                Ordering::Greater => {
                    let (term, count) = b.current.as_ref().unwrap();
                    write_run_record(&mut out, term, *count)?;
                    b.advance()?;
                }
                Ordering::Equal => {
                    let (term, a_count) = a.current.as_ref().unwrap();
                    let b_count = b.current.as_ref().unwrap().1;
                    let count = a_count
                        .checked_add(b_count)
                        .ok_or_else(|| anyhow!("text term frequency exceeds u32"))?;
                    write_run_record(&mut out, term, count)?;
                    a.advance()?;
                    b.advance()?;
                }
            }
        }
        out.flush()?;
        out.get_ref().sync_all()?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&path);
        return result.map(|_| path);
    }
    fs::remove_file(left).with_context(|| format!("remove merged run {}", left.display()))?;
    fs::remove_file(right).with_context(|| format!("remove merged run {}", right.display()))?;
    Ok(path)
}
