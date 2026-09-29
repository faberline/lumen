//! One borrowed fast-WAL vector staged as aligned native-endian f32 mmap data.
//!
//! Borrowed little-endian wire bytes are decoded to a private same-process
//! native mmap file. The backend and checkpoint journal can lend an aligned
//! slice without retaining a full decoded input or checkpoint buffer.

use std::fs::{self, File};
use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

#[cfg(test)]
use std::cell::RefCell;

use anyhow::{anyhow, bail, Context, Result};
use memmap2::{Mmap, MmapOptions};

const DECODE_CHUNK_BYTES: usize = 64 * 1024;
const READER_METADATA_BYTES: usize = 4096;
static NONCE: AtomicU64 = AtomicU64::new(0);

#[cfg(test)]
thread_local! { static LAST_STAGE_DIRECTORY: RefCell<Option<PathBuf>> = const { RefCell::new(None) }; }

#[cfg(test)]
pub(super) fn last_stage_directory_for_test() -> Option<PathBuf> {
    LAST_STAGE_DIRECTORY.with(|last| last.borrow().clone())
}

/// A private file-backed, native-aligned vector row.
#[derive(Clone, Debug)]
pub(crate) struct StagedVectorRow {
    mmap: Arc<Mmap>,
    dim: usize,
    _directory: Arc<OwnedDirectory>,
}

impl StagedVectorRow {
    /// Decode a wire `f32` span in bounded chunks. `payload` is untrusted LE
    /// bytes and may begin at any address. The callback must accept both the
    /// bounded decode buffer and reader metadata before any allocation or file
    /// creation happens.
    pub(crate) fn stage(
        payload: &[u8],
        dim: u32,
        mut reserve: impl FnMut(usize) -> Result<()>,
    ) -> Result<Self> {
        let dim = usize::try_from(dim).map_err(|_| anyhow!("vector dim exceeds usize"))?;
        if dim == 0 {
            bail!("vector dim must be > 0");
        }
        let expected = dim
            .checked_mul(std::mem::size_of::<f32>())
            .ok_or_else(|| anyhow!("vector byte length overflow"))?;
        if payload.len() != expected {
            bail!(
                "vector wire length {} does not match dim {dim}",
                payload.len()
            );
        }
        let scratch = DECODE_CHUNK_BYTES.min(expected);
        let required = scratch
            .checked_add(READER_METADATA_BYTES)
            .ok_or_else(|| anyhow!("vector staging reservation overflow"))?;
        reserve(required)?;
        let directory = Arc::new(OwnedDirectory::create()?);
        let path = directory.path.join("row.f32");
        let mut output =
            File::create_new(&path).with_context(|| format!("create {}", path.display()))?;
        // This is the only decoded heap buffer. File::write_all does not add a
        // caller-owned buffering allocation, so `scratch` prices every
        // simultaneous decoded workspace byte.
        let mut native = Vec::with_capacity(scratch);
        for chunk in payload.chunks(DECODE_CHUNK_BYTES) {
            native.clear();
            for word in chunk.chunks_exact(4) {
                native.extend_from_slice(
                    &f32::from_le_bytes(word.try_into().expect("four bytes")).to_ne_bytes(),
                );
            }
            output.write_all(&native)?;
        }
        output.flush()?;
        output.sync_all()?;
        // SAFETY: the private file is complete and never written after this
        // point; OwnedDirectory keeps its pathname alive through every clone.
        let mmap = unsafe { MmapOptions::new().map(&output) }.context("mmap staged vector")?;
        if mmap.len() != expected {
            bail!("staged vector length changed before mmap");
        }
        bytemuck::try_cast_slice::<u8, f32>(&mmap[..])
            .map_err(|error| anyhow!("staged vector mmap alignment: {error:?}"))?;
        Ok(Self {
            mmap: Arc::new(mmap),
            dim,
            _directory: directory,
        })
    }

    /// Create the exact current scalar-quantized decoded checkpoint view.
    /// The codebook is widened once over the complete row, then each value is
    /// encoded and decoded with the same arithmetic as vector_index::encode_sq
    /// and decode_sq. Only one bounded native-byte chunk is allocated.
    pub(crate) fn stage_sq_canonical(
        &self,
        mut codebook: crate::index::domain::vector::quantize::ScalarCodebook,
        mut reserve: impl FnMut(usize) -> Result<()>,
    ) -> Result<(Self, crate::index::domain::vector::quantize::ScalarCodebook)> {
        codebook.widen(self.as_f32_slice());
        let scratch = DECODE_CHUNK_BYTES.min(
            self.dim
                .checked_mul(4)
                .ok_or_else(|| anyhow!("vector byte length overflow"))?,
        );
        reserve(
            scratch
                .checked_add(READER_METADATA_BYTES)
                .ok_or_else(|| anyhow!("vector staging reservation overflow"))?,
        )?;
        let directory = Arc::new(OwnedDirectory::create()?);
        let path = directory.path.join("canonical.f32");
        let mut output = File::create_new(&path)?;
        let mut native = Vec::with_capacity(scratch);
        let span = (codebook.max - codebook.min).max(f32::MIN_POSITIVE);
        for values in self.as_f32_slice().chunks((DECODE_CHUNK_BYTES / 4).max(1)) {
            native.clear();
            for &value in values {
                let encoded =
                    (((value - codebook.min) / span).clamp(0.0, 1.0) * 255.0).round() as u8;
                let canonical = codebook.min + (encoded as f32 / 255.0) * span;
                native.extend_from_slice(&canonical.to_ne_bytes());
            }
            output.write_all(&native)?;
        }
        output.sync_all()?;
        let mmap =
            unsafe { MmapOptions::new().map(&output) }.context("mmap canonical staged vector")?;
        bytemuck::try_cast_slice::<u8, f32>(&mmap[..])
            .map_err(|error| anyhow!("canonical staged vector mmap alignment: {error:?}"))?;
        Ok((
            Self {
                mmap: Arc::new(mmap),
                dim: self.dim,
                _directory: directory,
            },
            codebook,
        ))
    }

    pub(crate) fn dim(&self) -> usize {
        self.dim
    }
    pub(crate) const fn retained_metadata_bound() -> usize {
        READER_METADATA_BYTES
    }
    pub(crate) fn as_f32_slice(&self) -> &[f32] {
        // Checked in stage after mmap creation. The mmap is immutable and Arc
        // keeps it alive for the returned borrow.
        bytemuck::try_cast_slice(&self.mmap[..]).expect("validated staged vector mmap")
    }
}

#[derive(Debug)]
struct OwnedDirectory {
    path: PathBuf,
}
impl OwnedDirectory {
    fn create() -> Result<Self> {
        let root = std::env::temp_dir();
        let process = std::process::id();
        for _ in 0..128 {
            let path = root.join(format!(
                "lumen-staged-vector-row-{process}-{}",
                NONCE.fetch_add(1, Ordering::Relaxed)
            ));
            let mut builder = fs::DirBuilder::new();
            #[cfg(unix)]
            {
                use std::os::unix::fs::DirBuilderExt;
                builder.mode(0o700);
            }
            match builder.create(&path) {
                Ok(()) => {
                    #[cfg(test)]
                    LAST_STAGE_DIRECTORY.with(|last| *last.borrow_mut() = Some(path.clone()));
                    return Ok(Self { path });
                }
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(error) => {
                    return Err(error).with_context(|| format!("create {}", path.display()))
                }
            }
        }
        bail!("could not allocate staged vector directory")
    }
}
impl Drop for OwnedDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

#[cfg(test)]
mod tests;
