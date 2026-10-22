//! Private scalar files prepared from borrowed, already durable WAL bytes.
//!
//! This has no live attachment operation. The caller selects winners against a
//! captured collection version, reserves before allocation, and later rechecks
//! that version before it publishes all of the files in one apply interval.

mod selected;

use crate::index::infrastructure::committed_scalar_files::selected::Selected;

use crate::index::application::engine::index::MAX_INDEX_ITEMS;
use crate::index::domain::sortable_f64::SortableF64;
use crate::ingest::infrastructure::wal::fast_index_scanner::{FastIndexScanner, FastIndexValue};
use crate::persistence::infrastructure::segment::{
    stream::{self, scalar_projection::ScalarProjectionScratch},
    SegmentReader,
};
use crate::shared_kernel::types::schema::FieldType;
use anyhow::{bail, ensure, Context, Result};
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(0);

#[derive(Debug)]
pub(in crate::index) struct PreparedScalarFile {
    pub(in crate::index) reader: Arc<SegmentReader>,
    external_ids: Vec<String>,
    item_ordinals: Vec<usize>,
    field: String,
    bytes: u64,
    pub(in crate::index) retained_bytes: usize,
}

/// A total requirement, not an incremental allocation request. Both callbacks
/// run outside apply. A failed callback leaves the source entirely unchanged.
fn prepare(
    scanner: &FastIndexScanner<'_>,
    field: &str,
    winning: &[usize],
    expected: FieldType,
    sequence: u64,
    parent: &Path,
    reserve_total: impl FnMut(usize) -> Result<()>,
) -> Result<PreparedScalarFile> {
    ensure!(
        scanner.cost().item_count <= MAX_INDEX_ITEMS,
        "committed scalar preparation exceeds existing Index item limit"
    );
    prepare_validated_fields(
        scanner,
        field,
        winning,
        expected,
        sequence,
        parent,
        reserve_total,
    )
}

/// Prepare scalar fields after the command-aware replacement planner has
/// checked its document and flattened-field limits. This retains every field,
/// ordinal, type, and reservation check; it only omits Index's item-count
/// policy so a bounded Replace command may flatten to more than 1,000 fields.
pub(in crate::index) fn prepare_validated_fields(
    scanner: &FastIndexScanner<'_>,
    field: &str,
    winning: &[usize],
    expected: FieldType,
    sequence: u64,
    parent: &Path,
    mut reserve_total: impl FnMut(usize) -> Result<()>,
) -> Result<PreparedScalarFile> {
    ensure!(
        matches!(
            expected,
            FieldType::Keyword | FieldType::Number | FieldType::Set
        ),
        "committed scalar preparation requires Keyword, Number, or Set"
    );
    u32::try_from(winning.len()).context("scalar local row count exceeds u32")?;
    for (row, ordinal) in winning.iter().enumerate() {
        ensure!(
            *ordinal < scanner.cost().item_count,
            "selected scalar ordinal is out of range"
        );
        ensure!(
            !winning[..row].contains(ordinal),
            "selected scalar ordinal repeats"
        );
    }
    // No allocation has occurred above or in this first borrowed pass.
    let mut identifier_bytes = field.len();
    let mut term_bytes = 0usize;
    let mut term_count = 0usize;
    let mut max_entry = scanner
        .cost()
        .item_count
        .checked_mul(5)
        .and_then(|n| n.checked_add(5))
        .context("scalar posting bound overflow")?
        .max(8);
    for (ordinal, item) in scanner.items().enumerate() {
        if !winning.contains(&ordinal) {
            continue;
        }
        ensure!(
            item.field == field,
            "selected scalar item belongs to a different field"
        );
        match (expected, &item.value) {
            (FieldType::Keyword, FastIndexValue::String(value)) => {
                max_entry = max_entry.max(value.len());
                term_bytes = term_bytes
                    .checked_add(value.len())
                    .context("scalar term byte bound overflow")?;
                term_count = term_count
                    .checked_add(1)
                    .context("scalar term count overflow")?
            }
            (FieldType::Number, FastIndexValue::Number(value)) => {
                SortableF64::new(*value)?;
                term_count = term_count
                    .checked_add(1)
                    .context("scalar key count overflow")?;
            }
            (FieldType::Set, FastIndexValue::StringList(list)) => {
                term_count = term_count
                    .checked_add(list.len())
                    .context("scalar set term count overflow")?;
                for value in list.values() {
                    max_entry = max_entry.max(value.len());
                    term_bytes = term_bytes
                        .checked_add(value.len())
                        .context("scalar set byte bound overflow")?;
                }
            }
            _ => bail!("selected scalar value does not match field type"),
        }
        identifier_bytes = identifier_bytes
            .checked_add(item.external_id.len())
            .context("scalar identifier bound overflow")?;
    }
    // Caller-owned descriptors, borrowed lookup nodes, final row maps and IDs.
    // Values are never collected. Codec/spool ownership is priced from the
    // format and the full-entry block boundary in the writer itself.
    let metadata = scanner
        .cost()
        .item_count
        .checked_mul(512)
        .and_then(|n| {
            identifier_bytes
                .checked_mul(4)
                .and_then(|ids| n.checked_add(ids))
        })
        .context("scalar metadata bound overflow")?;
    // Raw dictionaries lend term bytes directly to the file. Only postings
    // need a variable codec block, regardless of one term's length.
    let raw_dictionary = matches!(expected, FieldType::Keyword | FieldType::Set);
    let codec_entry = if raw_dictionary {
        scanner
            .cost()
            .item_count
            .checked_mul(5)
            .and_then(|n| n.checked_add(5))
            .context("scalar posting bound overflow")?
            .max(8)
    } else {
        max_entry
    };
    let workspace = stream::scalar_projection::scalar_projection_peak_bound(
        codec_entry,
        if raw_dictionary { 0 } else { term_bytes },
        term_count,
        term_count,
    )?;
    let total = metadata
        .checked_add(workspace)
        .context("scalar reservation overflow")?;
    reserve_total(total)?;

    let items: Vec<_> = scanner.items().collect();
    let mut ids = BTreeSet::new();
    for &ordinal in winning {
        ensure!(
            ids.insert(items[ordinal].external_id),
            "selected scalar rows repeat external ID"
        );
    }
    let view = Selected {
        items: &items,
        winning,
    };
    let mut directory = StageDirectory::create(parent)?;
    let path = directory.path.join("field.lseg");
    let scratch = ScalarProjectionScratch::new(codec_entry);
    let scratch = if raw_dictionary {
        scratch.raw_scalar_dictionary()
    } else {
        scratch
    };
    match expected {
        FieldType::Keyword => {
            stream::keyword::write_keyword_projection(&path, sequence, &view, scratch)?
        }
        FieldType::Number => {
            stream::number::write_number_projection(&path, sequence, &view, scratch)?
        }
        FieldType::Set => stream::set::write_set_projection(&path, sequence, &view, scratch)?,
        _ => unreachable!("validated scalar field type"),
    }
    let reader_bytes = SegmentReader::staged_metadata_bound(&path)?;
    reserve_total(
        total
            .checked_add(reader_bytes)
            .context("scalar reader bound overflow")?,
    )?;
    let bytes = std::fs::metadata(&path)?.len();
    let reader = Arc::new(SegmentReader::open_owned_stage(
        &path,
        directory.path.clone(),
    )?);
    directory.transferred = true;
    Ok(PreparedScalarFile {
        reader,
        external_ids: winning
            .iter()
            .map(|&ordinal| items[ordinal].external_id.to_owned())
            .collect(),
        item_ordinals: winning.to_vec(),
        field: field.to_owned(),
        bytes,
        retained_bytes: reader_bytes
            .checked_add(
                winning
                    .len()
                    .checked_mul(512)
                    .and_then(|n| n.checked_add(identifier_bytes.checked_mul(4)?))
                    .context("scalar retained metadata bound overflow")?,
            )
            .context("scalar retained bound overflow")?,
    })
}

struct StageDirectory {
    path: PathBuf,
    transferred: bool,
}
impl StageDirectory {
    fn create(parent: &Path) -> Result<Self> {
        for _ in 0..128 {
            let nonce = NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed);
            let path = parent.join(format!(
                "lumen-staged-scalar-{}-{nonce}",
                std::process::id()
            ));
            let mut builder = std::fs::DirBuilder::new();
            #[cfg(unix)]
            {
                use std::os::unix::fs::DirBuilderExt;
                builder.mode(0o700);
            }
            match builder.create(&path) {
                Ok(()) => {
                    return Ok(Self {
                        path,
                        transferred: false,
                    })
                }
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(error) => return Err(error).context("create private scalar stage directory"),
            }
        }
        bail!("could not allocate private scalar stage directory")
    }
}
impl Drop for StageDirectory {
    fn drop(&mut self) {
        if !self.transferred {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }
}

#[cfg(test)]
mod tests;
