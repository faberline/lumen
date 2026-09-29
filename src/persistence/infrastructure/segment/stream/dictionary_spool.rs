//! A disposable live dictionary spooled to its own temp segment, so a scalar
//! seal finds each row's ordinal by binary search instead of walking every
//! term.

use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

use anyhow::{anyhow, Context, Result};

use crate::composed_segment::ComposedSegmentReader;
use crate::persistence::infrastructure::segment::format::header_block;
use crate::persistence::infrastructure::segment::stream::var_writer::{
    new_stream_temp, pad_to_page, write_counted, StreamingVarWriter,
};
use crate::persistence::infrastructure::segment::stream::write_stream_directory;
use crate::persistence::infrastructure::segment::*;

/// A disposable, complete live dictionary encoded with the ordinary segment
/// VAR codec.  It replaces the old `rows * terms` ordinal walk with the
/// reader's binary search over bounded decoded blocks.  The file is created
/// with `create_new` and owns its own cleanup, so no other compaction temp can
/// be replaced or removed.
pub(super) struct DictionarySpool {
    pub(super) reader: SegmentReader,
    pub(super) path: PathBuf,
    pub(super) count: u64,
}

impl DictionarySpool {
    pub(super) fn build(
        target: &Path,
        seq: u64,
        view: &ComposedSegmentReader,
        mut live: impl FnMut(&str) -> bool,
    ) -> Result<Self> {
        let (path, file) = new_stream_temp(target)?;
        let cleanup_path = path.clone();
        let result = (|| {
            let mut out = BufWriter::new(file);
            let mut at = 0u64;
            write_counted(&mut out, &mut at, &header_block(seq, 0, 0, 0))?;
            pad_to_page(&mut out, &mut at)?;
            let dict_start = at;
            let mut dict = StreamingVarWriter::new();
            let mut terms = view.string_terms(false)?;
            while let Some(term) = terms.next()? {
                if live(&term) {
                    dict.push(term.as_bytes(), &mut out, &mut at)?;
                }
            }
            let (skip, off, len, count) = dict.finish(&mut out, &mut at, dict_start)?;
            write_stream_directory(
                &mut out,
                &mut at,
                vec![ColumnRef {
                    name: "dict".to_owned(),
                    role: ROLE_DICT,
                    byte_offset: off,
                    byte_len: len,
                    elem_count: count,
                    width: 0,
                    codec: CODEC_LZ4_VAR,
                    skip_index: skip,
                }],
            )?;
            out.flush()
                .with_context(|| format!("flush {}", path.display()))?;
            out.get_ref()
                .sync_all()
                .with_context(|| format!("fsync {}", path.display()))?;
            drop(out);
            let reader = SegmentReader::open(&path)
                .with_context(|| format!("open dictionary spool {}", path.display()))?;
            Ok(Self {
                reader,
                path,
                count,
            })
        })();
        if result.is_err() {
            let _ = std::fs::remove_file(&cleanup_path);
        }
        result
    }

    pub(super) fn dict_id(&self, value: &str) -> Result<u32> {
        #[cfg(test)]
        STREAM_SPOOL_LOOKUPS.with(|lookups| lookups.set(lookups.get() + 1));
        self.reader.keyword_dict_id(value).ok_or_else(|| {
            anyhow!(
                "composed string value missing from dictionary spool ({} bytes)",
                value.len()
            )
        })
    }
}

impl Drop for DictionarySpool {
    fn drop(&mut self) {
        // `path` came from our `create_new` call.  A failed cleanup is harmless:
        // it leaves only this process's uniquely-named temporary segment.
        let _ = std::fs::remove_file(&self.path);
    }
}

#[cfg(test)]
std::thread_local! {
    pub(super) static STREAM_SPOOL_LOOKUPS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}
