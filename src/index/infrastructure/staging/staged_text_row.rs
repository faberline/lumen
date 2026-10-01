//! A file-owned, one-row Text reader prepared before apply.
//!
//! The staging writer owns bounded scratch space. This receipt owns only the
//! final staged file through its reader. The caller must reserve raw input,
//! staging workspace, row handles, and reader metadata separately.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use anyhow::{anyhow, bail, Context, Result};

#[cfg(test)]
use std::cell::RefCell;

#[cfg(not(feature = "jieba"))]
use crate::index::domain::analysis::jieba_fallback_stream;
use crate::index::domain::analysis::{ngram_stream, tokenize};
#[cfg(feature = "jieba")]
use crate::index::infrastructure::analysis::jieba_disk_route;
use crate::persistence::infrastructure::segment::text_row_stage::{
    stage_text_row, TextRowStageOptions, TextTokenStream,
};
use crate::persistence::infrastructure::segment::SegmentReader;
use crate::shared_kernel::types::schema::Analyzer;

static STAGED_TEXT_ROW_NONCE: AtomicU64 = AtomicU64::new(0);

#[cfg(test)]
thread_local! {
    static LAST_STAGE_DIRECTORY: RefCell<Option<PathBuf>> = const { RefCell::new(None) };
}

/// Test-only observation of actual stage-directory creation. It has no effect
/// on stage ownership or cleanup.
#[cfg(test)]
pub(in crate::index) fn last_stage_directory_for_test() -> Option<PathBuf> {
    LAST_STAGE_DIRECTORY.with(|last| last.borrow().clone())
}

/// One prepared Text value. The reader's private stage guard keeps the exact
/// locally-created directory alive until its final `Arc` is dropped.
#[derive(Debug)]
pub(crate) struct StagedTextRow {
    reader: Arc<SegmentReader>,
    input_bytes: usize,
    doc_len: u32,
    unique_term_count: u64,
    total_unique_term_bytes: u64,
}

/// Temporary workspace is released when staging returns; reader bytes follow
/// the final mmap owner through apply and checkpoint.
#[derive(Clone, Copy)]
pub(in crate::index) enum StageAllocation {
    Workspace(usize),
    Reader(usize),
}

impl StagedTextRow {
    #[cfg(test)]
    pub(in crate::index) fn stage(
        input: &str,
        analyzer: Analyzer,
        scratch_bytes: usize,
        mut reserve_reader: impl FnMut(usize) -> Result<()>,
    ) -> Result<Self> {
        Self::stage_charged(
            input,
            analyzer,
            scratch_bytes,
            |allocation| match allocation {
                StageAllocation::Reader(bytes) => reserve_reader(bytes),
                StageAllocation::Workspace(_) => Ok(()),
            },
        )
    }

    /// Stage before apply. The caller has priced the row writer and the
    /// bounded short-token normalizer before entering this method.
    pub(in crate::index) fn stage_charged(
        input: &str,
        analyzer: Analyzer,
        scratch_bytes: usize,
        mut reserve: impl FnMut(StageAllocation) -> Result<()>,
    ) -> Result<Self> {
        let mut stage_dir = StageDirectory::create()?;
        let segment_path = stage_dir.path().join("row.lseg");
        let options = TextRowStageOptions { scratch_bytes };
        let has_large_token = analyzer == Analyzer::WhitespaceLower
            && input.split_whitespace().any(|raw| {
                raw.trim_matches(|c: char| !c.is_alphanumeric()).len()
                    > super::large_text_row::LARGE_TOKEN_SOURCE_BYTES
            });
        let (doc_len, reader_bytes) = if has_large_token {
            let receipt = super::large_text_row::stage_large_whitespace_row(
                input,
                &segment_path,
                stage_dir.path(),
                options,
                super::large_text_row::LARGE_TOKEN_SOURCE_BYTES,
                |bytes| reserve(StageAllocation::Workspace(bytes)),
            )?;
            (receipt.doc_len, receipt.final_reader_metadata_bytes)
        } else {
            #[cfg(feature = "jieba")]
            let mut route = if analyzer == Analyzer::Jieba {
                Some(jieba_disk_route::DiskRoute::create(stage_dir.path())?)
            } else {
                None
            };
            let doc_len = document_len(
                input,
                analyzer,
                #[cfg(feature = "jieba")]
                route.as_mut(),
            )?;
            let stream = token_stream(
                input,
                analyzer,
                #[cfg(feature = "jieba")]
                route,
            )?;
            stage_text_row(&segment_path, 0, doc_len, stage_dir.path(), options, stream)?;
            (
                doc_len,
                SegmentReader::staged_metadata_bound(&segment_path)?,
            )
        };

        // Every temporary helper mapping has dropped. Price the final reader
        // before opening it, then transfer the directory to its exact owner.
        reserve(StageAllocation::Reader(reader_bytes))?;
        let reader = Arc::new(SegmentReader::open_owned_stage(
            &segment_path,
            stage_dir.path().to_owned(),
        )?);
        let (unique_term_count, total_unique_term_bytes) = reader
            .text_dictionary_stats()
            .ok_or_else(|| anyhow!("staged Text dictionary is unreadable"))?;
        stage_dir.transfer();
        Ok(Self {
            reader,
            input_bytes: input.len(),
            doc_len,
            unique_term_count,
            total_unique_term_bytes,
        })
    }

    pub(in crate::index) fn reader(&self) -> &Arc<SegmentReader> {
        &self.reader
    }

    pub(in crate::index) fn doc_len(&self) -> u32 {
        self.doc_len
    }

    pub(in crate::index) fn input_bytes(&self) -> usize {
        self.input_bytes
    }

    /// Match the live Text index accounting: each distinct token stores its
    /// bytes plus this document's external ID once.
    pub(in crate::index) fn indexed_bytes(&self, external_id: &str) -> u64 {
        // Match the existing live `TextIndex::bytes` arithmetic exactly. A
        // staged row has at most its checked `u32` document-token count, so
        // these sums are bounded by the admitted record payload in practice.
        self.total_unique_term_bytes + self.unique_term_count * external_id.len() as u64
    }
}

struct StageDirectory {
    path: PathBuf,
    transferred: bool,
}

impl StageDirectory {
    fn create() -> Result<Self> {
        let root = std::env::temp_dir();
        let process = std::process::id();
        for _ in 0..128 {
            let nonce = STAGED_TEXT_ROW_NONCE.fetch_add(1, Ordering::Relaxed);
            let path = root.join(format!("lumen-staged-text-row-{process}-{nonce}"));
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
                    return Ok(Self {
                        path,
                        transferred: false,
                    });
                }
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(error) => {
                    return Err(error).with_context(|| {
                        format!("create staged Text directory {}", path.display())
                    });
                }
            }
        }
        bail!("could not allocate a unique staged Text directory")
    }

    fn path(&self) -> &Path {
        &self.path
    }

    fn transfer(&mut self) {
        self.transferred = true;
    }
}

impl Drop for StageDirectory {
    fn drop(&mut self) {
        if !self.transferred {
            let _ = fs::remove_dir_all(&self.path);
        }
    }
}

fn document_len(
    input: &str,
    analyzer: Analyzer,
    #[cfg(feature = "jieba")] route: Option<&mut jieba_disk_route::DiskRoute>,
) -> Result<u32> {
    match analyzer {
        Analyzer::Ngram => {
            ngram_stream::stream_default_ngrams(input, |_| Ok::<_, anyhow::Error>(()))
                .map_err(|error| anyhow!("stream staged Ngram Text length: {error}"))
        }
        Analyzer::WhitespaceLower => Ok(tokenize::for_whitespace_lower_cow(input, |_| {})),
        #[cfg(not(feature = "jieba"))]
        Analyzer::Jieba => {
            jieba_fallback_stream::stream_fallback_jieba(input, |_| Ok::<_, anyhow::Error>(()))
                .map_err(|error| anyhow!("stream staged fallback Jieba Text length: {error}"))
        }
        #[cfg(feature = "jieba")]
        Analyzer::Jieba => Ok(index_text::for_jieba_no_hmm(
            input,
            route.ok_or_else(|| anyhow!("missing dictionary Jieba route"))?,
            |_| Ok(()),
        )?),
    }
}

fn token_stream<'a>(
    input: &'a str,
    analyzer: Analyzer,
    #[cfg(feature = "jieba")] route: Option<jieba_disk_route::DiskRoute>,
) -> Result<Box<TextTokenStream<'a>>> {
    match analyzer {
        Analyzer::Ngram => Ok(Box::new(
            move |emit| match ngram_stream::stream_default_ngrams(input, |token| emit(token)) {
                Ok(_) => Ok(()),
                Err(ngram_stream::NgramStreamError::Callback(error)) => Err(error),
                Err(ngram_stream::NgramStreamError::TokenCountOverflow) => {
                    bail!("staged Ngram Text document length exceeds u32")
                }
            },
        )),
        Analyzer::WhitespaceLower => Ok(Box::new(move |emit| {
            let mut failure = None;
            tokenize::for_whitespace_lower_cow(input, |token| {
                if failure.is_none() {
                    if let Err(error) = emit(token.as_ref()) {
                        failure = Some(error);
                    }
                }
            });
            match failure {
                Some(error) => Err(error),
                None => Ok(()),
            }
        })),
        #[cfg(not(feature = "jieba"))]
        Analyzer::Jieba => {
            Ok(Box::new(
                move |emit| match jieba_fallback_stream::stream_fallback_jieba(input, |token| {
                    emit(token)
                }) {
                    Ok(_) => Ok(()),
                    Err(jieba_fallback_stream::JiebaFallbackStreamError::Callback(error)) => {
                        Err(error)
                    }
                    Err(jieba_fallback_stream::JiebaFallbackStreamError::TokenCountOverflow) => {
                        bail!("staged fallback Jieba Text document length exceeds u32")
                    }
                },
            ))
        }
        #[cfg(feature = "jieba")]
        Analyzer::Jieba => {
            let mut route = route.ok_or_else(|| anyhow!("missing dictionary Jieba route"))?;
            Ok(Box::new(move |emit| {
                // Preserve the callback's concrete workspace error for the
                // caller's bounded retry. I/O errors keep their own cause.
                let mut failure = None;
                let result =
                    index_text::for_jieba_no_hmm(input, &mut route, |token| match emit(token) {
                        Ok(()) => Ok(()),
                        Err(error) => {
                            failure = Some(error);
                            Err(std::io::Error::other("stop staged Jieba callback"))
                        }
                    });
                match failure {
                    Some(error) => Err(error),
                    None => result.map(|_| ()).map_err(Into::into),
                }
            }))
        }
    }
}

#[cfg(test)]
mod tests;
