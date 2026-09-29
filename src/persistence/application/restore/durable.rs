//! The durable restore itself: with every mutation excluded, save the candidate
//! as a new segment generation and commit it as `CURRENT`, reload and verify
//! `CURRENT`, then activate the candidate as live state. A failure after
//! `CURRENT` may have moved latches the sink until restart.

use std::sync::Arc;

use anyhow::{anyhow, Result};
use storage_durable::{CommitError, CommitFailureClass};

use crate::index::application::engine::Engine;
use crate::ingest::application::write_coordinator::errors::StorageFullError;
use crate::persistence::application::restore::SegmentRestoreSink;

impl SegmentRestoreSink {
    pub(super) async fn restore_durable(&self, candidate: Arc<Engine>) -> Result<()> {
        let _exclusive = self.gate.exclusive().await?;
        // The returned guard releases apply before every await and filesystem
        // operation below. It prevents old live state from publishing after
        // this candidate owns CURRENT but before activation completes.
        let restore_inhibition = self
            .live_engine
            .capture_barrier
            .apply()
            .inhibit_checkpoints_for_restore();
        let watermark = self.writer.applied_seq();

        let aof = Arc::clone(&self.aof);
        let sync = tokio::task::spawn_blocking(move || {
            let mut aof = aof.lock().map_err(|_| anyhow!("AOF mutex poisoned"))?;
            aof.sync_strict()
        })
        .await
        .map_err(|join| Self::not_committed(anyhow!("strict AOF sync task failed: {join}")))?;
        if let Err(error) = sync {
            if Self::storage_full(&error) {
                self.mark_full();
                return Err(anyhow::Error::new(StorageFullError(error.to_string())).context(error));
            }
            return Err(Self::not_committed(error));
        }

        let store = Arc::clone(&self.store);
        let candidate_for_save = Arc::clone(&candidate);
        let save = tokio::task::spawn_blocking(move || {
            store.save_required(&candidate_for_save, watermark)
        })
        .await;
        let committed_name = match save {
            Ok(Ok(name)) => name,
            Ok(Err(error)) => {
                let full = Self::storage_full(&error);
                if full {
                    self.mark_full();
                }
                if let Some(commit) = error
                    .chain()
                    .find_map(|cause| cause.downcast_ref::<CommitError>())
                {
                    if commit.class() == CommitFailureClass::CommitUncertain {
                        self.restart_while_inhibited(&restore_inhibition);
                        return Err(anyhow::Error::new(
                            crate::ingest::application::write_coordinator::errors::RestartRequired(
                                "segment restore commit outcome is uncertain; restart required"
                                    .into(),
                            ),
                        )
                        .context(error));
                    }
                }
                if full {
                    return Err(
                        anyhow::Error::new(StorageFullError(error.to_string())).context(error)
                    );
                }
                return Err(Self::not_committed(error));
            }
            Err(join) => {
                self.restart_while_inhibited(&restore_inhibition);
                return Err(anyhow::Error::new(
                    crate::ingest::application::write_coordinator::errors::RestartRequired(
                        "segment restore save task panicked; restart required".into(),
                    ),
                )
                .context(anyhow!("save task failed: {join}")));
            }
        };
        if let Some(observer) = &self.publication_observer {
            observer
                .after_candidate_current_published(committed_name.as_str().to_owned(), watermark)
                .await;
        }

        let store = Arc::clone(&self.store);
        let loaded =
            match tokio::task::spawn_blocking(move || store.load_current_generation()).await {
                Ok(loaded) => loaded,
                Err(join) => {
                    self.restart_while_inhibited(&restore_inhibition);
                    return Err(anyhow::Error::new(
                        crate::ingest::application::write_coordinator::errors::RestartRequired(
                            "segment restore reload task panicked; restart required".into(),
                        ),
                    )
                    .context(anyhow!("CURRENT reload task failed: {join}")));
                }
            };
        let loaded = match loaded {
            Ok(Some(loaded)) => loaded,
            Ok(None) => {
                self.restart_while_inhibited(&restore_inhibition);
                return Err(anyhow::Error::new(
                    crate::ingest::application::write_coordinator::errors::RestartRequired(
                        "segment restore committed without a CURRENT generation".into(),
                    ),
                ));
            }
            Err(error) => {
                self.restart_while_inhibited(&restore_inhibition);
                return Err(anyhow::Error::new(
                    crate::ingest::application::write_coordinator::errors::RestartRequired(
                        "segment restore could not reload CURRENT".into(),
                    ),
                )
                .context(error));
            }
        };
        #[cfg(test)]
        let forced_mismatch = self
            .reload_integrity_mismatch
            .swap(false, std::sync::atomic::Ordering::AcqRel);
        #[cfg(not(test))]
        let forced_mismatch = false;
        if forced_mismatch || loaded.name != committed_name || loaded.sequence != watermark {
            self.restart_while_inhibited(&restore_inhibition);
            return Err(anyhow::Error::new(
                crate::ingest::application::write_coordinator::errors::RestartRequired(
                    "segment restore CURRENT integrity check failed; restart required".into(),
                ),
            ));
        }
        let fresh = match Arc::try_unwrap(loaded.engine) {
            Ok(engine) => engine,
            Err(_) => {
                self.restart_while_inhibited(&restore_inhibition);
                return Err(anyhow::Error::new(
                    crate::ingest::application::write_coordinator::errors::RestartRequired(
                        "segment restore reload retained unexpected engine references".into(),
                    ),
                ));
            }
        };
        #[cfg(test)]
        let activation_failure = self
            .activation_failure
            .swap(false, std::sync::atomic::Ordering::AcqRel);
        #[cfg(not(test))]
        let activation_failure = false;
        if activation_failure {
            self.restart_while_inhibited(&restore_inhibition);
            return Err(anyhow::Error::new(
                crate::ingest::application::write_coordinator::errors::RestartRequired(
                    "segment restore live activation failed; restart required".into(),
                ),
            ));
        }
        let activation = restore_inhibition.activation_apply();
        let activated = self.live_engine.activate_replacement(fresh);
        if let Err(error) = activated {
            self.restart_while_inhibited(&restore_inhibition);
            drop(activation);
            return Err(anyhow::Error::new(
                crate::ingest::application::write_coordinator::errors::RestartRequired(
                    "segment restore live activation failed; restart required".into(),
                ),
            )
            .context(error));
        }
        drop(activation);
        drop(restore_inhibition);

        let aof = Arc::clone(&self.aof);
        let store = Arc::clone(&self.store);
        tokio::task::spawn_blocking(move || {
            #[cfg(unix)]
            let trim = (|| {
                let mut plan = {
                    let mut writer = aof.lock().map_err(|_| anyhow!("AOF mutex poisoned"))?;
                    writer.begin_trim(watermark)?
                };
                plan.copy_stable_prefix()?;
                let mut writer = aof.lock().map_err(|_| anyhow!("AOF mutex poisoned"))?;
                writer.finish_trim(plan)
            })();
            #[cfg(not(unix))]
            let trim = aof
                .lock()
                .map_err(|_| anyhow!("AOF mutex poisoned"))
                .and_then(|mut writer| writer.truncate_through(watermark));
            if let Err(error) = trim {
                tracing::warn!(error = %error, "durable restore AOF trim failed");
            }
            if let Err(error) = store.prune(3) {
                tracing::warn!(error = %error, "durable restore generation prune failed");
            }
        })
        .await
        .unwrap_or_else(
            |error| tracing::warn!(error = %error, "durable restore cleanup task failed"),
        );
        Ok(())
    }
}
