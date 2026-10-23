//! Stage the Vector rows and the winning scalar fields' private files outside
//! every lock, growing the borrowed reservation by each step's peak workspace
//! and adding what each step retains to `prepared.retained`.

use std::sync::Arc;

use anyhow::Result;

use crate::index::application::admission::{
    record_reservation::RecordReservation, RecordAdmissionError,
};
use crate::index::application::apply::committed_index_apply::{require, Prepared, VectorRows};
use crate::index::application::apply::committed_index_plan::{PlannedCell, ScalarAction};
use crate::index::application::checkpoint_capture::CheckpointValue;
use crate::index::infrastructure::committed_scalar_files;
use crate::index::infrastructure::staging::staged_vector_row;
use crate::ingest::infrastructure::wal::fast_index_scanner::{FastIndexScanner, FastIndexValue};
use crate::shared_kernel::types::schema::FieldType;
use crate::storage::infrastructure::composed_segment::ComposedSegmentReader;

pub(super) fn stage_committed_files(
    scanner: &FastIndexScanner<'_>,
    sequence: u64,
    prepared: &mut Prepared,
    reservation: &mut RecordReservation,
) -> Result<()> {
    // The source and canonical SQ checkpoint view use private aligned
    // mmap files. Only a bounded decode chunk and reader metadata are
    // admitted. No input or checkpoint Vec is built under apply.
    for action in &prepared.plan.actions {
        let ScalarAction::Apply {
            ordinal,
            cell,
            kind: FieldType::Vector,
            ..
        } = action
        else {
            continue;
        };
        let item = scanner
            .items()
            .nth(*ordinal)
            .expect("validated Vector ordinal");
        let FastIndexValue::Vector { values, len } = item.value else {
            unreachable!("planned Vector wire value")
        };
        let retained = prepared.retained;
        let raw = Arc::new(staged_vector_row::StagedVectorRow::stage(
            values,
            u32::try_from(len).expect("planned Vector dimension fits u32"),
            |workspace| {
                require(
                    reservation,
                    retained
                        .checked_add(workspace)
                        .ok_or(RecordAdmissionError::Overflow)?,
                )
            },
        )?);
        prepared.retained = prepared
            .retained
            .checked_add(staged_vector_row::StagedVectorRow::retained_metadata_bound())
            .ok_or(RecordAdmissionError::Overflow)?;
        let codebook = prepared
            .vector_codebooks
            .get_mut(&cell.field)
            .expect("Vector preparation snapshot");
        let canonical = match codebook {
            Some(snapshot) => {
                let retained = prepared.retained;
                let (row, widened) = raw.stage_sq_canonical(*snapshot, |workspace| {
                    require(
                        reservation,
                        retained
                            .checked_add(workspace)
                            .ok_or(RecordAdmissionError::Overflow)?,
                    )
                })?;
                *snapshot = widened;
                prepared.retained = prepared
                    .retained
                    .checked_add(staged_vector_row::StagedVectorRow::retained_metadata_bound())
                    .ok_or(RecordAdmissionError::Overflow)?;
                Arc::new(row)
            }
            None => raw.clone(),
        };
        prepared
            .vector_rows
            .insert(*ordinal, VectorRows { raw, canonical });
    }
    let parent = std::env::temp_dir();
    for (name, field) in &mut prepared.fields {
        if field.winners.is_empty() {
            continue;
        }
        let winning: Vec<_> = field.winners.iter().map(|(_, ordinal)| *ordinal).collect();
        let retained = prepared.retained;
        let file = committed_scalar_files::prepare_validated_fields(
            scanner,
            name,
            &winning,
            field.kind,
            sequence,
            &parent,
            |peak| {
                require(
                    reservation,
                    retained
                        .checked_add(peak)
                        .ok_or(RecordAdmissionError::Overflow)?,
                )
            },
        )?;
        prepared.retained = prepared
            .retained
            .checked_add(file.retained_bytes)
            .ok_or(RecordAdmissionError::Overflow)?;
        let base = match &field.before {
            Some(base) => base.clone(),
            None => {
                let retained = prepared.retained;
                let empty = committed_scalar_files::prepare_validated_fields(
                    scanner,
                    name,
                    &[],
                    field.kind,
                    sequence,
                    &parent,
                    |peak| {
                        require(
                            reservation,
                            retained
                                .checked_add(peak)
                                .ok_or(RecordAdmissionError::Overflow)?,
                        )
                    },
                )?;
                prepared.retained = prepared
                    .retained
                    .checked_add(empty.retained_bytes)
                    .ok_or(RecordAdmissionError::Overflow)?;
                Arc::new(ComposedSegmentReader::from_private_empty_base(
                    empty.reader,
                )?)
            }
        };
        let ids = field.winners.iter().map(|(id, _)| *id).collect();
        field.after = Some(Arc::new(base.with_private_delta(file.reader.clone(), ids)?));
        for (row, (id, _)) in field.winners.iter().enumerate() {
            *prepared
                .rows
                .get_mut(&PlannedCell {
                    id: *id,
                    field: name.clone(),
                })
                .expect("winner journal row") = Some(Arc::new(CheckpointValue::StagedScalar {
                reader: file.reader.clone(),
                row: row as u32,
            }));
        }
    }
    Ok(())
}
