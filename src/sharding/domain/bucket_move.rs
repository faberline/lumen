//! The virtual buckets whose owner changes between two map versions.

use anyhow::{bail, Result};

use crate::sharding::domain::virtual_bucket_shard_map::VirtualBucketShardMap;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BucketMove {
    pub bucket: u32,
    pub from_shard: u32,
    pub to_shard: u32,
}

/// Return the virtual buckets whose physical owner changes between two map
/// versions. A shard split keeps the virtual bucket count stable and changes
/// assignments in small increments.
pub fn bucket_moves(
    from: &VirtualBucketShardMap,
    to: &VirtualBucketShardMap,
) -> Result<Vec<BucketMove>> {
    if from.virtual_bucket_count() != to.virtual_bucket_count() {
        bail!(
            "reshard requires stable virtual_bucket_count ({} != {})",
            from.virtual_bucket_count(),
            to.virtual_bucket_count()
        );
    }

    let mut moves = Vec::new();
    for bucket in 0..from.virtual_bucket_count() {
        let from_shard = from
            .assignment_for_bucket(bucket)
            .expect("bucket within from-map range");
        let to_shard = to
            .assignment_for_bucket(bucket)
            .expect("bucket within to-map range");
        if from_shard != to_shard {
            moves.push(BucketMove {
                bucket,
                from_shard,
                to_shard,
            });
        }
    }
    Ok(moves)
}
