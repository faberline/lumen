//! `crate::operator::crd` before the DDD split. Re-exports its public items
//! from their new homes so callers outside the crate keep compiling.

pub use crate::operator::domain::lumen_spec::serving::{
    AuthMode, LogFormat, ServingBackupSpec, ServingBootstrapSpec, ServingSpec,
};
pub use crate::operator::domain::lumen_spec::status::{
    LumenCapacityStatus, LumenReshardStatus, LumenStatus,
};
pub use crate::operator::domain::lumen_spec::topology::{
    ReshardPhase, ReshardPolicy, ReshardWorkflowSpec, ShardMapSpec,
};
pub use crate::operator::domain::lumen_spec::{
    AdmissionSpec, Lumen, LumenSpec, PlacementSpec, Toleration, MAX_BODY_LIMIT_BYTES,
    MIN_BODY_LIMIT_BYTES,
};
