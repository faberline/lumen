//! `crate::operator::reconcile` before the DDD split. Re-exports its public
//! items from their new homes so callers outside the crate keep compiling.

pub use crate::operator::application::reconcile::plan_verdicts::PEER_IDENTITY_CONTEXT_KEY;
pub use crate::operator::application::reconcile::run;
