//! The HTTP handlers for collections and their reads: the collection lifecycle,
//! search, batch search and the QUERY twins of both, duplicates and stats.

pub(crate) mod batch_search;
pub(crate) mod collections;
pub(crate) mod duplicates;
pub(crate) mod query_method;
pub(crate) mod search;
pub(crate) mod stats;

/// How many authorization checks one request may have in flight at once.
///
/// The multi-collection paths (`GET /collections`, `POST /collections:search`)
/// ask one `SubjectAccessReview` per collection. Serially that is a round trip
/// per item; unbounded it is a way to point a fleet's whole list surface at the
/// apiserver at once. The cache absorbs the repeat traffic, so this bound only
/// has to keep the cold case civil.
const AUTHORIZATION_CONCURRENCY: usize = 16;
