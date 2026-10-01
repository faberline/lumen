//! `crate::types` before the DDD split. Re-exports its public items from their
//! new homes so callers outside the crate keep compiling.

pub use crate::shared_kernel::types::api_error::ApiError;
pub use crate::shared_kernel::types::document::{
    validate_batch_unindex_docs_request, BatchUnindexDocsRequest, BatchUnindexDocsValidationError,
    FieldValue, IndexItem, IndexRequest, IndexResponse, ReplaceDocBody, ReplaceDocItem,
    ReplaceDocResult, ReplaceDocsRequest, ReplaceDocsResponse, MAX_BATCH_REPLACE_SIZE,
    MAX_BATCH_UNINDEX_DOCS_SIZE, MAX_INDEX_BATCH_SIZE,
};
pub use crate::shared_kernel::types::query::{
    DuplicatedQuery, ExistsQuery, HammingQuery, HasChildQuery, IdsQuery, KnnQuery, MatchOp,
    MatchQuery, PrefixQuery, QueryNode, RangeBound, RangeQuery, RrfQuery, SortMissing, SortOrder,
    SortSpec, TermQuery, TermsQuery,
};
pub use crate::shared_kernel::types::schema::{
    Analyzer, CreateCollectionRequest, CreateCollectionResponse, FieldCapabilities, FieldSpec,
    FieldType, VectorBackend, VectorMetric, VectorQuantize, VectorSpec,
};
pub use crate::shared_kernel::types::search::{
    BatchSearchItem, BatchSearchRequest, BatchSearchResponse, BatchSearchResult, DuplicateGroup,
    DuplicatesRequest, DuplicatesResponse, SearchAllRequest, SearchAllResponse, SearchHit,
    SearchRequest, SearchResponse, MAX_BATCH_SEARCH_SIZE,
};
pub use crate::shared_kernel::types::stats::{CacheStats, FieldStats, StatsResponse, StorageStats};
