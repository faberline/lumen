//! Check behavior through the public compatibility facade used by clients.

use crate::types::{
    validate_batch_unindex_docs_request, Analyzer, BatchUnindexDocsRequest,
    BatchUnindexDocsValidationError, VectorBackend, MAX_BATCH_UNINDEX_DOCS_SIZE,
};

#[test]
fn facade_tokenizer_retains_term_frequency_and_unicode_whitespace() {
    assert_eq!(
        crate::tokenize::tokenize("Hello\u{2003}HELLO Rust", Analyzer::WhitespaceLower),
        ["hello", "hello", "rust"]
    );
    assert!(crate::tokenize::tokenize(" \t\n", Analyzer::WhitespaceLower).is_empty());
}

#[test]
fn facade_unindex_validation_rejects_empty_duplicates_and_oversized_batches() {
    let request = |ids: Vec<String>| BatchUnindexDocsRequest { external_ids: ids };
    assert_eq!(
        validate_batch_unindex_docs_request(&request(vec![])),
        Err(BatchUnindexDocsValidationError::Empty)
    );
    assert_eq!(
        validate_batch_unindex_docs_request(&request(vec!["opaque/id".into(), "opaque/id".into()])),
        Err(BatchUnindexDocsValidationError::DuplicateExternalId {
            external_id: "opaque/id".into()
        })
    );
    let too_many: Vec<_> = (0..=MAX_BATCH_UNINDEX_DOCS_SIZE)
        .map(|id| id.to_string())
        .collect();
    assert_eq!(
        validate_batch_unindex_docs_request(&request(too_many)),
        Err(BatchUnindexDocsValidationError::TooLarge {
            got: MAX_BATCH_UNINDEX_DOCS_SIZE + 1,
            max: MAX_BATCH_UNINDEX_DOCS_SIZE
        })
    );
    assert_eq!(
        validate_batch_unindex_docs_request(&request(vec!["opaque/id".into(), "".into()])),
        Ok(())
    );
}

#[test]
fn facade_vector_backend_keeps_wire_names_and_default() {
    assert_eq!(VectorBackend::default(), VectorBackend::HnswCpu);
    for (wire, backend) in [
        ("hnsw-cpu", VectorBackend::HnswCpu),
        ("flat-cpu", VectorBackend::FlatCpu),
    ] {
        assert_eq!(serde_json::to_value(backend).unwrap(), wire);
        assert_eq!(
            serde_json::from_value::<VectorBackend>(serde_json::json!(wire)).unwrap(),
            backend
        );
    }
    assert!(serde_json::from_str::<VectorBackend>("\"hnsw_gpu\"").is_err());
}
