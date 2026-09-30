//! `crate::spec` before the DDD split. Re-exports its public items from their
//! new homes so callers outside the crate keep compiling.

pub use crate::app::spec::llm_auth::llm_auth_md;
pub use crate::app::spec::llm_deployment::llm_deployment_md;
pub use crate::app::spec::llm_quickstart::llm_quickstart_md;
pub use crate::app::spec::llm_storage::llm_storage_md;
pub use crate::app::spec::llm_workflow::llm_workflow_md;
pub use crate::app::spec::query_shapes::query_shapes;
pub use crate::app::spec::{
    field_catalog, json_schema_json, llm_integration_md, llm_outline_md, llm_recipes_md,
    openapi_json, openapi_yaml,
};
