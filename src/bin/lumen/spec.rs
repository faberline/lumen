//! `lumen spec gen`: generate a typed client from lumen's own OpenAPI document.

use std::path::PathBuf;

use anyhow::Result;

use crate::cli::spec::{GenArgs, GenHttp, GenLang};

/// `lumen spec gen` — generate a typed client from lumen's own OpenAPI document
/// (offline; no engine or server) and write it into `--out`.
pub(crate) fn spec_gen(args: GenArgs) -> Result<()> {
    use openapi_codegen::{
        generate_for_target_with_file_bearer_auth as generate_for_target_with_file_auth,
        FileBearerAuth, FileBearerScheme, GenOptions, HttpClient, Lang, TargetPolicy,
        MANIFEST_FILE,
    };

    const TARGET_POLICY: &str = include_str!("../../../clients/codegen.toml");

    let lang = match args.lang {
        GenLang::Ts => Lang::Ts,
        GenLang::Py => Lang::Py,
        GenLang::Rust => Lang::Rust,
    };
    let target = TargetPolicy::from_toml(TARGET_POLICY)?.resolve(lang, args.target.as_deref())?;
    let opts = GenOptions {
        lang,
        target: Some(target),
        spec_path: PathBuf::new(),
        out_dir: args.out.clone(),
        client_name: "createClient".to_string(),
        http_client: match args.http {
            GenHttp::Fetch => HttpClient::Fetch,
            GenHttp::Axios => HttpClient::Axios,
        },
        emit_types: true,
        emit_client: true,
        // TanStack Query hooks are a TypeScript-only concern.
        emit_hooks: matches!(lang, Lang::Ts),
    };
    let auth = FileBearerAuth::new(
        "/var/run/secrets/kubernetes.io/serviceaccount/token",
        ".svc.cluster.local",
        [FileBearerScheme::Http, FileBearerScheme::Https],
    )?;
    let output =
        generate_for_target_with_file_auth(&lumen::spec::openapi_json(), &opts, target, &auth)?;
    output.write_to_dir(&args.out)?;
    for file in &output.files {
        let path = args.out.join(&file.rel_path);
        println!("generated {}", path.display());
    }
    println!("generated {}", args.out.join(MANIFEST_FILE).display());
    let requirements = output.requirements.expect("explicit target requirements");
    println!(
        "target: {} (minimum {} {})",
        requirements.target,
        requirements.language.id(),
        requirements.minimum_version
    );
    // Chainable output (#963): point at the generated client's entrypoint
    // module — the one file every language always emits (unconditionally
    // pushed by each emitter regardless of `--emit-*` selection).
    let entry_file = match lang {
        Lang::Ts => "index.ts",
        Lang::Py => "__init__.py",
        Lang::Rust => "mod.rs",
    };
    println!("next: {}", args.out.join(entry_file).display());
    Ok(())
}
