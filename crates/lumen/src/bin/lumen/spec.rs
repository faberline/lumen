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

    const TARGET_POLICY: &str = include_str!("../../../../../clients/codegen.toml");

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
    write_hydrate_helper(lang, &args.out)?;
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

/// Writes the ordered bulk hydration helper for `lang` into `out` and exports
/// it from the client's entrypoint module (the `index.ts`, `__init__.py` or
/// `mod.rs` that `write_to_dir` emits).
fn write_hydrate_helper(lang: openapi_codegen::Lang, out: &std::path::Path) -> Result<()> {
    use anyhow::Context;
    use openapi_codegen::Lang;

    let (file, source, entry, export) = match lang {
        Lang::Ts => (
            "hydrate.ts",
            include_str!("hydrate/hydrate.ts"),
            "index.ts",
            "export * from \"./hydrate\";\n",
        ),
        Lang::Py => (
            "hydrate.py",
            include_str!("hydrate/hydrate.py"),
            "__init__.py",
            "from .hydrate import Hydrated, HydratedItem, hydrate_hits, hydrate_hits_async  # noqa: F401\n",
        ),
        Lang::Rust => (
            "hydrate.rs",
            include_str!("hydrate/hydrate.rs"),
            "mod.rs",
            "pub mod hydrate;\n",
        ),
    };

    let path = out.join(file);
    std::fs::write(&path, source).with_context(|| format!("write {}", path.display()))?;
    println!("generated {}", path.display());

    let entry_path = out.join(entry);
    let mut entry_text = std::fs::read_to_string(&entry_path)
        .with_context(|| format!("read {}", entry_path.display()))?;
    if !entry_text.is_empty() && !entry_text.ends_with('\n') {
        entry_text.push('\n');
    }
    entry_text.push_str(export);
    std::fs::write(&entry_path, entry_text)
        .with_context(|| format!("write {}", entry_path.display()))?;
    Ok(())
}
