//! `lumen dockerfile`: the runtime image Dockerfiles, from source or from a
//! release.

use std::path::Path;

use anyhow::Result;

use crate::cli::{DockerfileArgs, DockerfileCmd, DockerfileVariant};
use crate::k8s::write_or_print;

/// `lumen dockerfile` — render runtime image artifacts. The checked-in
/// Dockerfiles remain the repo fixtures; CLI output strips ownership markers so
/// the result is the Dockerfile users build.
pub(crate) fn dockerfile(args: DockerfileArgs) -> Result<()> {
    match args.cmd {
        DockerfileCmd::Render(args) => {
            let (file_name, body) = match args.variant {
                DockerfileVariant::Source => ("Dockerfile", render_source_dockerfile()),
                DockerfileVariant::Release => (
                    "Dockerfile.release",
                    render_release_dockerfile(args.version.as_deref()),
                ),
            };
            let variant = args.variant;
            let version = args.version.clone();
            write_or_print(args.out.as_deref(), file_name, &body, move |target| {
                dockerfile_next_command(variant, version.as_deref(), target)
            })
        }
    }
}

/// `next:` builder for `dockerfile render --out` (#963): the matching
/// `docker build` invocation for the variant that was just written.
fn dockerfile_next_command(
    variant: DockerfileVariant,
    version: Option<&str>,
    target: &Path,
) -> String {
    match variant {
        DockerfileVariant::Source => format!("docker build -f {} -t lumen:dev .", target.display()),
        DockerfileVariant::Release => {
            let tag = cli_std::artifact::release_tag("lumen", version, env!("CARGO_PKG_VERSION"));
            let ver = tag.trim_start_matches("lumen@");
            format!(
                "docker build -f {} -t lumen:{ver} --build-arg LUMEN_VERSION={tag} .",
                target.display()
            )
        }
    }
}

fn render_source_dockerfile() -> String {
    cli_std::artifact::strip_source_ownership_markers(include_str!("../../../Dockerfile"))
}

fn render_release_dockerfile(version: Option<&str>) -> String {
    let tag = cli_std::artifact::release_tag("lumen", version, env!("CARGO_PKG_VERSION"));
    let version = tag.trim_start_matches("lumen@");
    let template = cli_std::artifact::strip_source_ownership_markers(include_str!(
        "../../../Dockerfile.release"
    ));
    let mut out = String::new();
    for line in template.lines() {
        if line.starts_with("#   docker build -f Dockerfile.release -t lumen:") {
            out.push_str(&format!(
                "#   docker build -f Dockerfile.release -t lumen:{version} \\"
            ));
        } else if line.starts_with("#     --build-arg LUMEN_VERSION=") {
            out.push_str(&format!("#     --build-arg LUMEN_VERSION={tag} ."));
        } else if line.starts_with("ARG LUMEN_VERSION=") {
            out.push_str(&format!("ARG LUMEN_VERSION={tag}"));
        } else {
            out.push_str(line);
        }
        out.push('\n');
    }
    out
}
