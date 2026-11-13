//! The command line: `lumen-acceptance functional|performance` and its arguments.

use std::path::PathBuf;

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(
    name = "lumen-acceptance",
    version,
    about = "Lumen shared acceptance runner"
)]
pub(super) struct Cli {
    #[command(subcommand)]
    pub(super) command: Command,
}

#[derive(Subcommand)]
pub(super) enum Command {
    /// Run the functional mode against a supplied endpoint.
    Functional(RunArgs),
    /// Run the performance mode against a supplied endpoint.
    Performance(RunArgs),
}

#[derive(Parser, Clone)]
pub(super) struct RunArgs {
    /// Base URL of the Lumen under test. Not used with --spawn.
    #[arg(
        long,
        default_value = "",
        required_unless_present = "spawn",
        conflicts_with = "spawn"
    )]
    pub(super) endpoint: String,
    /// Path of the lumen binary. The runner starts it as a child with its own data dir.
    #[arg(long)]
    pub(super) spawn: Option<PathBuf>,
    /// Directory that receives report.json.
    #[arg(long)]
    pub(super) artifact_dir: PathBuf,
    /// Tested commit.
    #[arg(long, default_value = "unknown")]
    pub(super) commit: String,
    /// Image digest under test.
    #[arg(long, default_value = "unknown")]
    pub(super) image_digest: String,
    /// Environment name.
    #[arg(long, default_value = "unknown")]
    pub(super) environment: String,
    /// CPU resource limit, for example 2.5.
    #[arg(long, default_value = "unknown")]
    pub(super) cpu_limit: String,
    /// Memory resource limit, for example 16GiB.
    #[arg(long, default_value = "unknown")]
    pub(super) memory_limit: String,
    /// Seed for the run.
    #[arg(long, default_value_t = 1)]
    pub(super) seed: u64,
    /// Comma-separated phase names. Omitted means the mode's default list.
    #[arg(long)]
    pub(super) phases: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cli_parses_both_modes_and_defaults() {
        let functional = Cli::try_parse_from([
            "lumen-acceptance",
            "functional",
            "--endpoint",
            "http://127.0.0.1:1",
            "--artifact-dir",
            "/tmp/a",
        ])
        .expect("functional parses");
        match functional.command {
            Command::Functional(args) => {
                assert_eq!(args.seed, 1);
                assert_eq!(args.commit, "unknown");
            }
            Command::Performance(_) => panic!("expected functional"),
        }

        let performance = Cli::try_parse_from([
            "lumen-acceptance",
            "performance",
            "--endpoint",
            "http://127.0.0.1:1",
            "--artifact-dir",
            "/tmp/a",
        ])
        .expect("performance parses");
        assert!(matches!(performance.command, Command::Performance(_)));

        let no_endpoint =
            Cli::try_parse_from(["lumen-acceptance", "functional", "--artifact-dir", "/tmp/a"]);
        assert!(no_endpoint.is_err());
    }

    #[test]
    fn spawn_and_endpoint_conflict() {
        let both = Cli::try_parse_from([
            "lumen-acceptance",
            "functional",
            "--endpoint",
            "http://x",
            "--spawn",
            "/bin/lumen",
            "--artifact-dir",
            "/tmp/a",
        ]);
        assert!(both.is_err());
    }

    #[test]
    fn spawn_replaces_endpoint() {
        let parsed = Cli::try_parse_from([
            "lumen-acceptance",
            "functional",
            "--spawn",
            "/bin/lumen",
            "--artifact-dir",
            "/tmp/a",
        ])
        .expect("spawn parses without --endpoint");
        match parsed.command {
            Command::Functional(args) => {
                assert_eq!(args.spawn, Some(PathBuf::from("/bin/lumen")));
            }
            Command::Performance(_) => panic!("expected functional"),
        }
    }
}
