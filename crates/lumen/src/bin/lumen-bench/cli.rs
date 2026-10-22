//! The command line: `lumen-bench run`, its arguments, and the cell list
//! `--types` names.

use anyhow::{bail, Result};
use clap::{Parser, Subcommand};

const DEFAULT_DOCUMENTS: usize = 20_000;
const DEFAULT_PAGE_SIZE: u32 = 100;
const DEFAULT_QUERIES: usize = 200;

#[derive(Parser)]
#[command(name = "lumen-bench", version, about = "Lumen local benchmark runner")]
pub(super) struct Cli {
    #[command(subcommand)]
    pub(super) command: Command,
}

#[derive(Subcommand)]
pub(super) enum Command {
    /// Run one or more benchmark cells.
    Run(RunArgs),
}

#[derive(Parser)]
pub(super) struct RunArgs {
    /// Comma-separated cell list. Supported: sorted_page_deep, bool_filter.
    #[arg(long, default_value = "sorted_page_deep")]
    pub(super) types: String,
    /// Compatibility knob used by vat runner specs; accepted but not interpreted yet.
    #[arg(long, default_value = "s")]
    pub(super) tiers: String,
    /// Query/page sample cap. For sorted_page_deep this caps measured pages near depth.
    #[arg(long, default_value_t = DEFAULT_QUERIES)]
    pub(super) queries: usize,
    /// Number of documents in the synthetic corpus.
    #[arg(long, default_value_t = DEFAULT_DOCUMENTS)]
    pub(super) documents: usize,
    /// Page size for sorted cursor walks.
    #[arg(long, default_value_t = DEFAULT_PAGE_SIZE)]
    pub(super) page_size: u32,
}

pub(super) fn parse_types(raw: &str) -> Result<Vec<&'static str>> {
    let mut cells = Vec::new();
    for token in raw.split(',') {
        let cell = token.trim();
        if cell.is_empty() {
            continue;
        }
        let known = match cell {
            "sorted_page_deep" => "sorted_page_deep",
            "bool_filter" => "bool_filter",
            other => bail!("unknown bench cell `{other}`; supported: sorted_page_deep,bool_filter"),
        };
        cells.push(known);
    }
    if cells.is_empty() {
        bail!("--types did not name any bench cells");
    }
    Ok(cells)
}
