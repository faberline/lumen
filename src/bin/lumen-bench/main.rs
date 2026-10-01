//! In-process latency gate for the two query shapes lumen is measured on.
//!
//! **`status=pass` on the output line is a literal.** `print_report` hard-codes
//! it, and a report is only printed after the cell has already passed, so the
//! field carries no information. The exit code is the verdict -- a failing cell
//! `bail!`s and prints nothing at all.
//!
//! And only one cell is actually gated. `summarize` stamps `budget_us` into every
//! report from the same `SORTED_PAGE_BUDGET_US` constant, but only
//! `sorted_page_deep` compares its p99 against it. A `cell=bool_filter` line
//! showing `budget_us=5000` is reporting a constant nothing enforced; that cell
//! fails only if a query returns no hits.
//!
//! What `sorted_page_deep` measures is the **deep** pages, not the walk. It walks
//! from page 0 to `(documents / 2) / page_size` following the cursor, and samples
//! only the last `--queries` of those pages, so the cheap early pages are
//! excluded from the percentile by design. `--queries` caps what is measured, not
//! what is walked. Percentiles are nearest-rank over the sorted samples, which
//! means that with fewer than about fifty samples p99 is simply the maximum and
//! the budget check becomes a max check.
//!
//! `--tiers` is accepted and never read (`let _tiers`); it exists so the vat
//! runner specs can pass it.
//!
//! The corpus is synthetic and entirely in-process -- `Engine::new()`, no server
//! and no persistence. `age` is `i % 1_000_000`, so for any corpus below a
//! million documents every sort key is distinct and the sorted walk never pays
//! for ties; `city` is `"taipei"` on every third document. Indexing is batched at
//! `MAX_INDEX_ITEMS / 2` because each document contributes two index items, one
//! per field.

mod cli;
mod report;
mod run;

use anyhow::Result;
use clap::Parser;

use crate::cli::{Cli, Command};
use crate::run::run;

fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Run(args) => run(args),
    }
}
