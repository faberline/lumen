//! Lumen shared acceptance runner. Slice 1 of #114: the runner skeleton only.
//! It adds no real functional or performance phase.

mod cli;
#[allow(dead_code)] // used by the performance phase of slice 7 (#264)
mod dataset;
mod http;
mod lifecycle;
#[allow(dead_code)] // used by the performance phase of slice 7 (#264)
mod load;
mod perf_gate;
mod report;
mod run;
mod spawn;
mod visible_version;
mod writes;

use std::process::ExitCode;

use clap::Parser;

use crate::cli::{Cli, Command};
use crate::report::Mode;

fn main() -> ExitCode {
    let cli = Cli::parse();
    let result = match cli.command {
        Command::Functional(args) => run::execute(Mode::Functional, args),
        Command::Performance(args) => run::execute(Mode::Performance, args),
    };
    match result {
        Ok(true) => ExitCode::SUCCESS,
        Ok(false) => ExitCode::FAILURE,
        Err(err) => {
            eprintln!("lumen-acceptance: {err:#}");
            ExitCode::FAILURE
        }
    }
}
