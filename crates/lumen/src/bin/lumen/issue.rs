//! `lumen issue`: search, view and file Lumen issues through the shared CLI
//! ops.

use anyhow::Result;

use crate::cli::{IssueArgs, IssueCommand};

/// This binary's identity + build provenance for the standard CLI ops
/// (`upgrade` / `issue`), per the CONTRIBUTING.md CLI convention.
pub(crate) const TOOL: cli_std::ToolInfo = cli_std::ToolInfo::new(
    "lumen",
    "faberline/lumen",
    env!("LUMEN_TARGET"),
    env!("CARGO_PKG_VERSION"),
    env!("LUMEN_GIT_SHA"),
    env!("LUMEN_BUILT_AT"),
);

pub(crate) async fn issue(args: IssueArgs) -> Result<()> {
    match args.command {
        IssueCommand::Search(args) => {
            let query = (!args.query.is_empty()).then(|| args.query.join(" "));
            cli_std::issue::search(
                &TOOL,
                cli_std::issue::SearchOptions::default()
                    .with_query(query)
                    .with_state(args.state)
                    .with_limit(args.limit),
            )
            .await
        }
        IssueCommand::View(args) => cli_std::issue::view(&TOOL, args.number).await,
        IssueCommand::Create(args) => {
            let message = (!args.message.is_empty()).then(|| args.message.join(" "));
            let title = args.title.unwrap_or_else(|| {
                if let Some(message) = message.as_deref() {
                    let head: String = message
                        .lines()
                        .next()
                        .unwrap_or("")
                        .chars()
                        .take(72)
                        .collect();
                    format!("lumen: {head}")
                } else {
                    "lumen: issue report".to_string()
                }
            });
            cli_std::issue::create(
                &TOOL,
                cli_std::issue::CreateOptions::new(title)
                    .with_message(message)
                    .with_url(args.url)
                    .with_repo(args.repo)
                    // Always tag with the project label so reports route
                    // automatically (CLI convention); keep any user labels too.
                    .with_label(
                        std::iter::once("app:lumen".to_string())
                            .chain(args.label)
                            .collect(),
                    )
                    .with_dry_run(args.dry_run)
                    .with_yes(args.yes),
            )
            .await
        }
        IssueCommand::Comment(args) => {
            let message = (!args.message.is_empty()).then(|| args.message.join(" "));
            cli_std::issue::comment(
                &TOOL,
                cli_std::issue::CommentOptions::try_new(args.number)?
                    .with_message(message)
                    .with_repo(args.repo)
                    .with_dry_run(args.dry_run)
                    .with_yes(args.yes),
            )
            .await
        }
    }
}
