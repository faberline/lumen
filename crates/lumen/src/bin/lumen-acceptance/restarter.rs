//! How a restart phase restarts the Lumen under test. With `--spawn` it is the owned child
//! (stop, then start). With `--restart-cmd` it runs the caller's shell command and then
//! waits until `GET /readyz` answers 200.

use std::process::Command;
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Result};

use crate::http::HttpApi;
use crate::spawn::SharedChild;

/// How long a restart command gets to bring the endpoint back.
const READY_WAIT: Duration = Duration::from_secs(120);

/// A restart by the caller's command, and the endpoint that must answer after it.
#[derive(Clone)]
pub(super) struct CommandRestart {
    pub(super) command: String,
    pub(super) endpoint: String,
}

/// The one way a restart phase restarts the server under test.
#[derive(Clone)]
pub(super) enum Restarter {
    Child(SharedChild),
    Command(CommandRestart),
}

impl Restarter {
    pub(super) fn restart(&self) -> Result<()> {
        match self {
            Restarter::Child(child) => {
                let mut child = child.lock().map_err(|_| anyhow!("child lock poisoned"))?;
                child.stop()?;
                child.start()
            }
            Restarter::Command(restart) => {
                let output = Command::new("sh")
                    .arg("-c")
                    .arg(&restart.command)
                    .output()?;
                if !output.status.success() {
                    let stderr = String::from_utf8_lossy(&output.stderr);
                    bail!(
                        "restart command failed: {}: {}",
                        output.status,
                        stderr.trim()
                    );
                }
                wait_ready(&HttpApi::new(&restart.endpoint)?, READY_WAIT)
            }
        }
    }
}

/// Polls `GET /readyz` every 500 ms until it answers 200, or fails at `limit`.
pub(super) fn wait_ready(api: &HttpApi, limit: Duration) -> Result<()> {
    let deadline = Instant::now() + limit;
    loop {
        if let Ok((200, _)) = api.call("GET", "/readyz", None) {
            return Ok(());
        }
        if Instant::now() >= deadline {
            bail!(
                "endpoint not ready after restart: GET /readyz did not return 200 within {limit:?}"
            );
        }
        std::thread::sleep(Duration::from_millis(500));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::http::tests::serve_for_test;

    #[test]
    fn command_restart_names_a_failing_command() {
        let restart = Restarter::Command(CommandRestart {
            command: "echo boom >&2; exit 3".into(),
            endpoint: "http://127.0.0.1:9".into(),
        });
        let err = restart
            .restart()
            .expect_err("a failing command fails the restart");
        let text = format!("{err:#}");
        assert!(text.contains("restart command failed"), "{text}");
        assert!(text.contains("boom"), "{text}");
    }

    #[test]
    fn wait_ready_times_out_on_an_unreachable_endpoint() {
        let api = HttpApi::new("http://127.0.0.1:9").expect("client");
        let err = wait_ready(&api, Duration::from_millis(600))
            .expect_err("an unreachable endpoint is never ready");
        assert!(
            format!("{err:#}").contains("GET /readyz did not return 200"),
            "{err:#}"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn command_restart_waits_for_ready() {
        let (_server, url) = serve_for_test();
        tokio::task::spawn_blocking(move || -> Result<()> {
            let restart = Restarter::Command(CommandRestart {
                command: "true".into(),
                endpoint: url,
            });
            restart.restart()
        })
        .await
        .expect("join")
        .expect("a restart that leaves the endpoint ready passes");
    }
}
