//! The `lumen serve` child of a `--spawn` run: start it, wait for readiness, stop it
//! and remove its data dir.

use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};

use crate::http::HttpApi;
use crate::run::Cleanup;

const READY_TIMEOUT: Duration = Duration::from_secs(60);
const STOP_TIMEOUT: Duration = Duration::from_secs(30);

/// One `lumen serve` child. The port is chosen once and kept for every start.
pub(super) struct LumenChild {
    binary: PathBuf,
    data_dir: PathBuf,
    port: u16,
    child: Option<Child>,
}

pub(super) type SharedChild = Arc<Mutex<LumenChild>>;

impl LumenChild {
    pub(super) fn new(binary: &Path, data_dir: &Path) -> Result<Self> {
        std::fs::create_dir_all(data_dir)
            .with_context(|| format!("create {}", data_dir.display()))?;
        let listener = TcpListener::bind("127.0.0.1:0").context("reserve a free port")?;
        let port = listener.local_addr()?.port();
        drop(listener);
        Ok(Self {
            binary: binary.to_path_buf(),
            data_dir: data_dir.to_path_buf(),
            port,
            child: None,
        })
    }

    pub(super) fn endpoint(&self) -> String {
        format!("http://127.0.0.1:{}", self.port)
    }

    pub(super) fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    pub(super) fn start(&mut self) -> Result<()> {
        if self.child.is_some() {
            bail!("lumen child already running");
        }
        let mut child = Command::new(&self.binary)
            .args(["serve", "--host", "127.0.0.1", "--port"])
            .arg(self.port.to_string())
            .arg("--data-dir")
            .arg(&self.data_dir)
            // Only the segment store replays the AOF on start, so a restart keeps data.
            .args(["--persistence", "segment"])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .with_context(|| format!("start {}", self.binary.display()))?;

        let api = HttpApi::new(&self.endpoint())?;
        let deadline = Instant::now() + READY_TIMEOUT;
        loop {
            if let Some(status) = child.try_wait()? {
                bail!("lumen child exited before it was ready: {status}");
            }
            if let Ok((200, _)) = api.call("GET", "/readyz", None) {
                self.child = Some(child);
                return Ok(());
            }
            if Instant::now() >= deadline {
                let _ = child.kill();
                let _ = child.wait();
                bail!("lumen child not ready after 60s");
            }
            std::thread::sleep(Duration::from_millis(200));
        }
    }

    /// Sends SIGTERM, waits up to 30 s, then kills. Does nothing when no child runs.
    pub(super) fn stop(&mut self) -> Result<()> {
        let Some(mut child) = self.child.take() else {
            return Ok(());
        };
        let _ = Command::new("kill")
            .args(["-TERM", &child.id().to_string()])
            .status();
        let deadline = Instant::now() + STOP_TIMEOUT;
        loop {
            if child.try_wait()?.is_some() {
                return Ok(());
            }
            if Instant::now() >= deadline {
                child.kill()?;
                child.wait()?;
                return Ok(());
            }
            std::thread::sleep(Duration::from_millis(100));
        }
    }
}

impl Drop for LumenChild {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

/// Stops the child and removes its data dir. The removal is always tried.
pub(super) struct SpawnCleanup(pub(super) SharedChild);

impl Cleanup for SpawnCleanup {
    fn run(&self) -> Result<()> {
        let mut child = self.0.lock().map_err(|_| anyhow!("child lock poisoned"))?;
        let stopped = child.stop();
        let removed = match std::fs::remove_dir_all(child.data_dir()) {
            Ok(()) => Ok(()),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(anyhow::Error::from(err)),
        };
        stopped.and(removed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_picks_a_port_and_creates_the_dir() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let data = tmp.path().join("d");
        let child = LumenChild::new(Path::new("/nonexistent/lumen"), &data).expect("new");
        assert!(child.data_dir().is_dir());
        assert!(child.endpoint().starts_with("http://127.0.0.1:"));
    }

    #[test]
    fn start_fails_for_a_missing_binary() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let mut child =
            LumenChild::new(Path::new("/nonexistent/lumen"), &tmp.path().join("d")).expect("new");
        assert!(child.start().is_err());
    }

    #[test]
    fn cleanup_removes_the_data_dir() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let data = tmp.path().join("d");
        let child = LumenChild::new(Path::new("/nonexistent/lumen"), &data).expect("new");
        assert!(data.is_dir());
        let shared: SharedChild = Arc::new(Mutex::new(child));
        SpawnCleanup(shared).run().expect("cleanup succeeds");
        assert!(!data.exists());
    }
}
