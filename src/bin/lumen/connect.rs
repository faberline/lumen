//! `lumen connect`: port-forward to the fleet and run a command against it.

pub(crate) mod proxy;

use std::time::Duration;

use anyhow::{Context, Result};

use crate::cli::client::ConnectArgs;
use crate::connect::proxy::{probe_serving_tls, serving_trust, start_client_proxy};

/// `lumen connect` (#1321, R1): spawn `kubectl port-forward`, wait until the
/// local end is reachable, run the wrapped command with `LUMEN_URL` set, then
/// tear the port-forward down (`ChildGuard::drop`) once the wrapped command
/// exits — regardless of its exit status — so no port-forward process is left
/// for the caller to track.
///
/// The child's environment gains exactly one variable, and it is a URL. It
/// used to also gain a bearer token, which meant every descendant of the
/// wrapped command — and anything that could read `/proc/<pid>/environ` —
/// inherited a bearer nobody had scoped to them. #2873 deleted that token;
/// #2878 gives the credential back without giving it to the child: with
/// `--client-sa`, the token is minted by `TokenRequest` through the caller's
/// own kubeconfig, held in this process, and attached to the child's requests
/// as they pass through a loopback proxy. The URL the child gets points at the
/// proxy rather than at the port-forward, and that is the only difference the
/// child can observe.
pub(crate) async fn connect(args: ConnectArgs) -> Result<()> {
    let service = args
        .service
        .clone()
        .or_else(|| args.cr.clone())
        .context("--service or --cr is required")?;

    // Which name this connection is *for*, as opposed to which socket it goes
    // through (#3113 R6). In TLS mode the upstream URL carries the Service's
    // own DNS name, so SNI, hostname verification, and the `Host` header all
    // address the identity the serving certificate asserts; only address
    // resolution points at the tunnel.
    let server_name = args
        .server_name
        .clone()
        .unwrap_or_else(|| format!("{service}.{}.svc", args.namespace));

    // Settled before anything is spawned: a transport nobody chose is not a
    // thing to discover after a port-forward is running.
    if args.ca_file.is_none() && !args.plaintext {
        anyhow::bail!(
            "say how this connection is secured: `--ca-file <PATH>` verifies {server_name} \
             against the fleet's published trust bundle, and `--plaintext` talks to a \
             development instance that serves no certificate.\nThere is no default because the \
             wrong one is silent: cleartext against a TLS fleet fails inside the wrapped \
             command's first request, and reads as a networking problem rather than as a \
             transport that was never chosen."
        );
    }

    let local_port = match args.local_port {
        Some(port) => port,
        None => cli_std::connect::free_local_port()?,
    };

    let mut pf_cmd = std::process::Command::new("kubectl");
    if let Some(ctx) = &args.context {
        pf_cmd.args(["--context", ctx]);
    }
    pf_cmd.args([
        "port-forward",
        "-n",
        &args.namespace,
        &format!("svc/{service}"),
        &format!("{local_port}:{}", args.remote_port),
    ]);
    pf_cmd.stdout(std::process::Stdio::null());
    pf_cmd.stderr(std::process::Stdio::null());
    let _forward =
        cli_std::connect::ChildGuard::spawn(&mut pf_cmd).context("start kubectl port-forward")?;

    cli_std::connect::wait_for_local_port_ready(local_port, Duration::from_secs(30))?;

    let trust = match &args.ca_file {
        Some(path) => Some(serving_trust(path, &server_name, local_port)?),
        None => None,
    };
    let upstream = match &trust {
        Some(_) => format!("https://{server_name}"),
        None => format!("http://127.0.0.1:{local_port}"),
    };

    // One probe before anything else runs. Every way TLS goes wrong here —
    // the wrong CA, the wrong name, an expired leaf, a fleet still serving
    // cleartext — is a fact about the deployment, and it should be reported
    // as one rather than surfacing as the wrapped command's first request
    // failing with a transport error (#3113 R7).
    if let Some(client) = &trust {
        probe_serving_tls(client, &server_name, args.ca_file.as_deref()).await?;
    }

    // Mint before spawning anything. A missing RBAC grant is the most common
    // way this command fails, and it should fail here — where the error can
    // name the account and the check — rather than three layers down inside
    // the child's first HTTP response (R6).
    let mut proxy = match &args.client_sa {
        Some(client_sa) => Some(start_client_proxy(&args, client_sa, &upstream, trust).await?),
        None => None,
    };

    let child_url = match &proxy {
        Some(proxy) => {
            eprintln!(
                "lumen connect: forwarding {} -> {upstream} -> svc/{service}:{} in {}, \
                 authenticating as serviceaccount {}/{}{}. The token is held here; the wrapped \
                 command sees only the local URL.",
                proxy.local_url(),
                args.remote_port,
                args.namespace,
                args.namespace,
                args.client_sa.as_deref().unwrap_or_default(),
                match &args.ca_file {
                    Some(path) => format!(", verifying {server_name} against {}", path.display()),
                    None => ", over cleartext (--plaintext)".to_string(),
                },
            );
            proxy.local_url()
        }
        None => {
            // R4: say out loud that this connection is unauthenticated, on
            // stderr so the wrapped command's own stdout stays
            // machine-readable. A caller who gets a 401 deserves to be told
            // why here, not left to infer it from the server's response.
            eprintln!(
                "lumen connect: forwarding {upstream} -> svc/{service}:{} in {} with no \
                 credential. Pass --client-sa <NAME> to authenticate as a ServiceAccount; \
                 without it a serving instance with `auth: required` refuses every request, and \
                 `auth: disabled` accepts them all.",
                args.remote_port, args.namespace
            );
            upstream.clone()
        }
    };

    let (program, rest) = args
        .command
        .split_first()
        .context("wrapped command is empty")?;
    let mut child_cmd = tokio::process::Command::new(program);
    child_cmd.args(rest);
    child_cmd.env("LUMEN_URL", &child_url);
    let mut child = child_cmd.spawn().context("run wrapped command")?;

    // Three ways this ends, and all three must tear down the port-forward and
    // the proxy (R7). The child exiting is the ordinary one. A token refresh
    // that fails means the grant was revoked mid-session: keeping the child
    // alive would leave it talking to a proxy that can only answer 503, so we
    // end it. Ctrl-C is the caller ending it.
    enum Ending {
        Child(std::process::ExitStatus),
        Refresh(String),
        Interrupted,
    }
    let ending = tokio::select! {
        status = child.wait() => Ending::Child(status.context("wait for the wrapped command")?),
        fatal = next_fatal(proxy.as_mut()) => Ending::Refresh(fatal),
        signal = tokio::signal::ctrl_c() => {
            signal.context("listen for interrupt")?;
            Ending::Interrupted
        }
    };

    // `_forward` and `proxy` drop at the end of this scope on every path, so
    // the port-forward process and the loopback listener go with them.
    match ending {
        Ending::Child(status) if status.success() => Ok(()),
        Ending::Child(status) => anyhow::bail!("wrapped command exited with {status}"),
        Ending::Refresh(detail) => {
            let _ = child.start_kill();
            let _ = child.wait().await;
            anyhow::bail!(
                "the wrapped command was stopped because its token could not be renewed: {detail}"
            )
        }
        Ending::Interrupted => {
            let _ = child.start_kill();
            let _ = child.wait().await;
            anyhow::bail!("interrupted")
        }
    }
}

/// The proxy's fatal-error arm of `connect`'s `select!`. Without a proxy there
/// is nothing to wait for, and a future that never resolves is the honest way
/// to say so — `select!` then simply has one fewer way to finish.
async fn next_fatal(proxy: Option<&mut service_auth::k8s::LoopbackProxy>) -> String {
    match proxy {
        Some(proxy) => match proxy.next_fatal().await {
            Some(error) => error.to_string(),
            None => std::future::pending().await,
        },
        None => std::future::pending().await,
    }
}
