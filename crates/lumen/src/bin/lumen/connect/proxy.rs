//! The loopback proxy `lumen connect` stands up: the serving TLS trust it
//! probes and the token it mints for the wrapped command.

use anyhow::Result;

use crate::cli::client::ConnectArgs;

#[cfg(feature = "delegated-auth")]
use std::time::Duration;

#[cfg(feature = "delegated-auth")]
use anyhow::Context;

/// Mint a token for `client_sa` and stand up the loopback proxy that lends it
/// to the wrapped command (#2878, R1/R2/R4).
///
/// The first mint happens here rather than lazily on the first proxied
/// request, so `lumen connect --client-sa` fails immediately and legibly when
/// the caller lacks the grant.
#[cfg(feature = "delegated-auth")]
pub(super) async fn start_client_proxy(
    args: &ConnectArgs,
    client_sa: &str,
    upstream: &str,
    trust: Option<ServingTrust>,
) -> Result<service_auth::k8s::LoopbackProxy> {
    let tokens = client_token_source(args.context.as_deref(), &args.namespace, client_sa).await?;
    first_mint(&tokens, &args.namespace, client_sa).await?;
    match trust {
        Some(client) => {
            service_auth::k8s::LoopbackProxy::start_with_client(upstream, tokens, client).await
        }
        None => service_auth::k8s::LoopbackProxy::start(upstream, tokens).await,
    }
    .context("start the local authenticated proxy")
}

/// The verifying client, where there is one to have.
///
/// `--ca-file` requires `--client-sa`, and `--client-sa` requires the
/// `delegated-auth` feature, so a build without it cannot reach a state where
/// this holds anything — which is why the placeholder is a unit rather than a
/// second implementation to keep in step.
#[cfg(feature = "delegated-auth")]
type ServingTrust = reqwest::Client;
#[cfg(not(feature = "delegated-auth"))]
type ServingTrust = ();

/// The client the proxy forwards through when the fleet serves TLS (#3113 R6).
///
/// Two things are deliberately absent. There is no option to skip verification:
/// the failure this command is most likely to hit is a *correct* refusal, and a
/// flag that turned it off would turn the private trust domain into decoration.
/// And the public root store is not merely augmented but switched off — with it
/// on, any public CA could still vouch for this name.
#[cfg(feature = "delegated-auth")]
pub(super) fn serving_trust(
    ca_file: &std::path::Path,
    server_name: &str,
    local_port: u16,
) -> Result<ServingTrust> {
    let pem = std::fs::read_to_string(ca_file).with_context(|| {
        format!(
            "read the serving trust bundle {}. Obtain the public CA separately \
             from the deployment administrator or external certificate platform",
            ca_file.display()
        )
    })?;
    let addr = std::net::SocketAddr::from(([127, 0, 0, 1], local_port));
    service_auth::k8s::verifying_client(&pem, server_name, addr).with_context(|| {
        format!(
            "build a client that verifies {server_name} against {}",
            ca_file.display()
        )
    })
}

/// Reach the far end once, and turn whatever went wrong into something the
/// caller can act on (#3113 R7).
///
/// Each branch names a different deployment fact, because they have different
/// fixes and a single "handshake failed" would send a caller looking in the
/// wrong place. None of them offers to stop verifying, and none of them
/// suggests a certificate for `localhost` — that leaf would be valid against
/// every port-forward anyone opens, which is the opposite of what naming the
/// Service buys.
#[cfg(feature = "delegated-auth")]
pub(super) async fn probe_serving_tls(
    client: &ServingTrust,
    server_name: &str,
    ca_file: Option<&std::path::Path>,
) -> Result<()> {
    let ca = ca_file
        .map(|path| path.display().to_string())
        .unwrap_or_else(|| "<none>".to_string());
    let error = match client
        .get(format!("https://{server_name}/healthz"))
        .timeout(Duration::from_secs(15))
        .send()
        .await
    {
        // Any HTTP answer means the handshake completed, which is all this
        // probe is about. `/healthz` needs no credential, so a status other
        // than 200 is the fleet's business and not this command's.
        Ok(_) => return Ok(()),
        Err(error) => error,
    };

    // The useful text is in the source chain — rustls' rejection reason is
    // several layers below reqwest's "error sending request".
    let mut detail = error.to_string();
    let mut source: Option<&(dyn std::error::Error + 'static)> = std::error::Error::source(&error);
    while let Some(inner) = source {
        detail = format!("{detail}: {inner}");
        source = inner.source();
    }
    let lowered = detail.to_ascii_lowercase();

    let diagnosis = if lowered.contains("unknownissuer") || lowered.contains("unknown issuer") {
        format!(
            "the certificate {server_name} presented was not signed by anything in {ca}. Either \
             the bundle is from another cluster or CA pool, or the fleet's leaf was issued \
             outside it — obtain the public CA separately from the deployment administrator"
        )
    } else if lowered.contains("notvalidforname") || lowered.contains("not valid for name") {
        format!(
            "the far end holds a certificate that does not name {server_name}. Pass \
             `--server-name` with one of the names it does hold (the operator requests \
             `<service>.<namespace>.svc` and its cluster FQDN), or check that `--service`/`--cr` \
             names the Service you meant"
        )
    } else if lowered.contains("expired") {
        format!(
            "the certificate {server_name} presented has expired. Ask the deployment administrator \
             or external certificate platform to rotate the externally provisioned serving Secret \
             and distribute its public CA; keep verification enabled and do not downgrade to plaintext"
        )
    } else if lowered.contains("corrupt message")
        || lowered.contains("handshake eof")
        || lowered.contains("unexpected eof")
        || lowered.contains("http instead of https")
    {
        // The far end read a ClientHello, made nothing of it, and hung up.
        // That is overwhelmingly one thing: a port serving cleartext.
        format!(
            "svc/{server_name} did not complete a TLS handshake and closed the connection, which \
             is what a port still answering in cleartext does. Use `--plaintext` if that is a \
             development instance; in production, have the deployment administrator or external \
             certificate platform provision the serving TLS Secret and set `spec.servingTlsSecret`"
        )
    } else {
        format!("could not complete a TLS handshake with {server_name}: {detail}")
    };

    anyhow::bail!(
        "{diagnosis}.\n\
         The port-forward is only transport: the socket is on 127.0.0.1, but the identity being \
         verified is the Kubernetes Service's, and that is the one the certificate names. There \
         is no option to skip that check."
    )
}

/// Without `delegated-auth` there is no token to carry and so no proxy to
/// verify through. Refusing here keeps the flag from looking like it worked.
#[cfg(not(feature = "delegated-auth"))]
pub(super) fn serving_trust(
    _ca_file: &std::path::Path,
    _server_name: &str,
    _local_port: u16,
) -> Result<ServingTrust> {
    anyhow::bail!(
        "--ca-file needs the `delegated-auth` feature (rebuild with \
         `cargo build -p lumen --features delegated-auth`); this binary has no client that can \
         verify a private serving certificate"
    )
}

/// Unreachable: the only producer of a [`ServingTrust`] in this build fails.
#[cfg(not(feature = "delegated-auth"))]
pub(super) async fn probe_serving_tls(
    _client: &ServingTrust,
    _server_name: &str,
    _ca_file: Option<&std::path::Path>,
) -> Result<()> {
    Ok(())
}

/// The first mint, with Lumen's own remediation attached (#2878, R6).
///
/// `service-auth` already names the caller, the ServiceAccount, and the
/// `kubectl auth can-i` question — everything a provider-neutral library can
/// know. What it cannot know is that this repository ships a command that
/// writes the missing grant, so the CLI adds that here rather than teaching the
/// library about Lumen.
#[cfg(feature = "delegated-auth")]
pub(crate) async fn first_mint(
    tokens: &service_auth::k8s::TokenSource,
    namespace: &str,
    client_sa: &str,
) -> Result<service_auth::k8s::ProjectedToken> {
    match tokens.token().await {
        Ok(token) => Ok(token),
        Err(error @ service_auth::k8s::TokenRequestError::Forbidden { .. }) => {
            anyhow::bail!(
                "{error}. `lumen k8s access render --namespace {namespace} --client-sa \
                 {client_sa} --issuer <you>` emits exactly that grant"
            )
        }
        Err(other) => Err(other.into()),
    }
}

/// Same signature, no minter: a build without `delegated-auth` has no
/// TokenRequest client linked in, and pretending otherwise would mean either
/// running unauthenticated behind the caller's back or inventing a credential.
/// Naming the missing feature is the only honest answer.
#[cfg(not(feature = "delegated-auth"))]
pub(super) async fn start_client_proxy(
    _args: &ConnectArgs,
    _client_sa: &str,
    _upstream: &str,
    _trust: Option<ServingTrust>,
) -> Result<service_auth::k8s::LoopbackProxy> {
    anyhow::bail!(
        "--client-sa needs the `delegated-auth` feature (rebuild with \
         `cargo build -p lumen --features delegated-auth`); this binary cannot mint a \
         ServiceAccount token"
    )
}

/// One short-lived audience-bound token, minted for an explicitly named
/// ServiceAccount through whatever identity the kubeconfig already holds
/// (#2878, R1/R3).
///
/// The audience is `lumen`'s own, not a parameter: a token a serving node will
/// accept is exactly a token minted for that audience, and letting a flag
/// choose otherwise would only produce credentials that fail at the far end.
#[cfg(feature = "delegated-auth")]
pub(crate) async fn client_token_source(
    context: Option<&str>,
    namespace: &str,
    client_sa: &str,
) -> Result<std::sync::Arc<service_auth::k8s::TokenSource>> {
    let target =
        service_auth::k8s::TokenRequestTarget::new(namespace, client_sa, lumen::auth::AUDIENCE)?;
    let minter = service_auth::k8s::KubeTokenMinter::from_context(context).await?;
    Ok(std::sync::Arc::new(service_auth::k8s::TokenSource::new(
        std::sync::Arc::new(minter),
        target,
    )))
}
