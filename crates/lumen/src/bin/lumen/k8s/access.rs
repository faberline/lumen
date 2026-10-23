//! `lumen k8s access render`: the client access handoff, as five objects and no
//! credential.

use anyhow::Result;

use crate::cli::k8s::K8sAccessRenderArgs;

#[cfg(feature = "operator")]
use anyhow::Context;

/// `lumen k8s access render` (#2889) — the client access handoff, as five
/// objects and no credential.
///
/// The boundary this renders has two hops, and the reason to render it rather
/// than describe it in a runbook is that the two are easy to collapse into
/// one:
///
/// 1. a human account or a Google service account authenticates to
///    *kube-apiserver* through its kubeconfig credential plugin, and
///    Kubernetes RBAC decides whether that principal may create a TokenRequest
///    for one named ServiceAccount;
/// 2. the short-lived, audience-bound token that comes back is the only
///    credential Lumen ever sees, and Lumen asks Kubernetes RBAC what *that
///    ServiceAccount* may do.
///
/// Binding the Google principal straight to the Lumen role authorizes the same
/// human and looks like it worked — right until the first request, which
/// arrives carrying a ServiceAccount token nobody granted anything to. The two
/// RoleBindings here take deliberately different subject kinds so that
/// shortcut is not expressible.
///
/// `serviceaccounts/token` is the namespace's privilege-escalation surface:
/// `create` on it without `resourceNames` mints a token for *every*
/// ServiceAccount in the namespace, the operator's included. So the issuer
/// Role always names its ServiceAccount, and the bundle is scanned with
/// [`service_k8s::render::rbac::first_wildcard`] before it is emitted — a
/// wildcard that reached any field of any object is a bug in this function,
/// and the render fails rather than hands one out.
#[cfg(feature = "operator")]
pub(super) fn render_access_yaml(args: &K8sAccessRenderArgs) -> Result<String> {
    use service_k8s::render::rbac::{
        first_wildcard, role, role_binding, NamedRule, Role, RoleBinding, RoleSubject,
        ServiceAccountSubject,
    };

    let namespace = object_name("--namespace", &args.namespace)?;
    let client = object_name("--client-sa", &args.client_sa)?;
    let issuers = parse_issuers(&args.issuers)?;
    let grants = parse_grants(&args.grants)?;
    if grants.is_empty() && !args.instance_admin {
        anyhow::bail!(
            "nothing would be granted: pass at least one `--grant \
             <collection-id>=read|write|admin` or `--instance-admin`"
        );
    }

    let issuer_role_name = format!("{client}-token-issuer");
    let lumen_role_name = format!("{client}-lumen-access");
    let labels = serde_json::json!({
        "app.kubernetes.io/name": "lumen",
        "app.kubernetes.io/instance": client,
        "app.kubernetes.io/component": "access",
        "app.kubernetes.io/managed-by": "lumen-cli",
        "app.kubernetes.io/part-of": "lumen",
    });

    // Hop 1: who may mint this ServiceAccount's token.
    let issuer_rules = [NamedRule {
        api_groups: &[""],
        resources: &["serviceaccounts/token"],
        resource_names: &[client],
        verbs: &["create"],
    }];
    let issuer_subjects: Vec<RoleSubject<'_>> =
        issuers.iter().copied().map(RoleSubject::User).collect();

    // Hop 2: what that ServiceAccount may do, in Lumen's own vocabulary. The
    // resource and verb names come from `lumen::auth`, which is what the
    // serving side puts in its SubjectAccessReview — one definition, so a
    // rendered grant cannot describe a check Lumen does not make.
    let api_groups = [lumen::auth::API_GROUP];
    let collections = [lumen::auth::COLLECTIONS_RESOURCE];
    let admin = [lumen::auth::ADMIN_RESOURCE];
    let admin_verbs = [lumen::auth::verb(service_auth::role_map::Role::Admin)];
    let grant_names: Vec<[&str; 1]> = grants
        .iter()
        .map(|grant| [grant.collection.as_str()])
        .collect();
    let mut lumen_rules: Vec<NamedRule<'_>> = grants
        .iter()
        .zip(&grant_names)
        .map(|(grant, names)| NamedRule {
            api_groups: &api_groups,
            resources: &collections,
            resource_names: names,
            verbs: &grant.verbs,
        })
        .collect();
    if args.instance_admin {
        lumen_rules.push(NamedRule {
            api_groups: &api_groups,
            resources: &admin,
            // The admin surface is one namespace-wide object, not a set of
            // named ones — `AuthTarget::Admin` sends no resource name — so
            // there is nothing to enumerate here.
            resource_names: &[],
            // And it is checked at exactly one role: every `ensure_admin` call
            // site asks for `Role::Admin`. Granting the lower verbs too would
            // widen the grant past anything Lumen can ask for.
            verbs: &admin_verbs,
        });
    }

    let documents = vec![
        serde_json::json!({
            "apiVersion": "v1",
            "kind": "ServiceAccount",
            "metadata": { "name": client, "namespace": namespace, "labels": labels },
        }),
        role(Role {
            name: &issuer_role_name,
            namespace,
            labels: labels.clone(),
            rules: &issuer_rules,
        }),
        role_binding(RoleBinding {
            name: &issuer_role_name,
            namespace,
            labels: labels.clone(),
            role: &issuer_role_name,
            subjects: &issuer_subjects,
        }),
        role(Role {
            name: &lumen_role_name,
            namespace,
            labels: labels.clone(),
            rules: &lumen_rules,
        }),
        role_binding(RoleBinding {
            name: &lumen_role_name,
            namespace,
            labels,
            role: &lumen_role_name,
            subjects: &[RoleSubject::ServiceAccount(ServiceAccountSubject {
                namespace,
                name: client,
            })],
        }),
    ];

    let mut out = String::from(ACCESS_BUNDLE_HEADER);
    for (index, document) in documents.iter().enumerate() {
        if let Some(field) = first_wildcard(document) {
            anyhow::bail!(
                "refusing to render a wildcard RBAC grant: `{}` in the {} object",
                field,
                document["kind"].as_str().unwrap_or("rendered")
            );
        }
        if index > 0 {
            out.push_str("---\n");
        }
        out.push_str(&serde_yaml::to_string(document).context("render access bundle YAML")?);
    }
    Ok(cli_std::artifact::ensure_trailing_newline(&out))
}

/// Leads the rendered bundle so the object list is readable without the
/// issue that produced it. Comments, not a wrapper: the body stays raw
/// multi-document YAML that `kubectl apply -f -` accepts unchanged.
#[cfg(feature = "operator")]
const ACCESS_BUNDLE_HEADER: &str = "\
# Lumen client access (#2889). Two hops, five objects, no credential.
#
#   1. `<client-sa>-token-issuer` lets the named Kubernetes users create a
#      TokenRequest for exactly one ServiceAccount. Those users authenticate
#      to kube-apiserver, never to Lumen.
#   2. `<client-sa>-lumen-access` is what Lumen's SubjectAccessReview reads
#      once that ServiceAccount's token arrives.
#
# Mint a token with:
#   kubectl create token <client-sa> -n <namespace> \\
#     --audience lumen.axiom.dev --duration 10m
";

#[cfg(not(feature = "operator"))]
pub(super) fn render_access_yaml(_args: &K8sAccessRenderArgs) -> Result<String> {
    anyhow::bail!(
        "this lumen build was compiled without operator support; rebuild with \
         `--features operator` (the published binary and image include it)"
    )
}

/// A Kubernetes object name (RFC 1123 label), checked here rather than left to
/// `kubectl apply`. A rejected name is a CLI error; an accepted-but-wrong one
/// becomes a `resourceNames` entry that matches nothing, which RBAC reports as
/// an ordinary denial with no hint that the grant was misspelled.
#[cfg(feature = "operator")]
fn object_name<'a>(flag: &str, value: &'a str) -> Result<&'a str> {
    let shaped = !value.is_empty()
        && value.len() <= 63
        && value
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
        && !value.starts_with('-')
        && !value.ends_with('-');
    if !shaped {
        anyhow::bail!(
            "{flag} must be a DNS-1123 label — 1-63 lowercase letters, digits, \
             or `-`, not starting or ending with `-` — got `{value}`"
        );
    }
    Ok(value)
}

/// One collection's grant: the id RBAC will match on, and the verbs it earns.
#[cfg(feature = "operator")]
struct CollectionGrant {
    collection: String,
    verbs: Vec<&'static str>,
}

/// Parse `--grant <collection-id>=read|write|admin`, rejecting duplicates.
///
/// Two `--grant` flags for one collection would render two rules that RBAC
/// unions, so the narrower one is silently irrelevant — exactly the case where
/// a deployer believes they tightened a grant and did not.
#[cfg(feature = "operator")]
fn parse_grants(specs: &[String]) -> Result<Vec<CollectionGrant>> {
    let mut grants: Vec<CollectionGrant> = Vec::new();
    for spec in specs {
        let (collection, level) = spec.split_once('=').ok_or_else(|| {
            anyhow::anyhow!("--grant expects `<collection-id>=read|write|admin`, got `{spec}`")
        })?;
        if collection.is_empty()
            || collection.len() > 253
            || collection
                .chars()
                .any(|c| c.is_whitespace() || c.is_control() || c == '*')
        {
            anyhow::bail!(
                "--grant collection id must be 1-253 characters with no whitespace \
                 and no `*` — got `{collection}` in `{spec}`"
            );
        }
        let level = match level {
            "read" => service_auth::role_map::Role::Read,
            "write" => service_auth::role_map::Role::Write,
            "admin" => service_auth::role_map::Role::Admin,
            other => anyhow::bail!(
                "--grant level must be `read`, `write`, or `admin` — got `{other}` in `{spec}`"
            ),
        };
        if grants.iter().any(|grant| grant.collection == collection) {
            anyhow::bail!(
                "--grant names `{collection}` twice; RBAC unions the rules, so the \
                 narrower grant would have no effect"
            );
        }
        grants.push(CollectionGrant {
            collection: collection.to_string(),
            verbs: granted_verbs(level),
        });
    }
    Ok(grants)
}

/// Every verb Lumen can ask for at or below `level`.
///
/// The role-to-verb mapping is one-to-one (`read` -> `get`, `write` ->
/// `update`, `admin` -> `delete`) and lives in `lumen::auth`; the *grant* is
/// cumulative because `Role::covers` is — a writer that could not read the
/// collection it writes would be denied by the same check that lets it write.
#[cfg(feature = "operator")]
fn granted_verbs(level: service_auth::role_map::Role) -> Vec<&'static str> {
    use service_auth::role_map::Role;
    [Role::Read, Role::Write, Role::Admin]
        .into_iter()
        .filter(|needed| level.covers(*needed))
        .map(lumen::auth::verb)
        .collect()
}

/// Validate `--issuer` names without interpreting them.
///
/// A Kubernetes username is whatever the API server's authenticator produced:
/// a Google address, an OIDC subject, a certificate CN. Parsing it here would
/// invent a distinction the authorizer does not make, so the only rejections
/// are the ones that would produce a binding matching the wrong set of people
/// — an empty name, a wildcard, stray control characters, or surrounding
/// whitespace that YAML would keep and the API server would not.
#[cfg(feature = "operator")]
fn parse_issuers(issuers: &[String]) -> Result<Vec<&str>> {
    let mut names: Vec<&str> = Vec::new();
    for issuer in issuers {
        if issuer.is_empty()
            || issuer.contains('*')
            || issuer.chars().any(char::is_control)
            || issuer.trim() != issuer
        {
            anyhow::bail!(
                "--issuer must be the username `kubectl auth whoami` prints, with no \
                 `*`, no control characters, and no surrounding whitespace — got `{issuer}`"
            );
        }
        if names.contains(&issuer.as_str()) {
            anyhow::bail!("--issuer names `{issuer}` twice");
        }
        names.push(issuer);
    }
    Ok(names)
}
