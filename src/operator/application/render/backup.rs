//! The scheduled backup runner: its ServiceAccount and optional CronJob (#808).

use serde_json::{json, Value};
use service_k8s::render::{self, RenderCtx};

use crate::operator::application::render::identity::control_plane_token;
use crate::operator::application::render::{BACKUP_COMPONENT, CLIENT_PORT};
use crate::operator::domain::lumen_spec::serving::AuthMode;
use crate::operator::domain::lumen_spec::Lumen;

/// A stable, per-instance identity for scheduled backup jobs.
///
/// It is rendered even when no backup schedule is currently configured. That
/// keeps its lifecycle declarative across policy toggles and gives platform
/// automation a stable cloud-neutral target for Workload Identity annotations.
/// Like every other child, it is owned by the `Lumen` CR and is garbage
/// collected with the instance.
pub(super) fn backup_service_account(cx: &RenderCtx<'_>) -> Value {
    let name = format!("{}-backup", cx.name);
    json!({
        "apiVersion": "v1",
        "kind": "ServiceAccount",
        "metadata": cx.meta(&name, BACKUP_COMPONENT),
    })
}

/// The optional backup CronJob (#808): rendered only when
/// `spec.serving.backup` is set. Lumen already produces a consistent
/// point-in-time snapshot over HTTP (`GET /admin/backup`, see
/// `src/persistence/interfaces/http/backup.rs`); this CronJob adds nothing new to the
/// WAL/snapshot path, it only *schedules and transports* that existing
/// endpoint's bytes to a destination via `lumen backup`
/// (`libs/service-backup`). The shared [`service_k8s::render::cron_job`] helper
/// stays manifest-only.
pub(super) fn backup_cron_job(lumen: &Lumen, cx: &RenderCtx<'_>) -> Option<Value> {
    let policy = lumen.spec.serving.backup.as_ref()?;
    let cron_name = format!("{}-backup", cx.name);
    // Cluster-DNS FQDN of the serving ClusterIP Service (`serving_service`),
    // reachable from any namespace's CronJob pod regardless of the operator's
    // own DNS search suffix.
    let url = format!(
        "http://{}.{}.svc.cluster.local:{CLIENT_PORT}",
        cx.name, cx.ns
    );
    let mut args = vec![
        "backup".to_string(),
        "--url".to_string(),
        url,
        "--dest".to_string(),
        policy.destination.clone(),
    ];
    if let Some(secs) = policy.retention_secs {
        args.push("--retention-secs".to_string());
        args.push(secs.to_string());
    }
    // The runner's own credential (#2877): a token minted for the backup
    // ServiceAccount, bound to Lumen's audience, expiring in ten minutes, and
    // rotated in place by the kubelet. The projection itself is
    // unconditional — one pod shape whatever `spec.auth` says — because a
    // mounted file nobody reads costs nothing, while a manifest that changes
    // shape with an auth toggle is a second thing to get wrong.
    //
    // Presenting it is conditional. A fleet with `auth: disabled` rejects a
    // *presented* bearer (#2871), so the flag that makes the runner read the
    // file only appears when the fleet actually requires an identity.
    //
    // What travels on the CronJob is the path, not the token: the material
    // never appears in the pod spec, in `kubectl describe`, or in whatever
    // pipeline ships that manifest to the cluster.
    let projection = control_plane_token();
    if matches!(lumen.spec.auth, AuthMode::Required) {
        args.push("--token-file".to_string());
        args.push(projection.file_path());
    }
    let env: Vec<serde_json::Value> = Vec::new();
    let image_pull_policy = lumen
        .spec
        .image_pull_policy
        .clone()
        .unwrap_or_else(|| "IfNotPresent".to_string());
    Some(render::cron_job(render::CronJob {
        cx,
        name: &cron_name,
        component: BACKUP_COMPONENT,
        schedule: &policy.schedule,
        image: lumen.spec.image.as_str(),
        image_pull_policy: &image_pull_policy,
        command: vec!["lumen".into()],
        args,
        env,
        env_from: vec![],
        volumes: vec![projection.volume()],
        volume_mounts: vec![projection.mount()],
        service_account_name: Some(&cron_name),
        cpu: "100m",
        memory: "128Mi",
        successful_jobs_history_limit: 3,
        failed_jobs_history_limit: 3,
    }))
}
