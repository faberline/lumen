//! Admission checks on a `LumenSpec`, and the topology facts they rest on.

use crate::domain::lumen_spec::{LumenSpec, MAX_BODY_LIMIT_BYTES, MIN_BODY_LIMIT_BYTES};

impl LumenSpec {
    /// Cross-field invariants the structural schema cannot express (#2678 R7,
    /// #2764).
    ///
    /// It carries no rule today. The one it used to carry — identity grants
    /// with no audience — described a verifier this operator no longer
    /// configures: authentication is the cluster's TokenReview, and an audience
    /// is no longer a field an author can leave empty (#2872). The hook stays
    /// because it is the only place on the reconcile path that can refuse a
    /// spec, and because [`crate::fleet`] runs it over the specs it
    /// composes — a rule added here holds for both, and a rule added anywhere
    /// else would not.
    pub fn validate(&self) -> Result<(), String> {
        if !crate::domain::capacity::is_valid_direct_gce_machine_type(
            &self.placement.initial_machine_type,
        ) {
            return Err(format!(
                "initialMachineType ({}) is not an allowed direct GCE machine type; service-tier names are forbidden",
                self.placement.initial_machine_type
            ));
        }
        if let Some(limit) = self.body_limit_bytes {
            if !(MIN_BODY_LIMIT_BYTES..=MAX_BODY_LIMIT_BYTES).contains(&limit) {
                return Err(format!(
                    "bodyLimitBytes ({limit}) must be between {MIN_BODY_LIMIT_BYTES} (1 MiB) and {MAX_BODY_LIMIT_BYTES} (64 MiB) inclusive"
                ));
            }
        }
        let mut seen = std::collections::BTreeSet::new();
        for (i, entry) in self.extra_env.iter().enumerate() {
            let name = entry.name.as_str();
            let sources = [
                entry.value.is_some(),
                entry.secret_key_ref.is_some(),
                entry.config_map_key_ref.is_some(),
            ]
            .into_iter()
            .filter(|set| *set)
            .count();
            if sources != 1 {
                return Err(format!(
                    "extraEnv[{i}] ({name}) must set exactly one of value, secretKeyRef, configMapKeyRef"
                ));
            }
            if !is_valid_env_name(name) {
                return Err(format!(
                    "extraEnv[{i}] name ({name}) is not a valid environment variable name"
                ));
            }
            for key_ref in [&entry.secret_key_ref, &entry.config_map_key_ref]
                .into_iter()
                .flatten()
            {
                if key_ref.name.is_empty() || key_ref.key.is_empty() {
                    return Err(format!(
                        "extraEnv[{i}] ({name}) secretKeyRef/configMapKeyRef needs a non-empty name and key"
                    ));
                }
            }
            if is_reserved_env_name(name) {
                return Err(format!(
                    "extraEnv[{i}] ({name}) is an environment variable the operator owns"
                ));
            }
            if !seen.insert(name) {
                return Err(format!("extraEnv has duplicate name ({name})"));
            }
        }
        for (i, arg) in self.extra_args.iter().enumerate() {
            if !arg.starts_with("--") {
                return Err(format!("extraArgs[{i}] must be a flag starting with --"));
            }
            if arg.chars().any(char::is_whitespace) {
                return Err(format!("extraArgs[{i}] must not contain whitespace"));
            }
        }
        Ok(())
    }

    /// Does this instance run a replicated Raft group, and therefore owe an
    /// instance-scoped peer identity (#2890)?
    ///
    /// Not a `validate()` rule on purpose: refusing the spec would fail the
    /// reconcile outright, and a failed reconcile writes no status. An operator
    /// whose replicated instance is missing its Secret needs to be *told* which
    /// Secret, which is a `PeerIdentityReady=False` condition — so the check
    /// lives on the status path instead (see [`super::reconcile`]).
    ///
    /// [`super::reconcile`]: crate::application::reconcile
    pub fn peer_identity_required(&self) -> bool {
        self.replicas_per_shard > 1
    }

    pub fn storage_pod_count(&self) -> i32 {
        if self.replicas_per_shard > 1 {
            (self.shard_count * self.replicas_per_shard) as i32
        } else if self.shard_count > 1 {
            self.shard_count as i32
        } else {
            // Single shard, single member, no raft consensus (#1317): every
            // pod's `shard_index` (`ordinal % shard_count`) collapses to 0,
            // so more than one live pod here means multiple uncoordinated
            // local copies behind one Service — confirmed empirically on a
            // kind cluster (a write via one pod is invisible on the others;
            // a load-balanced Service returns divergent results for the
            // same read). Clamp to exactly 1; multi-replica scaling requires
            // opting into `replicasPerShard > 1` (raft-HA).
            1
        }
    }
}

/// Is `name` an environment variable the operator owns? The operator writes
/// these names itself, so an extra entry must not reuse them.
pub fn is_reserved_env_name(name: &str) -> bool {
    const PREFIXES: [&str; 5] = ["LUMEN_", "POD_", "RAFT_", "SHARD_", "VIRTUAL_"];
    const NAMES: [&str; 2] = ["REPLICAS_PER_SHARD", "VOTER_COUNT"];
    PREFIXES.iter().any(|prefix| name.starts_with(prefix)) || NAMES.contains(&name)
}

fn is_valid_env_name(name: &str) -> bool {
    !name.is_empty()
        && !name.starts_with(|c: char| c.is_ascii_digit())
        && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
}
