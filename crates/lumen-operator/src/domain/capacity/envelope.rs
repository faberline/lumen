//! Pure Kubernetes-native resource envelope definition, quantity parsing,
//! fixed documented bounds validation, and allocatable placement fitting.
//!
//! A [`ResourceEnvelope`] models the pure compute and storage requests of a
//! Lumen instance without depending on direct GCE machine types, platform
//! catalog entries, or cloud provider node pools.

use serde::{Deserialize, Serialize};

use crate::domain::capacity::placement::{derive_requests, CapacityVector};
use crate::domain::capacity::{Rejection, RejectionReason};

/// Minimum CPU request allowed for a Lumen instance in millicores (500m / 0.5 vCPU).
pub const MIN_CPU_MILLICORES: u64 = 500;

/// Minimum memory request allowed for a Lumen instance in MiB (512Mi).
pub const MIN_MEMORY_MIB: u64 = 512;

/// Minimum persistent data volume size allowed for a Lumen instance in bytes (1 GiB).
pub const MIN_STORAGE_BYTES: u64 = 1024 * 1024 * 1024;

/// Alias for [`MIN_STORAGE_BYTES`].
pub const MIN_DATA_STORAGE_BYTES: u64 = MIN_STORAGE_BYTES;

/// Pure resource envelope defining compute and storage requests for a Lumen instance.
///
/// Contains CPU in millicores, memory in MiB, data storage in bytes, and an optional
/// storage class name. Has NO GCE machine type, node pool name, or catalog field.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct ResourceEnvelope {
    pub cpu_millicores: u64,
    pub memory_mib: u64,
    pub storage_bytes: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage_class: Option<String>,
}

impl ResourceEnvelope {
    /// Construct a new [`ResourceEnvelope`].
    pub fn new(
        cpu_millicores: u64,
        memory_mib: u64,
        storage_bytes: u64,
        storage_class: Option<String>,
    ) -> Self {
        Self {
            cpu_millicores,
            memory_mib,
            storage_bytes,
            storage_class,
        }
    }

    /// Parse Kubernetes quantity strings into a [`ResourceEnvelope`].
    pub fn parse(
        cpu: &str,
        memory: &str,
        storage: &str,
        storage_class: Option<&str>,
    ) -> Result<Self, Rejection> {
        parse_resource_envelope(cpu, memory, storage, storage_class)
    }

    /// Check this envelope against fixed documented minimum bounds.
    pub fn check_bounds(&self) -> Result<(), Rejection> {
        check_envelope_bounds(self)
    }

    /// Check whether this envelope fits a given [`CapacityVector`] allocatable
    /// capacity after reserves and headroom, by reusing [`derive_requests`].
    pub fn fits_allocatable(
        &self,
        allocatable: CapacityVector,
        reserves: CapacityVector,
        headroom: CapacityVector,
    ) -> Result<(), Rejection> {
        fits_allocatable(self, allocatable, reserves, headroom)
    }

    /// Convenience getter for data storage in bytes.
    pub fn data_storage_bytes(&self) -> u64 {
        self.storage_bytes
    }
}

/// Parse a Kubernetes CPU quantity string (e.g. "500m", "2") into millicores.
pub fn parse_cpu_quantity(s: &str, field_path: &str) -> Result<u64, Rejection> {
    let trimmed = s.trim();
    if trimmed.is_empty() {
        return Err(Rejection {
            reason: RejectionReason::InvalidEnvelope,
            field_path: field_path.to_string(),
            message: "CPU quantity must not be empty".to_string(),
        });
    }
    if trimmed.starts_with('-') {
        return Err(Rejection {
            reason: RejectionReason::InvalidEnvelope,
            field_path: field_path.to_string(),
            message: format!("CPU quantity `{s}` must not be negative"),
        });
    }
    if let Some(m_val) = trimmed.strip_suffix('m') {
        if m_val.is_empty() || !m_val.chars().all(|c| c.is_ascii_digit()) {
            return Err(Rejection {
                reason: RejectionReason::InvalidEnvelope,
                field_path: field_path.to_string(),
                message: format!("invalid CPU quantity format `{s}`"),
            });
        }
        let val: u64 = m_val.parse().map_err(|_| Rejection {
            reason: RejectionReason::InvalidEnvelope,
            field_path: field_path.to_string(),
            message: format!("CPU quantity `{s}` overflows u64"),
        })?;
        if val == 0 {
            return Err(Rejection {
                reason: RejectionReason::InvalidEnvelope,
                field_path: field_path.to_string(),
                message: "CPU quantity must be greater than zero".to_string(),
            });
        }
        Ok(val)
    } else if trimmed.chars().all(|c| c.is_ascii_digit()) {
        let cores: u64 = trimmed.parse().map_err(|_| Rejection {
            reason: RejectionReason::InvalidEnvelope,
            field_path: field_path.to_string(),
            message: format!("CPU quantity `{s}` overflows u64"),
        })?;
        if cores == 0 {
            return Err(Rejection {
                reason: RejectionReason::InvalidEnvelope,
                field_path: field_path.to_string(),
                message: "CPU quantity must be greater than zero".to_string(),
            });
        }
        cores.checked_mul(1000).ok_or_else(|| Rejection {
            reason: RejectionReason::InvalidEnvelope,
            field_path: field_path.to_string(),
            message: format!("CPU quantity `{s}` in millicores overflows u64"),
        })
    } else {
        Err(Rejection {
            reason: RejectionReason::InvalidEnvelope,
            field_path: field_path.to_string(),
            message: format!("unknown suffix or invalid format in CPU quantity `{s}`"),
        })
    }
}

/// Parse a Kubernetes memory quantity string (e.g. "512Mi", "4Gi") into MiB.
pub fn parse_memory_quantity(s: &str, field_path: &str) -> Result<u64, Rejection> {
    let trimmed = s.trim();
    if trimmed.is_empty() {
        return Err(Rejection {
            reason: RejectionReason::InvalidEnvelope,
            field_path: field_path.to_string(),
            message: "memory quantity must not be empty".to_string(),
        });
    }
    if trimmed.starts_with('-') {
        return Err(Rejection {
            reason: RejectionReason::InvalidEnvelope,
            field_path: field_path.to_string(),
            message: format!("memory quantity `{s}` must not be negative"),
        });
    }

    let (num_part, multiplier_mib) = if let Some(p) = trimmed.strip_suffix("Ei") {
        (
            p,
            1024u64
                .checked_mul(1024)
                .and_then(|x| x.checked_mul(1024))
                .and_then(|x| x.checked_mul(1024)),
        )
    } else if let Some(p) = trimmed.strip_suffix("Pi") {
        (p, Some(1024u64 * 1024 * 1024 * 1024))
    } else if let Some(p) = trimmed.strip_suffix("Ti") {
        (p, Some(1024u64 * 1024 * 1024))
    } else if let Some(p) = trimmed.strip_suffix("Gi") {
        (p, Some(1024u64))
    } else if let Some(p) = trimmed.strip_suffix("Mi") {
        (p, Some(1u64))
    } else if let Some(p) = trimmed.strip_suffix("Ki") {
        if p.is_empty() || !p.chars().all(|c| c.is_ascii_digit()) {
            return Err(Rejection {
                reason: RejectionReason::InvalidEnvelope,
                field_path: field_path.to_string(),
                message: format!("invalid memory quantity format `{s}`"),
            });
        }
        let kib: u64 = p.parse().map_err(|_| Rejection {
            reason: RejectionReason::InvalidEnvelope,
            field_path: field_path.to_string(),
            message: format!("memory quantity `{s}` overflows u64"),
        })?;
        if kib == 0 {
            return Err(Rejection {
                reason: RejectionReason::InvalidEnvelope,
                field_path: field_path.to_string(),
                message: "memory quantity must be greater than zero".to_string(),
            });
        }
        let mib = kib / 1024;
        if mib == 0 {
            return Err(Rejection {
                reason: RejectionReason::InvalidEnvelope,
                field_path: field_path.to_string(),
                message: format!("memory quantity `{s}` is below 1 MiB"),
            });
        }
        return Ok(mib);
    } else {
        return Err(Rejection {
            reason: RejectionReason::InvalidEnvelope,
            field_path: field_path.to_string(),
            message: format!("unknown suffix or invalid format in memory quantity `{s}`"),
        });
    };

    if num_part.is_empty() || !num_part.chars().all(|c| c.is_ascii_digit()) {
        return Err(Rejection {
            reason: RejectionReason::InvalidEnvelope,
            field_path: field_path.to_string(),
            message: format!("invalid memory quantity format `{s}`"),
        });
    }

    let val: u64 = num_part.parse().map_err(|_| Rejection {
        reason: RejectionReason::InvalidEnvelope,
        field_path: field_path.to_string(),
        message: format!("memory quantity `{s}` overflows u64"),
    })?;

    if val == 0 {
        return Err(Rejection {
            reason: RejectionReason::InvalidEnvelope,
            field_path: field_path.to_string(),
            message: "memory quantity must be greater than zero".to_string(),
        });
    }

    let mult = multiplier_mib.ok_or_else(|| Rejection {
        reason: RejectionReason::InvalidEnvelope,
        field_path: field_path.to_string(),
        message: format!("memory quantity `{s}` multiplier overflows u64"),
    })?;

    val.checked_mul(mult).ok_or_else(|| Rejection {
        reason: RejectionReason::InvalidEnvelope,
        field_path: field_path.to_string(),
        message: format!("memory quantity `{s}` overflows u64"),
    })
}

/// Parse a Kubernetes data storage quantity string (e.g. "10Gi", "512Mi") into bytes.
pub fn parse_storage_quantity(s: &str, field_path: &str) -> Result<u64, Rejection> {
    let trimmed = s.trim();
    if trimmed.is_empty() {
        return Err(Rejection {
            reason: RejectionReason::InvalidEnvelope,
            field_path: field_path.to_string(),
            message: "storage quantity must not be empty".to_string(),
        });
    }
    if trimmed.starts_with('-') {
        return Err(Rejection {
            reason: RejectionReason::InvalidEnvelope,
            field_path: field_path.to_string(),
            message: format!("storage quantity `{s}` must not be negative"),
        });
    }

    const KIB: u64 = 1024;
    const MIB: u64 = 1024 * 1024;
    const GIB: u64 = 1024 * 1024 * 1024;
    const TIB: u64 = 1024 * 1024 * 1024 * 1024;
    const PIB: u64 = 1024 * 1024 * 1024 * 1024 * 1024;

    let (num_part, multiplier) = if let Some(p) = trimmed.strip_suffix("Pi") {
        (p, Some(PIB))
    } else if let Some(p) = trimmed.strip_suffix("Ti") {
        (p, Some(TIB))
    } else if let Some(p) = trimmed.strip_suffix("Gi") {
        (p, Some(GIB))
    } else if let Some(p) = trimmed.strip_suffix("Mi") {
        (p, Some(MIB))
    } else if let Some(p) = trimmed.strip_suffix("Ki") {
        (p, Some(KIB))
    } else if let Some(p) = trimmed.strip_suffix('P') {
        (p, Some(1_000_000_000_000_000))
    } else if let Some(p) = trimmed.strip_suffix('T') {
        (p, Some(1_000_000_000_000))
    } else if let Some(p) = trimmed.strip_suffix('G') {
        (p, Some(1_000_000_000))
    } else if let Some(p) = trimmed.strip_suffix('M') {
        (p, Some(1_000_000))
    } else if let Some(p) = trimmed.strip_suffix('k') {
        (p, Some(1_000))
    } else if let Some(p) = trimmed.strip_suffix('K') {
        (p, Some(1_000))
    } else if trimmed.chars().all(|c| c.is_ascii_digit()) {
        (trimmed, Some(1))
    } else {
        return Err(Rejection {
            reason: RejectionReason::InvalidEnvelope,
            field_path: field_path.to_string(),
            message: format!("unknown suffix or invalid format in storage quantity `{s}`"),
        });
    };

    if num_part.is_empty() || !num_part.chars().all(|c| c.is_ascii_digit()) {
        return Err(Rejection {
            reason: RejectionReason::InvalidEnvelope,
            field_path: field_path.to_string(),
            message: format!("invalid storage quantity format `{s}`"),
        });
    }

    let val: u64 = num_part.parse().map_err(|_| Rejection {
        reason: RejectionReason::InvalidEnvelope,
        field_path: field_path.to_string(),
        message: format!("storage quantity `{s}` overflows u64"),
    })?;

    if val == 0 {
        return Err(Rejection {
            reason: RejectionReason::InvalidEnvelope,
            field_path: field_path.to_string(),
            message: "storage quantity must be greater than zero".to_string(),
        });
    }

    let mult = multiplier.ok_or_else(|| Rejection {
        reason: RejectionReason::InvalidEnvelope,
        field_path: field_path.to_string(),
        message: format!("storage quantity `{s}` multiplier overflows u64"),
    })?;

    val.checked_mul(mult).ok_or_else(|| Rejection {
        reason: RejectionReason::InvalidEnvelope,
        field_path: field_path.to_string(),
        message: format!("storage quantity `{s}` in bytes overflows u64"),
    })
}

/// Pure function parsing Kubernetes quantity strings into a [`ResourceEnvelope`].
///
/// Rejects zero, negative, empty, unknown-suffix, and overflowing values with a
/// [`Rejection`] having [`RejectionReason::InvalidEnvelope`] and naming the bad field.
pub fn parse_resource_envelope(
    cpu: &str,
    memory: &str,
    storage: &str,
    storage_class: Option<&str>,
) -> Result<ResourceEnvelope, Rejection> {
    let cpu_millicores = parse_cpu_quantity(cpu, "cpu")?;
    let memory_mib = parse_memory_quantity(memory, "memory")?;
    let storage_bytes = parse_storage_quantity(storage, "storage")?;
    Ok(ResourceEnvelope {
        cpu_millicores,
        memory_mib,
        storage_bytes,
        storage_class: storage_class.map(|s| s.to_string()),
    })
}

/// Check an envelope against fixed documented minimum bounds.
///
/// Boundary values pass; one unit below fails.
pub fn check_envelope_bounds(envelope: &ResourceEnvelope) -> Result<(), Rejection> {
    if envelope.cpu_millicores < MIN_CPU_MILLICORES {
        return Err(Rejection {
            reason: RejectionReason::InvalidEnvelope,
            field_path: "cpu_millicores".to_string(),
            message: format!(
                "CPU request {}m is below minimum allowed bound of {}m",
                envelope.cpu_millicores, MIN_CPU_MILLICORES
            ),
        });
    }
    if envelope.memory_mib < MIN_MEMORY_MIB {
        return Err(Rejection {
            reason: RejectionReason::InvalidEnvelope,
            field_path: "memory_mib".to_string(),
            message: format!(
                "memory request {}Mi is below minimum allowed bound of {}Mi",
                envelope.memory_mib, MIN_MEMORY_MIB
            ),
        });
    }
    if envelope.storage_bytes < MIN_STORAGE_BYTES {
        return Err(Rejection {
            reason: RejectionReason::InvalidEnvelope,
            field_path: "storage_bytes".to_string(),
            message: format!(
                "data storage request {} bytes is below minimum allowed bound of {} bytes",
                envelope.storage_bytes, MIN_STORAGE_BYTES
            ),
        });
    }
    Ok(())
}

/// Say whether an envelope fits a given [`CapacityVector`] allocatable capacity
/// after reserves and headroom, by reusing [`derive_requests`] and not copying its math.
pub fn fits_allocatable(
    envelope: &ResourceEnvelope,
    allocatable: CapacityVector,
    reserves: CapacityVector,
    headroom: CapacityVector,
) -> Result<(), Rejection> {
    let available = derive_requests(allocatable, reserves, headroom)?;
    if envelope.cpu_millicores > available.cpu_millicores {
        return Err(Rejection {
            reason: RejectionReason::InsufficientAllocatable,
            field_path: "cpu_millicores".to_string(),
            message: format!(
                "envelope CPU request ({}m) exceeds derived allocatable capacity ({}m)",
                envelope.cpu_millicores, available.cpu_millicores
            ),
        });
    }
    if envelope.memory_mib > available.memory_mib {
        return Err(Rejection {
            reason: RejectionReason::InsufficientAllocatable,
            field_path: "memory_mib".to_string(),
            message: format!(
                "envelope memory request ({}Mi) exceeds derived allocatable capacity ({}Mi)",
                envelope.memory_mib, available.memory_mib
            ),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::capacity::preflight::{
        preflight_capacity, resolve_machine_type, CapacityRequest,
    };
    use crate::domain::capacity::{CapacityCatalog, CapacitySpec, CatalogEntry, RejectionReason};

    #[test]
    fn parse_valid_quantities_into_resource_envelope() {
        let env = parse_resource_envelope("500m", "512Mi", "10Gi", Some("standard-rwo"))
            .expect("parse valid envelope");
        assert_eq!(env.cpu_millicores, 500);
        assert_eq!(env.memory_mib, 512);
        assert_eq!(env.storage_bytes, 10 * 1024 * 1024 * 1024);
        assert_eq!(env.storage_class.as_deref(), Some("standard-rwo"));

        // Whole cores for CPU, Gi for memory, Mi for storage
        let env2 = parse_resource_envelope("2", "4Gi", "512Mi", None)
            .expect("parse second valid envelope");
        assert_eq!(env2.cpu_millicores, 2000);
        assert_eq!(env2.memory_mib, 4096);
        assert_eq!(env2.storage_bytes, 512 * 1024 * 1024);
        assert_eq!(env2.storage_class, None);
    }

    #[test]
    fn parse_rejects_zero_negative_empty_unknown_suffix_and_overflow() {
        // Zero values
        let err = parse_resource_envelope("0m", "512Mi", "10Gi", None).unwrap_err();
        assert_eq!(err.reason, RejectionReason::InvalidEnvelope);
        assert_eq!(err.field_path, "cpu");

        let err = parse_resource_envelope("500m", "0Mi", "10Gi", None).unwrap_err();
        assert_eq!(err.reason, RejectionReason::InvalidEnvelope);
        assert_eq!(err.field_path, "memory");

        let err = parse_resource_envelope("500m", "512Mi", "0Gi", None).unwrap_err();
        assert_eq!(err.reason, RejectionReason::InvalidEnvelope);
        assert_eq!(err.field_path, "storage");

        // Negative values
        let err = parse_resource_envelope("-500m", "512Mi", "10Gi", None).unwrap_err();
        assert_eq!(err.reason, RejectionReason::InvalidEnvelope);
        assert_eq!(err.field_path, "cpu");

        let err = parse_resource_envelope("500m", "-4Gi", "10Gi", None).unwrap_err();
        assert_eq!(err.reason, RejectionReason::InvalidEnvelope);
        assert_eq!(err.field_path, "memory");

        let err = parse_resource_envelope("500m", "512Mi", "-10Gi", None).unwrap_err();
        assert_eq!(err.reason, RejectionReason::InvalidEnvelope);
        assert_eq!(err.field_path, "storage");

        // Empty values
        let err = parse_resource_envelope("", "512Mi", "10Gi", None).unwrap_err();
        assert_eq!(err.reason, RejectionReason::InvalidEnvelope);
        assert_eq!(err.field_path, "cpu");

        let err = parse_resource_envelope("500m", "  ", "10Gi", None).unwrap_err();
        assert_eq!(err.reason, RejectionReason::InvalidEnvelope);
        assert_eq!(err.field_path, "memory");

        let err = parse_resource_envelope("500m", "512Mi", "", None).unwrap_err();
        assert_eq!(err.reason, RejectionReason::InvalidEnvelope);
        assert_eq!(err.field_path, "storage");

        // Unknown suffix
        let err = parse_resource_envelope("500foo", "512Mi", "10Gi", None).unwrap_err();
        assert_eq!(err.reason, RejectionReason::InvalidEnvelope);
        assert_eq!(err.field_path, "cpu");

        let err = parse_resource_envelope("500m", "4GB", "10Gi", None).unwrap_err();
        assert_eq!(err.reason, RejectionReason::InvalidEnvelope);
        assert_eq!(err.field_path, "memory");

        let err = parse_resource_envelope("500m", "512Mi", "10GiB", None).unwrap_err();
        assert_eq!(err.reason, RejectionReason::InvalidEnvelope);
        assert_eq!(err.field_path, "storage");

        // Overflow
        let err = parse_resource_envelope("999999999999999999999999999m", "512Mi", "10Gi", None)
            .unwrap_err();
        assert_eq!(err.reason, RejectionReason::InvalidEnvelope);
        assert_eq!(err.field_path, "cpu");
    }

    #[test]
    fn check_bounds_boundary_values_pass_and_one_unit_below_fails() {
        // Exact boundary values pass
        let boundary = ResourceEnvelope {
            cpu_millicores: MIN_CPU_MILLICORES,
            memory_mib: MIN_MEMORY_MIB,
            storage_bytes: MIN_STORAGE_BYTES,
            storage_class: None,
        };
        assert!(check_envelope_bounds(&boundary).is_ok());

        // CPU: 1 unit below fails
        let mut cpu_below = boundary.clone();
        cpu_below.cpu_millicores = MIN_CPU_MILLICORES - 1;
        let err = check_envelope_bounds(&cpu_below).unwrap_err();
        assert_eq!(err.reason, RejectionReason::InvalidEnvelope);
        assert_eq!(err.field_path, "cpu_millicores");

        // Memory: 1 unit below fails
        let mut mem_below = boundary.clone();
        mem_below.memory_mib = MIN_MEMORY_MIB - 1;
        let err = check_envelope_bounds(&mem_below).unwrap_err();
        assert_eq!(err.reason, RejectionReason::InvalidEnvelope);
        assert_eq!(err.field_path, "memory_mib");

        // Storage: 1 unit below fails
        let mut storage_below = boundary.clone();
        storage_below.storage_bytes = MIN_STORAGE_BYTES - 1;
        let err = check_envelope_bounds(&storage_below).unwrap_err();
        assert_eq!(err.reason, RejectionReason::InvalidEnvelope);
        assert_eq!(err.field_path, "storage_bytes");
    }

    #[test]
    fn fits_allocatable_accepts_equal_and_refuses_larger() {
        let allocatable = CapacityVector {
            cpu_millicores: 4000,
            memory_mib: 8192,
        };
        let reserves = CapacityVector {
            cpu_millicores: 500,
            memory_mib: 1024,
        };
        let headroom = CapacityVector {
            cpu_millicores: 500,
            memory_mib: 1024,
        };
        // derive_requests yields available: 3000m, 6144Mi

        // Equal envelope is accepted
        let equal_env = ResourceEnvelope {
            cpu_millicores: 3000,
            memory_mib: 6144,
            storage_bytes: 10 * 1024 * 1024 * 1024,
            storage_class: None,
        };
        assert!(fits_allocatable(
            &equal_env,
            allocatable.clone(),
            reserves.clone(),
            headroom.clone()
        )
        .is_ok());

        // Smaller envelope is accepted
        let smaller_env = ResourceEnvelope {
            cpu_millicores: 2000,
            memory_mib: 4096,
            storage_bytes: 10 * 1024 * 1024 * 1024,
            storage_class: None,
        };
        assert!(fits_allocatable(
            &smaller_env,
            allocatable.clone(),
            reserves.clone(),
            headroom.clone()
        )
        .is_ok());

        // CPU larger than derived requests is refused
        let cpu_larger = ResourceEnvelope {
            cpu_millicores: 3001,
            memory_mib: 6144,
            storage_bytes: 10 * 1024 * 1024 * 1024,
            storage_class: None,
        };
        let err = fits_allocatable(
            &cpu_larger,
            allocatable.clone(),
            reserves.clone(),
            headroom.clone(),
        )
        .unwrap_err();
        assert_eq!(err.reason, RejectionReason::InsufficientAllocatable);
        assert_eq!(err.field_path, "cpu_millicores");

        // Memory larger than derived requests is refused
        let mem_larger = ResourceEnvelope {
            cpu_millicores: 3000,
            memory_mib: 6145,
            storage_bytes: 10 * 1024 * 1024 * 1024,
            storage_class: None,
        };
        let err = fits_allocatable(
            &mem_larger,
            allocatable.clone(),
            reserves.clone(),
            headroom.clone(),
        )
        .unwrap_err();
        assert_eq!(err.reason, RejectionReason::InsufficientAllocatable);
        assert_eq!(err.field_path, "memory_mib");

        // Insufficient allocatable initially (allocatable <= reserves + headroom)
        let tight_allocatable = CapacityVector {
            cpu_millicores: 900,
            memory_mib: 2000,
        };
        let err =
            fits_allocatable(&smaller_env, tight_allocatable, reserves, headroom).unwrap_err();
        assert_eq!(err.reason, RejectionReason::InsufficientAllocatable);
        assert_eq!(err.field_path, "allocatable");
    }

    #[test]
    fn legacy_catalog_bounds_pinned_for_resolve_and_preflight() {
        let entry = CatalogEntry::new(
            "e2-standard-2",
            "lumen.axiom.dev/capacity-profile",
            "ready",
            10,
        );
        let catalog = CapacityCatalog::new(vec![entry]);

        // 1. Listed type resolves
        let resolved =
            resolve_machine_type("e2-standard-2", &catalog).expect("listed machine type resolves");
        assert_eq!(resolved.machine_type, "e2-standard-2");
        assert_eq!(resolved.selector_key, "lumen.axiom.dev/capacity-profile");
        assert_eq!(resolved.selector_value, "e2-standard-2");
        assert_eq!(resolved.lifecycle_state, "ready");
        assert_eq!(resolved.max_nodes, 10);

        let req = CapacityRequest {
            spec: CapacitySpec {
                initial_machine_type: "e2-standard-2".to_string(),
            },
            old_member_disrupted: false,
        };
        let preflight_ok =
            preflight_capacity(&req, Some(&catalog)).expect("listed machine type preflight ok");
        assert_eq!(preflight_ok.machine_type, "e2-standard-2");

        // 2. Unlisted type is rejected with today's RejectionReason and message text
        let unlisted_err = resolve_machine_type("unlisted-type", &catalog).unwrap_err();
        assert_eq!(unlisted_err.reason, RejectionReason::UnsupportedMachineType);
        assert_eq!(unlisted_err.field_path, "machine_type");
        assert_eq!(
            unlisted_err.message,
            "machine type `unlisted-type` is not present in capacity catalog"
        );

        let unlisted_req = CapacityRequest {
            spec: CapacitySpec {
                initial_machine_type: "unlisted-type".to_string(),
            },
            old_member_disrupted: false,
        };
        let preflight_unlisted_err = preflight_capacity(&unlisted_req, Some(&catalog)).unwrap_err();
        assert_eq!(
            preflight_unlisted_err.reason,
            RejectionReason::UnsupportedMachineType
        );
        assert_eq!(preflight_unlisted_err.field_path, "machine_type");
        assert_eq!(
            preflight_unlisted_err.message,
            "machine type `unlisted-type` not found in catalog"
        );

        // 3. Tier name such as "small" is rejected with today's RejectionReason and message text
        let tier_err = resolve_machine_type("small", &catalog).unwrap_err();
        assert_eq!(tier_err.reason, RejectionReason::UnsupportedMachineType);
        assert_eq!(tier_err.field_path, "machine_type");
        assert_eq!(
            tier_err.message,
            "machine type `small` is not present in capacity catalog"
        );

        let tier_req = CapacityRequest {
            spec: CapacitySpec {
                initial_machine_type: "small".to_string(),
            },
            old_member_disrupted: false,
        };
        let preflight_tier_err = preflight_capacity(&tier_req, Some(&catalog)).unwrap_err();
        assert_eq!(
            preflight_tier_err.reason,
            RejectionReason::UnsupportedMachineType
        );
        assert_eq!(preflight_tier_err.field_path, "machine_type");
        assert_eq!(
            preflight_tier_err.message,
            "machine type `small` not found in catalog"
        );

        // 4. Entry not in allowed lifecycle state (e.g. "offline") is rejected
        let entry_offline = CatalogEntry::new(
            "e2-standard-4",
            "lumen.axiom.dev/capacity-profile",
            "offline",
            10,
        );
        let catalog_offline = CapacityCatalog::new(vec![entry_offline]);
        let req_offline = CapacityRequest {
            spec: CapacitySpec {
                initial_machine_type: "e2-standard-4".to_string(),
            },
            old_member_disrupted: false,
        };
        let offline_err = preflight_capacity(&req_offline, Some(&catalog_offline)).unwrap_err();
        assert_eq!(offline_err.reason, RejectionReason::CatalogIncompatible);
        assert_eq!(offline_err.field_path, "catalog");
        assert_eq!(
            offline_err.message,
            "capacity profile for `e2-standard-4` is marked incompatible or invalid"
        );

        // 5. Entry in draining lifecycle is rejected
        let entry_draining = CatalogEntry::new(
            "e2-standard-8",
            "lumen.axiom.dev/capacity-profile",
            "draining",
            10,
        );
        let catalog_draining = CapacityCatalog::new(vec![entry_draining]);
        let req_draining = CapacityRequest {
            spec: CapacitySpec {
                initial_machine_type: "e2-standard-8".to_string(),
            },
            old_member_disrupted: false,
        };
        let draining_err = preflight_capacity(&req_draining, Some(&catalog_draining)).unwrap_err();
        assert_eq!(draining_err.reason, RejectionReason::CatalogDraining);
        assert_eq!(draining_err.field_path, "catalog");
        assert_eq!(
            draining_err.message,
            "capacity profile for `e2-standard-8` is in draining lifecycle"
        );
    }

    #[test]
    fn negative_control_resource_envelope_has_no_catalog_dependency() {
        // Negative control: verify ResourceEnvelope, its parser, bounds check,
        // and allocatable fitting have zero dependency on CapacityCatalog.
        // The parser and bounds check take no catalog argument and construct
        // an envelope with NO CapacityCatalog in scope.
        let env = parse_resource_envelope("500m", "512Mi", "10Gi", Some("standard-rwo"))
            .expect("envelope parses with no catalog in scope");

        assert_eq!(env.cpu_millicores, 500);
        assert_eq!(env.memory_mib, 512);
        assert_eq!(env.storage_bytes, 10 * 1024 * 1024 * 1024);
        assert_eq!(env.storage_class.as_deref(), Some("standard-rwo"));

        check_envelope_bounds(&env).expect("bounds check with no catalog in scope");

        let allocatable = CapacityVector {
            cpu_millicores: 2000,
            memory_mib: 4096,
        };
        let reserves = CapacityVector {
            cpu_millicores: 500,
            memory_mib: 1024,
        };
        let headroom = CapacityVector {
            cpu_millicores: 500,
            memory_mib: 1024,
        };
        fits_allocatable(&env, allocatable, reserves, headroom)
            .expect("fits allocatable with no catalog in scope");
    }
}
