use std::sync::Mutex;

use crate::access::application::auth_config::{
    AuthConfig, AuthProfile, SERVICE_ACCOUNT_NAMESPACE_FILE,
};

// Process-global env mutex shared across the env-mutating tests.
static AUTH_ENV_LOCK: Mutex<()> = Mutex::new(());

fn clear_auth_env() {
    unsafe {
        std::env::remove_var("LUMEN_AUTH");
        std::env::remove_var("LUMEN_AUTH_NAMESPACE");
        std::env::remove_var("POD_NAMESPACE");
    }
}

// ---- configuration --------------------------------------------------

#[test]
fn auth_config_open_is_not_required() {
    let config = AuthConfig::open();
    assert!(!config.required);
    assert_eq!(config.profile(), AuthProfile::Off);
}

#[test]
fn auth_config_from_env_open_when_unset() {
    let _g = AUTH_ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    clear_auth_env();
    let cfg = AuthConfig::from_env().unwrap();
    assert!(!cfg.required);
}

#[test]
fn auth_config_from_env_accepts_both_disabled_spellings() {
    let _g = AUTH_ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    for spelling in ["off", "disabled", "DISABLED", " off "] {
        clear_auth_env();
        unsafe {
            std::env::set_var("LUMEN_AUTH", spelling);
        }
        let cfg = AuthConfig::from_env()
            .unwrap_or_else(|e| panic!("`{spelling}` is a disabled spelling: {e:#}"));
        assert!(!cfg.required);
    }
    clear_auth_env();
}

/// R9: `required` resolves the serving namespace every check is scoped to.
#[test]
fn auth_config_required_takes_the_serving_namespace_from_the_environment() {
    let _g = AUTH_ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    clear_auth_env();
    unsafe {
        std::env::set_var("LUMEN_AUTH", "required");
        std::env::set_var("POD_NAMESPACE", "serving");
    }
    let cfg = AuthConfig::from_env().unwrap();
    assert!(cfg.required);
    assert_eq!(cfg.profile(), AuthProfile::ManagedAudience);
    assert_eq!(cfg.namespace, "serving");

    // The explicit override wins over the downward-API value.
    unsafe {
        std::env::set_var("LUMEN_AUTH_NAMESPACE", "override");
    }
    assert_eq!(AuthConfig::from_env().unwrap().namespace, "override");
    clear_auth_env();
}

#[test]
fn auth_config_in_cluster_selects_only_the_kubernetes_default_profile() {
    let _g = AUTH_ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    clear_auth_env();
    unsafe {
        std::env::set_var("LUMEN_AUTH", " in-cluster ");
        std::env::set_var("POD_NAMESPACE", "lumen");
    }
    let config = AuthConfig::from_env().unwrap();
    assert!(config.required);
    assert_eq!(config.namespace, "lumen");
    assert_eq!(config.profile(), AuthProfile::KubernetesDefault);
    assert_eq!(
        AuthConfig::required_in("lumen").profile(),
        AuthProfile::ManagedAudience
    );
    assert_eq!(
        AuthConfig::in_cluster("lumen").profile(),
        AuthProfile::KubernetesDefault
    );
    clear_auth_env();
}

/// R9: an unscoped SubjectAccessReview asks about a different resource
/// than the request touched, so `required` refuses to start without a
/// namespace — unless the pod's own ServiceAccount file supplies one,
/// which is exactly when delegation can work at all.
#[test]
fn auth_config_required_fails_closed_without_a_serving_namespace() {
    let _g = AUTH_ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    clear_auth_env();
    unsafe {
        std::env::set_var("LUMEN_AUTH", "required");
    }
    let result = AuthConfig::from_env();
    clear_auth_env();
    if std::path::Path::new(SERVICE_ACCOUNT_NAMESPACE_FILE).exists() {
        // Running inside a pod: the namespace is discoverable, so there is
        // nothing to fail closed about.
        assert!(result.unwrap().required);
        return;
    }
    let message = format!("{:#}", result.unwrap_err());
    assert!(message.contains("LUMEN_AUTH=required"), "{message}");
    assert!(message.contains("SubjectAccessReview"), "{message}");
    assert!(message.contains("Refusing to start"), "{message}");
}

#[test]
fn auth_config_in_cluster_fails_closed_without_a_serving_namespace() {
    let _g = AUTH_ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    clear_auth_env();
    unsafe {
        std::env::set_var("LUMEN_AUTH", "in-cluster");
    }
    let result = AuthConfig::from_env();
    clear_auth_env();
    if std::path::Path::new(SERVICE_ACCOUNT_NAMESPACE_FILE).exists() {
        assert_eq!(result.unwrap().profile(), AuthProfile::KubernetesDefault);
        return;
    }
    let message = format!("{:#}", result.unwrap_err());
    assert!(message.contains("LUMEN_AUTH=in-cluster"), "{message}");
    assert!(message.contains("SubjectAccessReview"), "{message}");
    assert!(message.contains("Refusing to start"), "{message}");
}

#[test]
fn auth_config_from_env_rejects_unknown_auth_mode() {
    let _g = AUTH_ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    clear_auth_env();
    unsafe {
        std::env::set_var("LUMEN_AUTH", "require");
    }
    let err = AuthConfig::from_env().unwrap_err();
    assert!(err.to_string().contains("LUMEN_AUTH"));
    clear_auth_env();
}
