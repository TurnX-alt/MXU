//! CloudWeb controller backends.
//!
//! Two implementations share one factory:
//!
//! * `fake` — deterministic stub for factory/pool/lifetime qualification only.
//! * `sidecar` — real client for the CloudWeb sidecar process over Windows
//!   named pipes (protocol `contract-v0.1`).
//!
//! There is no fallback between them: an unknown or unavailable backend fails
//! with an explicit error instead of silently degrading.

mod fake;
mod sidecar;
pub mod wire;

pub use fake::FakeCloudWebController;
use maa_framework::controller::Controller;
use sha2::{Digest, Sha256};

/// Backends the Host can actually serve. Anything else is a configuration error.
pub const SUPPORTED_BACKENDS: [&str; 2] = ["fake", "sidecar"];

/// Build a CloudWeb controller for the configured backend.
pub fn create_cloudweb_controller(
    backend: &str,
    session_id: &str,
    profile: &str,
) -> Result<Controller, String> {
    match backend {
        "fake" => Controller::new_custom(FakeCloudWebController::new(session_id, profile))
            .map_err(|error| error.to_string()),
        "sidecar" => sidecar::create_sidecar_controller(session_id, profile),
        other => Err(format!(
            "cloudweb_backend_unsupported:{other}; accepted: {}",
            SUPPORTED_BACKENDS.join(",")
        )),
    }
}

/// Stable UUID derived from a seed, namespaced by backend so the fake stub and
/// the real sidecar never present the same controller identity.
pub(crate) fn stable_uuid(seed: &str) -> String {
    let hex = format!("{:x}", Sha256::digest(seed.as_bytes()));
    format!(
        "{}-{}-{}-{}-{}",
        &hex[0..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..32]
    )
}

#[cfg(test)]
mod tests {
    use super::{create_cloudweb_controller, SUPPORTED_BACKENDS};

    #[test]
    fn an_unknown_backend_is_named_with_the_accepted_set() {
        let error = create_cloudweb_controller("nope", "session-a", "default")
            .expect_err("unknown backend must not produce a controller");
        assert!(
            error.starts_with("cloudweb_backend_unsupported:nope"),
            "{error}"
        );
        for backend in SUPPORTED_BACKENDS {
            assert!(error.contains(backend), "{error}");
        }
    }

    #[test]
    fn the_fake_backend_needs_no_sidecar_process() {
        create_cloudweb_controller("fake", "session-factory", "default")
            .expect("fake backend is available everywhere");
    }

    #[test]
    fn the_sidecar_backend_never_falls_back_to_the_fake_stub() {
        // Constructing the real backend only resolves configuration, so the
        // result depends on CLOUDWEB_SIDECAR_PATH. Either way it must not be a
        // fake controller: an error states the missing configuration, and a
        // success is a controller that launches a process on connect.
        match std::env::var(super::sidecar::SIDECAR_PATH_ENV) {
            Ok(_) => {
                create_cloudweb_controller("sidecar", "session-sidecar", "default")
                    .expect("configured sidecar backend produces a controller");
            }
            Err(_) => {
                let error = create_cloudweb_controller("sidecar", "session-sidecar", "default")
                    .expect_err("unconfigured sidecar backend must fail");
                assert!(
                    error.contains("cloudweb_sidecar_exec_missing"),
                    "unexpected error: {error}"
                );
            }
        }
    }
}
