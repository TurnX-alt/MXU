//! Real CloudWeb sidecar backend: process lifecycle, named-pipe client,
//! owner/lease handling, frame consumption and input mapping.
//!
//! Layout:
//!
//! * [`SidecarClient`] is synchronous and generic over [`Transport`], so the
//!   protocol state machine is unit-tested against a scripted test double.
//! * [`SidecarCloudWebController`] is the `CustomControllerCallback` used by
//!   MaaFramework. It launches the sidecar executable, owns the child process
//!   and the Windows named pipes, and never falls back to the fake stub.
//!
//! Lease model (contract sections 4/9, review-4 N1): the control pipe is serial,
//! so a slow command cannot be interleaved with a renewal. The client renews
//! before and after slow operations, re-acquires a new generation when the
//! sidecar reports the lease is gone, and records the observed renewal gap and
//! lease losses instead of claiming the 350 ms lease is guaranteed.

use std::cmp::Ordering;
use std::path::PathBuf;
use std::process::Command;
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use maa_framework::{
    controller::Controller, custom_controller::CustomControllerCallback, ControllerFeature,
};
use serde_json::{json, Value};

use super::stable_uuid;
use super::wire::{self, FrameHeader, SidecarError};

/// Environment key for the sidecar executable path.
pub const SIDECAR_PATH_ENV: &str = "CLOUDWEB_SIDECAR_PATH";
/// Environment key for additional sidecar arguments.
pub const SIDECAR_ARGS_ENV: &str = "CLOUDWEB_SIDECAR_ARGS";
/// Environment key carrying the one-time launch token into the child process.
pub const TOKEN_ENV: &str = "CLOUDWEB_TOKEN";

/// Host name reported during the handshake.
const HOST_NAME: &str = "MXU";
/// UI profile requested during the handshake.
const UI_PROFILE: &str = "cloud_web.default";
/// Capabilities the host declares; the sidecar's declared set is authoritative
/// for what the session can actually do.
const HOST_CAPABILITIES: [&str; 5] = [
    "frame.latest",
    "input.keyboard",
    "input.relative",
    "input.text",
    "geometry.v1",
];

/// Sidecar startup and handshake budget (contract section 9).
const READINESS_TIMEOUT: Duration = Duration::from_secs(10);
/// Control-plane read budget for one command round trip.
const CONTROL_TIMEOUT: Duration = Duration::from_secs(3);
/// Frame-plane read budget for one capture.
const FRAME_TIMEOUT: Duration = Duration::from_millis(1000);
/// Grace period between asking the child to stop and forcing a kill.
const CLOSE_GRACE: Duration = Duration::from_millis(1500);

/// Lease values assumed until the sidecar reports its limits.
const FALLBACK_LEASE_MS: u64 = 350;
const FALLBACK_RENEW_MS: u64 = 100;
const FALLBACK_OBSERVATION_MS: u64 = 1000;

/// One frame-plane packet: the fixed header plus the PNG payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawFrame {
    pub header: [u8; wire::FRAME_HEADER_BYTES],
    pub bytes: Vec<u8>,
}

/// Blocking byte transport used by [`SidecarClient`].
pub trait Transport {
    /// Sends one length-prefixed control frame.
    fn send_control(&mut self, frame: &[u8]) -> Result<(), SidecarError>;
    /// Reads one control payload. `None` means the peer closed the pipe.
    fn recv_control(&mut self) -> Result<Option<Vec<u8>>, SidecarError>;
    /// Reads one frame packet, waiting at most `timeout` for it.
    fn recv_frame(&mut self, timeout: Duration) -> Result<Option<RawFrame>, SidecarError>;
}

/// Launch parameters for one sidecar process.
#[derive(Clone)]
pub struct LaunchConfig {
    pub exec: PathBuf,
    pub extra_args: Vec<String>,
    pub session_id: String,
    pub profile: String,
    pub owner_id: String,
    pub backend_epoch: String,
    pub control_pipe: String,
    pub frame_pipe: String,
    /// One-time launch token. Never logged, never committed.
    token: String,
}

impl std::fmt::Debug for LaunchConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("LaunchConfig")
            .field("exec", &self.exec)
            .field("extra_args", &self.extra_args)
            .field("session_id", &self.session_id)
            .field("profile", &self.profile)
            .field("owner_id", &self.owner_id)
            .field("backend_epoch", &self.backend_epoch)
            .field("control_pipe", &self.control_pipe)
            .field("frame_pipe", &self.frame_pipe)
            .field("token", &"<redacted>")
            .finish()
    }
}

impl LaunchConfig {
    /// Resolves launch parameters through an injectable environment lookup, so
    /// tests never mutate process state.
    pub fn resolve(
        session_id: &str,
        profile: &str,
        lookup: &dyn Fn(&str) -> Option<String>,
    ) -> Result<Self, SidecarError> {
        if session_id.trim().is_empty() {
            return Err(SidecarError::local(
                "cloudweb_session_required",
                "CloudWeb requires a non-empty session id",
            ));
        }
        let exec = lookup(SIDECAR_PATH_ENV)
            .map(PathBuf::from)
            .filter(|path| !path.as_os_str().is_empty())
            .ok_or_else(|| {
                SidecarError::local(
                    "cloudweb_sidecar_exec_missing",
                    format!("{SIDECAR_PATH_ENV} is not set; the sidecar backend cannot launch"),
                )
            })?;
        let extra_args = match lookup(SIDECAR_ARGS_ENV) {
            Some(raw) if !raw.trim().is_empty() => shell_words::split(&raw).map_err(|error| {
                SidecarError::local("cloudweb_sidecar_args_invalid", error.to_string())
            })?,
            _ => Vec::new(),
        };
        let token = random_hex(32)?;
        Ok(Self {
            exec,
            extra_args,
            session_id: session_id.to_string(),
            profile: profile.to_string(),
            owner_id: format!("mxu:{session_id}"),
            backend_epoch: random_hex(16)?,
            control_pipe: format!(r"\\.\pipe\cloudweb-mxu-ctl-{}", random_hex(16)?),
            frame_pipe: format!(r"\\.\pipe\cloudweb-mxu-frame-{}", random_hex(16)?),
            token,
        })
    }

    /// Command line for the child process. The token travels only in the child
    /// environment, so it never appears here.
    pub fn arguments(&self) -> Vec<String> {
        let mut arguments = vec![
            "--control-pipe".to_string(),
            self.control_pipe.clone(),
            "--frame-pipe".to_string(),
            self.frame_pipe.clone(),
            "--session".to_string(),
            self.session_id.clone(),
            "--epoch".to_string(),
            self.backend_epoch.clone(),
        ];
        arguments.extend(self.extra_args.iter().cloned());
        arguments
    }

    pub fn token(&self) -> &str {
        &self.token
    }
}

/// Random hex for the pipe-name nonces, backend epoch and launch token. The v1
/// trust boundary is the current Windows user (contract section 2.1); the pipe
/// ACL hardening itself is tracked as CW-07 and is not claimed here.
#[cfg(windows)]
fn random_hex(bytes: usize) -> Result<String, SidecarError> {
    use windows::Win32::Foundation::NTSTATUS;
    use windows::Win32::Security::Cryptography::{
        BCryptGenRandom, BCRYPT_USE_SYSTEM_PREFERRED_RNG,
    };

    let mut buffer = vec![0u8; bytes];
    let status = unsafe { BCryptGenRandom(None, &mut buffer, BCRYPT_USE_SYSTEM_PREFERRED_RNG) };
    if status != NTSTATUS(0) {
        return Err(SidecarError::local(
            "cloudweb_random_failed",
            format!("BCryptGenRandom failed with status {}", status.0),
        ));
    }
    Ok(hex(&buffer))
}

/// Non-Windows builds have no named-pipe transport.
#[cfg(not(windows))]
fn random_hex(_bytes: usize) -> Result<String, SidecarError> {
    Err(SidecarError::local(
        "cloudweb_sidecar_windows_only",
        "the sidecar backend uses Windows named pipes",
    ))
}

fn hex(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push_str(&format!("{byte:02x}"));
    }
    output
}

/// Values accepted by the handshake.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HelloInfo {
    pub backend_version: String,
    pub generation: u64,
    pub state: String,
    pub capabilities: Vec<String>,
    pub lease_ms: u64,
    pub renew_ms: u64,
    pub observation_max_age_ms: u64,
}

/// A capture the host may justify an action with.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CapturedFrame {
    pub bytes: Vec<u8>,
    pub frame_id: u64,
    pub geometry_revision: u64,
    pub captured_at: Instant,
}

/// Synchronous protocol client. The sidecar keeps the input ledger; this type
/// only tracks the host-side view of identity, generation and observation.
pub struct SidecarClient<T: Transport> {
    transport: T,
    session_id: String,
    backend_epoch: String,
    owner_id: String,
    token: String,
    generation: u64,
    seq: u64,
    lease_ms: u64,
    renew_ms: u64,
    observation_max_age_ms: u64,
    capabilities: Vec<String>,
    backend_version: String,
    state: String,
    owner_renewed_at: Option<Instant>,
    observation: Option<CapturedFrame>,
    handshaked: bool,
    faulted: bool,
    closed: bool,
    last_error: Option<String>,
    /// Largest observed delay between two successful renewals: the N1 evidence
    /// that the serial control pipe can hold renewals behind a slow command.
    max_renew_gap_ms: u64,
    /// Times the lease was already gone when the host tried to renew.
    lease_losses: u64,
    slowest_command_ms: u64,
    commands: u64,
}

impl<T: Transport> SidecarClient<T> {
    pub fn new(config: &LaunchConfig, transport: T) -> Self {
        Self {
            transport,
            session_id: config.session_id.clone(),
            backend_epoch: config.backend_epoch.clone(),
            owner_id: config.owner_id.clone(),
            token: config.token.clone(),
            generation: 0,
            seq: 0,
            lease_ms: FALLBACK_LEASE_MS,
            renew_ms: FALLBACK_RENEW_MS,
            observation_max_age_ms: FALLBACK_OBSERVATION_MS,
            capabilities: Vec::new(),
            backend_version: String::new(),
            state: String::new(),
            owner_renewed_at: None,
            observation: None,
            handshaked: false,
            faulted: false,
            closed: false,
            last_error: None,
            max_renew_gap_ms: 0,
            lease_losses: 0,
            slowest_command_ms: 0,
            commands: 0,
        }
    }

    pub fn session_id(&self) -> &str {
        &self.session_id
    }

    pub fn has_capability(&self, name: &str) -> bool {
        self.capabilities.iter().any(|value| value == name)
    }

    pub fn is_faulted(&self) -> bool {
        self.faulted
    }

    pub fn is_closed(&self) -> bool {
        self.closed
    }

    pub fn observation_age_ms(&self) -> Option<u64> {
        self.observation
            .as_ref()
            .map(|frame| frame.captured_at.elapsed().as_millis() as u64)
    }

    /// Sends `hello` and validates `hello_ok`. Every failure keeps an explicit
    /// error code; there is no fallback to another backend.
    pub fn handshake(&mut self) -> Result<HelloInfo, SidecarError> {
        if self.handshaked {
            return Err(SidecarError::local(
                "cloudweb_already_handshaked",
                "the handshake already completed",
            ));
        }
        let hello = wire::hello_frame(
            &self.token,
            HOST_NAME,
            env!("CARGO_PKG_VERSION"),
            "",
            &HOST_CAPABILITIES,
            UI_PROFILE,
        );
        self.write(&hello)?;
        let value = self.read_control()?.ok_or_else(|| {
            self.fail(SidecarError::local(
                "cloudweb_control_closed",
                "control pipe closed before hello_ok",
            ))
        })?;

        let kind = value["type"].as_str().unwrap_or_default();
        if kind != "hello_ok" {
            return Err(self.fail(SidecarError::from_frame(&value)));
        }
        let major = value["protocol_major"].as_u64().unwrap_or_default();
        if major != u64::from(wire::PROTOCOL_MAJOR) {
            return Err(self.fail(SidecarError::local(
                "cloudweb_protocol_mismatch",
                format!("sidecar protocol major {major} is not supported"),
            )));
        }
        let epoch = value["backend_epoch"].as_str().unwrap_or_default();
        if epoch != self.backend_epoch {
            return Err(self.fail(SidecarError::local(
                "cloudweb_epoch_mismatch",
                "sidecar backend epoch does not match the launched process",
            )));
        }
        let state = value["state"].as_str().unwrap_or_default().to_string();
        if state == "closed" || state == "faulted" {
            return Err(self.fail(SidecarError::local(
                "cloudweb_sidecar_not_ready",
                format!("sidecar reported state {state}"),
            )));
        }

        self.handshaked = true;
        self.generation = value["generation"].as_u64().unwrap_or_default();
        self.backend_version = value["backend_version"]
            .as_str()
            .unwrap_or_default()
            .to_string();
        self.capabilities = value["capabilities"]
            .as_array()
            .map(|values| {
                values
                    .iter()
                    .filter_map(Value::as_str)
                    .map(str::to_string)
                    .collect()
            })
            .unwrap_or_default();
        if let Some(limits) = value.get("limits") {
            self.lease_ms = limits["lease_ms"].as_u64().unwrap_or(self.lease_ms);
            self.renew_ms = limits["renew_ms"].as_u64().unwrap_or(self.renew_ms);
            self.observation_max_age_ms = limits["observation_max_age_ms"]
                .as_u64()
                .unwrap_or(self.observation_max_age_ms);
        }
        self.state = state.clone();
        Ok(HelloInfo {
            backend_version: self.backend_version.clone(),
            generation: self.generation,
            state,
            capabilities: self.capabilities.clone(),
            lease_ms: self.lease_ms,
            renew_ms: self.renew_ms,
            observation_max_age_ms: self.observation_max_age_ms,
        })
    }

    /// Requests ownership. A new owner always starts a new generation.
    pub fn acquire_owner(&mut self) -> Result<(), SidecarError> {
        let detail = self.command("acquire_owner", json!({ "owner_id": self.owner_id }))?;
        let lease_ms = detail
            .as_ref()
            .and_then(|value| value["lease_ms"].as_u64())
            .unwrap_or(self.lease_ms);
        log::info!(
            target: "cloudweb",
            "CloudWeb owner acquired: session_id={} generation={} lease_ms={}",
            self.session_id,
            self.generation,
            lease_ms
        );
        self.owner_renewed_at = Some(Instant::now());
        Ok(())
    }

    /// Renews the lease. When the sidecar reports the lease is gone the owner is
    /// dropped locally so the next acquisition starts a new generation; the
    /// rejected command is never replayed.
    pub fn renew_lease(&mut self) -> Result<(), SidecarError> {
        let gap_ms = self
            .owner_renewed_at
            .map(|at| at.elapsed().as_millis() as u64)
            .unwrap_or(0);
        self.max_renew_gap_ms = self.max_renew_gap_ms.max(gap_ms);
        let result = self.command("renew_lease", json!({ "owner_id": self.owner_id }));
        match result {
            Ok(_) => {
                self.owner_renewed_at = Some(Instant::now());
                Ok(())
            }
            Err(error) => {
                let lost = error.is_lease_loss() || error.is_generation_stale();
                self.owner_renewed_at = None;
                if lost {
                    self.lease_losses += 1;
                }
                Err(error)
            }
        }
    }

    /// Refreshes an already held lease. Frames and preview do not need
    /// ownership, so a session without an owner is left untouched here.
    fn keep_lease_warm(&mut self) {
        let due = self
            .owner_renewed_at
            .is_some_and(|at| at.elapsed() >= Duration::from_millis(self.renew_ms));
        if due {
            match self.renew_lease() {
                Ok(()) => {}
                Err(error) if error.is_lease_loss() || error.is_generation_stale() => {
                    log::warn!(
                        target: "cloudweb",
                        "CloudWeb lease is no longer held ({}); the next input acquires a new generation",
                        error.code()
                    );
                }
                Err(error) => {
                    log::debug!(target: "cloudweb", "CloudWeb renewal skipped: {}", error.code());
                }
            }
        }
    }

    /// Keeps ownership alive for input: acquire when missing, renew when due,
    /// and recover a new generation when the lease was fenced while the pipe was
    /// busy.
    fn ensure_lease(&mut self) -> Result<(), SidecarError> {
        let due = match self.owner_renewed_at {
            None => true,
            Some(at) => at.elapsed() >= Duration::from_millis(self.renew_ms),
        };
        if !due {
            return Ok(());
        }
        if self.owner_renewed_at.is_some() {
            return match self.renew_lease() {
                Ok(()) => Ok(()),
                Err(error) if error.is_lease_loss() || error.is_generation_stale() => {
                    log::warn!(
                        target: "cloudweb",
                        "CloudWeb lease lost before renewal, acquiring a new generation: {} (max gap {} ms)",
                        error.code(),
                        self.max_renew_gap_ms
                    );
                    self.acquire_owner()
                }
                Err(error) => Err(error),
            };
        }
        self.acquire_owner()
    }

    /// Takes the latest canonical frame from the frame plane and pairs it with
    /// the receipt metadata that describes exactly those bytes.
    pub fn capture(&mut self) -> Result<CapturedFrame, SidecarError> {
        self.keep_lease_warm();
        let detail = self
            .command("get_latest_frame", json!({}))?
            .ok_or_else(|| {
                SidecarError::local("cloudweb_frame_metadata", "capture returned no metadata")
            })?;
        let frame_id = detail["frame_id"].as_u64().unwrap_or_default();
        let width = detail["width"].as_u64().unwrap_or_default();
        let height = detail["height"].as_u64().unwrap_or_default();
        let freshness = detail["freshness"]
            .as_str()
            .unwrap_or("unknown")
            .to_string();
        let geometry_revision = detail["geometry_revision"].as_u64().unwrap_or_default();
        if width != u64::from(wire::CANONICAL_WIDTH) || height != u64::from(wire::CANONICAL_HEIGHT)
        {
            return Err(self.fail_local(
                "cloudweb_frame_not_canonical",
                format!("frame is {width}x{height}, expected 1280x720"),
            ));
        }
        if wire::Freshness::parse(&freshness) != Some(wire::Freshness::Fresh) {
            // A stale or unknown frame must never be presented as a new
            // observation to the recognizer.
            return Err(self.fail_local(
                "cloudweb_frame_not_fresh",
                format!("frame {frame_id} freshness is {freshness}"),
            ));
        }

        let (header, bytes) = self.read_matching_frame(frame_id)?;
        if header.generation != self.generation {
            return Err(self.fail_local(
                "cloudweb_frame_generation",
                format!(
                    "frame generation {} is not the current {}",
                    header.generation, self.generation
                ),
            ));
        }
        if header.freshness != wire::Freshness::Fresh {
            return Err(self.fail_local(
                "cloudweb_frame_not_fresh",
                format!("frame plane header reports {:?}", header.freshness),
            ));
        }

        let frame = CapturedFrame {
            bytes,
            frame_id,
            geometry_revision,
            captured_at: Instant::now(),
        };
        self.observation = Some(frame.clone());
        // A capture can consume most of the lease, so renew right after it.
        if self.owner_renewed_at.is_some() {
            let _ = self.renew_after_slow_command();
        }
        Ok(frame)
    }

    fn renew_after_slow_command(&mut self) -> Result<(), SidecarError> {
        let overdue = self
            .owner_renewed_at
            .is_some_and(|at| at.elapsed().as_millis() as u64 > self.lease_ms / 2);
        if !overdue {
            return Ok(());
        }
        match self.renew_lease() {
            Ok(()) => Ok(()),
            Err(error) if error.is_lease_loss() => {
                log::warn!(
                    target: "cloudweb",
                    "CloudWeb lease expired during a slow command (gap {} ms, lease {} ms)",
                    self.max_renew_gap_ms,
                    self.lease_ms
                );
                Err(error)
            }
            Err(error) => Err(error),
        }
    }

    fn read_matching_frame(
        &mut self,
        expected_frame_id: u64,
    ) -> Result<(FrameHeader, Vec<u8>), SidecarError> {
        let deadline = Instant::now() + FRAME_TIMEOUT;
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(self.fail_local(
                    "cloudweb_frame_timeout",
                    format!("no frame {expected_frame_id} arrived in time"),
                ));
            }
            let packet = self
                .transport
                .recv_frame(remaining)?
                .ok_or_else(|| self.fail_local("cloudweb_frame_closed", "frame pipe closed"))?;
            let header = FrameHeader::decode(&packet.header)?;
            match header.frame_id.cmp(&expected_frame_id) {
                Ordering::Equal => return Ok((header, packet.bytes)),
                // The plane can still hold a packet published before this
                // capture; skip it and wait for the matching one.
                Ordering::Less => continue,
                Ordering::Greater => {
                    return Err(self.fail_local(
                        "cloudweb_frame_desynced",
                        format!(
                            "frame plane is ahead: {} > requested {expected_frame_id}",
                            header.frame_id
                        ),
                    ));
                }
            }
        }
    }

    /// The CDP path is single-pointer; contact ids other than 0 have no
    /// mapping and are refused instead of being silently remapped.
    fn left_button(contact: i32) -> Result<u8, SidecarError> {
        match contact {
            0 => Ok(0),
            other => Err(SidecarError::local(
                "cloudweb_unsupported_multitouch",
                format!("contact {other} has no single-pointer mapping"),
            )),
        }
    }

    fn required_capability(op: &str) -> Option<&'static str> {
        match op {
            "key_down" | "key_up" => Some("input.keyboard"),
            "insert_text" => Some("input.text"),
            "move_relative" => Some("input.relative"),
            "mouse_down" | "mouse_up" | "move_absolute" => Some("geometry.v1"),
            _ => None,
        }
    }

    fn observation_for_input(&self) -> Result<Option<wire::Observation>, SidecarError> {
        let frame = match &self.observation {
            Some(frame) => frame,
            None => return Ok(None),
        };
        let age_ms = frame.captured_at.elapsed().as_millis() as u64;
        if age_ms > self.observation_max_age_ms {
            return Err(SidecarError::local(
                "cloudweb_observation_expired",
                format!("observation {} is {} ms old", frame.frame_id, age_ms),
            ));
        }
        Ok(Some(wire::Observation {
            frame_id: frame.frame_id,
            age_ms,
            geometry_revision: frame.geometry_revision,
        }))
    }

    /// One input action behind the ownership, capability and observation gates.
    /// A refused action returns without sending anything.
    fn input(&mut self, op: &str, params: Value, safety: bool) -> Result<(), SidecarError> {
        if let Some(capability) = Self::required_capability(op) {
            if !self.has_capability(capability) {
                return Err(self.record(SidecarError::local(
                    "cloudweb_capability_missing",
                    format!("{op} requires the {capability} capability"),
                )));
            }
        }
        self.ensure_lease()?;
        let observation = if safety {
            // Releasing a held key/button stays allowed without a new frame.
            self.observation_for_input().unwrap_or(None)
        } else {
            self.observation_for_input()?
        };
        if observation.is_none() && !safety {
            return Err(self.record(SidecarError::local(
                "cloudweb_no_observation",
                format!("{op} needs a fresh observation; run screencap first"),
            )));
        }

        let started = Instant::now();
        let attempt = self.send(op, params.clone(), observation.as_ref());
        match attempt {
            Ok(_) => Ok(()),
            // A `rejected` receipt never dispatched, so one refresh and retry is
            // allowed. An `unknown` receipt is never retried.
            Err(error) if !safety && error.is_frame_stale() => {
                log::debug!(
                    target: "cloudweb",
                    "CloudWeb rejected {op} for a stale observation, refreshing once"
                );
                self.capture()?;
                let observation = self.observation_for_input()?;
                let retry = self.send(op, params, observation.as_ref());
                self.slowest_command_ms = self
                    .slowest_command_ms
                    .max(started.elapsed().as_millis() as u64);
                retry.map(|_| ())
            }
            Err(error) => Err(error),
        }
    }

    pub fn tap(&mut self, x: i32, y: i32) -> Result<(), SidecarError> {
        self.press(0, x, y)?;
        self.release_button(0)
    }

    pub fn press(&mut self, contact: i32, x: i32, y: i32) -> Result<(), SidecarError> {
        let button = Self::left_button(contact)?;
        self.input(
            "mouse_down",
            json!({ "button": button, "x": x, "y": y }),
            false,
        )
    }

    pub fn release_button(&mut self, contact: i32) -> Result<(), SidecarError> {
        let button = Self::left_button(contact)?;
        self.input("mouse_up", json!({ "button": button }), true)
    }

    pub fn move_to(&mut self, x: i32, y: i32) -> Result<(), SidecarError> {
        self.input("move_absolute", json!({ "x": x, "y": y }), false)
    }

    pub fn look(&mut self, dx: i32, dy: i32) -> Result<(), SidecarError> {
        self.input("move_relative", json!({ "dx": dx, "dy": dy }), false)
    }

    pub fn key_down(&mut self, key: i32) -> Result<(), SidecarError> {
        self.input("key_down", json!({ "key": key }), false)
    }

    pub fn key_up(&mut self, key: i32) -> Result<(), SidecarError> {
        self.input("key_up", json!({ "key": key }), true)
    }

    pub fn click_key(&mut self, key: i32) -> Result<(), SidecarError> {
        self.key_down(key)?;
        self.key_up(key)
    }

    pub fn scroll(&mut self, dx: i32, dy: i32) -> Result<(), SidecarError> {
        self.input("wheel", json!({ "dx": dx, "dy": dy }), false)
    }

    pub fn input_text(&mut self, text: &str) -> Result<(), SidecarError> {
        self.input("insert_text", json!({ "text": text }), false)
    }

    /// Swipe as a full down/move/up path: the P03 observation is that a partial
    /// path is rejected by the input channel. The duration is not interpolated
    /// by the v1 command plane and is only recorded in the log.
    pub fn swipe(
        &mut self,
        x1: i32,
        y1: i32,
        x2: i32,
        y2: i32,
        duration: i32,
    ) -> Result<(), SidecarError> {
        self.press(0, x1, y1)?;
        let moved = self.move_to(x2, y2);
        let released = self.release_button(0);
        match (moved, released) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(error), _) | (_, Err(error)) => {
                log::debug!(
                    target: "cloudweb",
                    "CloudWeb swipe of {} ms stopped at {}: {}",
                    duration,
                    error.code(),
                    error.message
                );
                Err(error)
            }
        }
    }

    /// Gives up ownership: the sidecar fences the generation and releases every
    /// held key/button before returning ownership.
    pub fn release_owner(&mut self) -> Result<(), SidecarError> {
        let result = self.command("release_owner", json!({ "owner_id": self.owner_id }));
        self.owner_renewed_at = None;
        self.observation = None;
        match result {
            Ok(_) => Ok(()),
            Err(error) if error.is_lease_loss() => {
                // The sidecar already fenced the expired owner.
                Ok(())
            }
            Err(error) => Err(error),
        }
    }

    /// Asks the sidecar to close the session and stop. Afterwards the client
    /// refuses further commands. Close stays allowed on a faulted session,
    /// because only a close may leave that state.
    pub fn close(&mut self) -> Result<(), SidecarError> {
        if self.closed {
            return Ok(());
        }
        let result = self.command(
            "close",
            json!({ "mode": "session", "reason": "host_close" }),
        );
        self.closed = true;
        self.owner_renewed_at = None;
        self.observation = None;
        match result {
            Ok(_) => {
                self.state = "closed".to_string();
                Ok(())
            }
            Err(error) => Err(self.record(error)),
        }
    }

    pub fn diagnostics(&self) -> Value {
        json!({
            "backend": "cloud_web",
            "implementation": "sidecar",
            "protocol_version": wire::PROTOCOL_MAJOR,
            "backend_version": self.backend_version,
            "session_id": self.session_id,
            "backend_epoch": self.backend_epoch,
            "generation": self.generation,
            "state": self.state,
            "capabilities": self.capabilities,
            "owner_lease": {
                "held": self.owner_renewed_at.is_some(),
                "lease_ms": self.lease_ms,
                "renew_ms": self.renew_ms,
                "max_renew_gap_ms": self.max_renew_gap_ms,
                "lease_losses": self.lease_losses,
            },
            "observation_max_age_ms": self.observation_max_age_ms,
            "observation_age_ms": self.observation_age_ms(),
            "commands": self.commands,
            "slowest_command_ms": self.slowest_command_ms,
            "last_error": self.last_error,
        })
    }

    /// One command round trip; returns the receipt detail when accepted.
    fn command(&mut self, op: &str, params: Value) -> Result<Option<Value>, SidecarError> {
        self.send(op, params, None)
    }

    fn send(
        &mut self,
        op: &str,
        params: Value,
        observation: Option<&wire::Observation>,
    ) -> Result<Option<Value>, SidecarError> {
        if self.closed {
            return Err(SidecarError::local(
                "cloudweb_closed",
                "the CloudWeb session is closed",
            ));
        }
        // Close is the one command that may still run on a faulted session.
        if self.faulted && op != "close" {
            return Err(SidecarError::local(
                "cloudweb_faulted",
                "the CloudWeb session is faulted; close and relaunch are required",
            ));
        }
        if !self.handshaked {
            return Err(SidecarError::local(
                "cloudweb_not_handshaked",
                "the CloudWeb handshake has not completed",
            ));
        }

        self.seq += 1;
        let seq = self.seq;
        let deadline_ms = CONTROL_TIMEOUT.as_millis() as u64;
        let frame = wire::command_frame(
            op,
            &self.session_id,
            &self.backend_epoch,
            self.generation,
            seq,
            params,
            deadline_ms,
            observation,
        );
        let started = Instant::now();
        self.write(&frame)?;
        let value = self.read_control()?.ok_or_else(|| {
            self.fail(SidecarError::local(
                "cloudweb_control_closed",
                "control pipe closed during a command",
            ))
        })?;
        self.commands += 1;
        self.slowest_command_ms = self
            .slowest_command_ms
            .max(started.elapsed().as_millis() as u64);

        // Every receipt carries the sidecar's current generation, so the host
        // resynchronises instead of replaying a rejected command.
        if let Some(generation) = value["generation"].as_u64() {
            self.generation = generation;
        }

        let kind = value["type"].as_str().unwrap_or_default();
        if kind == "error" {
            return Err(self.fail(SidecarError::from_frame(&value)));
        }
        if kind != "receipt" {
            return Err(self.record(SidecarError::local(
                "cloudweb_protocol_shape",
                format!("expected a receipt, got {kind}"),
            )));
        }
        let receipt_seq = value["seq"].as_u64().unwrap_or_default();
        if receipt_seq != seq {
            return Err(self.record(SidecarError::local(
                "cloudweb_seq_mismatch",
                format!("receipt seq {receipt_seq} does not match command seq {seq}"),
            )));
        }

        match value["status"].as_str().unwrap_or_default() {
            "accepted" | "dispatched" => Ok(value.get("detail").cloned()),
            // Contract section 6: an unknown result must not be retried and the
            // remote input state stays possibly held on the sidecar side.
            "unknown" => Err(self.record(SidecarError::from_frame(&value))),
            other => {
                let error = SidecarError::from_frame(&value);
                let error = if other == "cancelled" {
                    SidecarError::new("cancelled", error.message)
                } else {
                    error
                };
                if error.is_faulted() {
                    self.faulted = true;
                }
                self.last_error = Some(error.code.clone());
                Err(error)
            }
        }
    }

    /// Marks the session unusable when the sidecar reports a terminal failure.
    fn record(&mut self, error: SidecarError) -> SidecarError {
        if error.is_faulted() || error.code == "cloudweb_sidecar_not_ready" {
            self.faulted = true;
        }
        self.last_error = Some(error.code.clone());
        error
    }

    fn fail(&mut self, error: SidecarError) -> SidecarError {
        self.faulted = true;
        self.last_error = Some(error.code.clone());
        error
    }

    fn fail_local(&mut self, code: &str, message: impl Into<String>) -> SidecarError {
        self.record(SidecarError::local(code, message))
    }

    fn write(&mut self, value: &Value) -> Result<(), SidecarError> {
        let frame = wire::encode_control(value)?;
        self.transport.send_control(&frame).map_err(|error| {
            self.record(SidecarError::new(
                format!("cloudweb_{}", error.code),
                format!("control write failed: {}", error.message),
            ))
        })
    }

    fn read_control(&mut self) -> Result<Option<Value>, SidecarError> {
        match self.transport.recv_control()? {
            Some(payload) => Ok(Some(wire::decode_control(&payload)?)),
            None => Ok(None),
        }
    }
}

/// Builds the real CloudWeb controller. Configuration comes from the
/// environment; the launch token is only passed into the child process.
#[cfg(windows)]
pub fn create_sidecar_controller(session_id: &str, profile: &str) -> Result<Controller, String> {
    let config = LaunchConfig::resolve(session_id, profile, &|key| std::env::var(key).ok())
        .map_err(|error| {
            format!(
                "cloudweb_sidecar_config_invalid:{} {}",
                error.code(),
                error.message
            )
        })?;
    Controller::new_custom(SidecarCloudWebController::new(config))
        .map_err(|error| error.to_string())
}

/// Non-Windows hosts have no named-pipe transport for the sidecar.
#[cfg(not(windows))]
pub fn create_sidecar_controller(_session_id: &str, _profile: &str) -> Result<Controller, String> {
    Err("cloudweb_sidecar_windows_only".to_string())
}

/// CloudWeb controller backed by a launched sidecar process.
#[cfg(windows)]
pub struct SidecarCloudWebController {
    config: LaunchConfig,
    client: Mutex<Option<SidecarClient<PipeTransport>>>,
    child: Mutex<Option<std::process::Child>>,
    /// Machine-readable reason of the last failed connect, surfaced through
    /// `get_info` so a rejected connection is not opaque to the UI.
    connect_error: Mutex<Option<SidecarError>>,
    connected: AtomicBool,
    uuid: String,
}

#[cfg(windows)]
impl SidecarCloudWebController {
    pub fn new(config: LaunchConfig) -> Self {
        let uuid = stable_uuid(&format!("sidecar:{}:{}", config.session_id, config.profile));
        Self {
            config,
            client: Mutex::new(None),
            child: Mutex::new(None),
            connect_error: Mutex::new(None),
            connected: AtomicBool::new(false),
            uuid,
        }
    }

    /// Records why the session could not be established.
    fn set_connect_error(&self, error: &SidecarError) {
        *self
            .connect_error
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(error.clone());
    }

    fn take_connect_error(&self) -> Option<SidecarError> {
        self.connect_error
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    /// Launches the sidecar, waits for its readiness line and connects both
    /// pipes. Every failure path leaves no child process behind.
    fn launch(&self) -> Result<SidecarClient<PipeTransport>, SidecarError> {
        use std::io::BufRead;

        let mut command = Command::new(&self.config.exec);
        command
            .args(self.config.arguments())
            .env(TOKEN_ENV, self.config.token())
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped());
        let mut child = command.spawn().map_err(|error| {
            SidecarError::local("cloudweb_sidecar_spawn_failed", error.to_string())
        })?;

        let stdout = child.stdout.take();
        let stderr = child.stderr.take();
        let (Some(stdout), Some(stderr)) = (stdout, stderr) else {
            reap(&mut child);
            return Err(SidecarError::local(
                "cloudweb_sidecar_pipes",
                "the sidecar stdout and stderr must be piped",
            ));
        };

        // Register the child before anything can fail, so the cleanup path
        // always owns it.
        *self.lock_child() = Some(child);

        let (ready_tx, ready_rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let mut lines = std::io::BufReader::new(stdout).lines();
            let mut first = true;
            while let Some(Ok(line)) = lines.next() {
                if first {
                    first = false;
                    let _ = ready_tx.send(line);
                } else {
                    log::debug!(target: "cloudweb", "sidecar stdout: {line}");
                }
            }
        });
        std::thread::spawn(move || {
            for line in std::io::BufReader::new(stderr)
                .lines()
                .map_while(Result::ok)
            {
                log::debug!(target: "cloudweb", "sidecar stderr: {line}");
            }
        });

        let line = match ready_rx.recv_timeout(READINESS_TIMEOUT) {
            Ok(line) => line,
            Err(_) => {
                return Err(self.terminate_after(
                    "cloudweb_sidecar_not_ready",
                    "the sidecar never reported readiness",
                ));
            }
        };
        if !line.starts_with("cloudweb-sidecar ready") {
            log::warn!(target: "cloudweb", "sidecar exited early: {line}");
            return Err(self.terminate_after(
                "cloudweb_sidecar_startup_failed",
                "the sidecar did not report readiness",
            ));
        }
        if let Err(error) = self.verify_ready_line(&line) {
            let SidecarError { code, message } = error;
            return Err(self.terminate_after(&code, message));
        }

        match PipeTransport::connect(&self.config) {
            Ok(transport) => Ok(SidecarClient::new(&self.config, transport)),
            Err(error) => {
                let SidecarError { code, message } = error;
                Err(self.terminate_after(&code, message))
            }
        }
    }

    /// The child reports the effective identity; refuse to continue when the
    /// readiness line disagrees with the launch request.
    fn verify_ready_line(&self, line: &str) -> Result<(), SidecarError> {
        let mut reported_session = None;
        let mut reported_epoch = None;
        for token in line.split_whitespace() {
            let Some((key, value)) = token.split_once('=') else {
                continue;
            };
            match key {
                "session_id" => reported_session = Some(value),
                "backend_epoch" => reported_epoch = Some(value),
                _ => {}
            }
        }
        if reported_session != Some(self.config.session_id.as_str())
            || reported_epoch != Some(self.config.backend_epoch.as_str())
        {
            return Err(SidecarError::local(
                "cloudweb_sidecar_identity_mismatch",
                "the sidecar session identity does not match the launch request",
            ));
        }
        Ok(())
    }

    fn lock_child(&self) -> std::sync::MutexGuard<'_, Option<std::process::Child>> {
        self.child
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Kills and reaps the registered child. Deterministic回收 path only: a
    /// host that dies before this runs leaves the sidecar to its own idle
    /// timeout, because job-object / parent-death recovery is CW-12 and is not
    /// implemented here.
    fn terminate_child(&self) {
        let mut slot = self.lock_child();
        let Some(child) = slot.as_mut() else {
            return;
        };
        reap(child);
        *slot = None;
    }

    fn terminate_after(&self, code: &str, message: impl Into<String>) -> SidecarError {
        self.terminate_child();
        SidecarError::local(code, message)
    }

    /// Test helper: connect and report the child pid, so an integration test can
    /// prove the process is reaped after the controller is dropped.
    #[cfg(all(windows, test))]
    pub fn launch_for_test(&self) -> Result<u32, String> {
        use maa_framework::custom_controller::CustomControllerCallback;
        if !CustomControllerCallback::connect(self) {
            let reason = self
                .take_connect_error()
                .map(|error| format!("{} {}", error.code(), error.message))
                .unwrap_or_else(|| "the sidecar session could not be established".to_string());
            return Err(reason);
        }
        let slot = self.lock_child();
        slot.as_ref()
            .map(std::process::Child::id)
            .ok_or_else(|| "no sidecar child is registered".to_string())
    }

    fn with_client<R>(
        &self,
        what: &str,
        action: impl FnOnce(&mut SidecarClient<PipeTransport>) -> Result<R, SidecarError>,
    ) -> Result<R, SidecarError> {
        let mut guard = self
            .client
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let Some(client) = guard.as_mut() else {
            return Err(SidecarError::local(
                "cloudweb_not_connected",
                format!("the CloudWeb session is not connected; cannot run {what}"),
            ));
        };
        match action(client) {
            Ok(value) => Ok(value),
            Err(error) => {
                log::warn!(
                    target: "cloudweb",
                    "CloudWeb {what} failed: {} {}",
                    error.code(),
                    error.message
                );
                Err(error)
            }
        }
    }
}

/// Waits briefly for a graceful exit, then kills and reaps.
#[cfg(windows)]
fn reap(child: &mut std::process::Child) {
    let deadline = Instant::now() + CLOSE_GRACE;
    loop {
        match child.try_wait() {
            Ok(Some(status)) => {
                log::debug!(
                    target: "cloudweb",
                    "CloudWeb sidecar exited with {status}"
                );
                return;
            }
            Ok(None) => {}
            Err(error) => {
                log::debug!(target: "cloudweb", "sidecar try_wait failed: {error}");
            }
        }
        if Instant::now() >= deadline {
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    if let Err(error) = child.kill() {
        log::warn!(target: "cloudweb", "failed to kill the CloudWeb sidecar: {error}");
    }
    if let Err(error) = child.wait() {
        log::warn!(target: "cloudweb", "failed to reap the CloudWeb sidecar: {error}");
    }
}

#[cfg(windows)]
impl CustomControllerCallback for SidecarCloudWebController {
    fn connect(&self) -> bool {
        let mut client = match self.launch() {
            Ok(client) => client,
            Err(error) => {
                log::error!(
                    target: "cloudweb",
                    "CloudWeb sidecar launch failed: {} {}",
                    error.code(),
                    error.message
                );
                self.set_connect_error(&error);
                return false;
            }
        };
        match client.handshake() {
            Ok(info) => log::info!(
                target: "cloudweb",
                "CloudWeb sidecar handshaked: session_id={} generation={} state={} backend={} lease_ms={}",
                client.session_id(),
                info.generation,
                info.state,
                info.backend_version,
                info.lease_ms
            ),
            Err(error) => {
                log::error!(
                    target: "cloudweb",
                    "CloudWeb handshake failed: {} {}",
                    error.code(),
                    error.message
                );
                self.set_connect_error(&error);
                drop(client);
                self.terminate_child();
                return false;
            }
        }
        // Ownership is taken lazily by the first input: `acquire_owner` requires
        // the sidecar to be Ready, and a CloudWeb session can legitimately still
        // be in BrowserReady / NeedsLogin / Queueing when the host connects.
        // Frames and health are available without ownership, so probing one here
        // reports the real session state without pretending readiness.
        match client.capture() {
            Ok(frame) => log::info!(
                target: "cloudweb",
                "CloudWeb first observation ready: session_id={} frame_id={} geometry_revision={}",
                client.session_id(),
                frame.frame_id,
                frame.geometry_revision
            ),
            Err(error) => log::info!(
                target: "cloudweb",
                "CloudWeb session connected without a ready frame: {} {}",
                error.code(),
                error.message
            ),
        }
        *self
            .connect_error
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = None;
        *self
            .client
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(client);
        self.connected.store(true, AtomicOrdering::SeqCst);
        true
    }

    fn connected(&self) -> bool {
        if !self.connected.load(AtomicOrdering::SeqCst) {
            return false;
        }
        // A faulted or closed session is not connected: report it so the
        // framework reconnects instead of pushing input into a dead session.
        let guard = self
            .client
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        match guard.as_ref() {
            Some(client) => !(client.is_faulted() || client.is_closed()),
            None => false,
        }
    }

    fn request_uuid(&self) -> Option<String> {
        Some(self.uuid.clone())
    }

    fn get_features(&self) -> ControllerFeature {
        ControllerFeature::USE_MOUSE_DOWN_UP_INSTEAD_OF_CLICK
            | ControllerFeature::USE_KEY_DOWN_UP_INSTEAD_OF_CLICK
    }

    fn screencap(&self) -> Option<Vec<u8>> {
        self.with_client("screencap", |client| client.capture())
            .ok()
            .map(|frame| frame.bytes)
    }

    fn click(&self, x: i32, y: i32) -> bool {
        self.with_client("click", |client| client.tap(x, y)).is_ok()
    }

    fn swipe(&self, x1: i32, y1: i32, x2: i32, y2: i32, duration: i32) -> bool {
        self.with_client("swipe", move |client| {
            client.swipe(x1, y1, x2, y2, duration)
        })
        .is_ok()
    }

    fn touch_down(&self, contact: i32, x: i32, y: i32, _pressure: i32) -> bool {
        self.with_client("touch_down", move |client| client.press(contact, x, y))
            .is_ok()
    }

    fn touch_move(&self, contact: i32, x: i32, y: i32, _pressure: i32) -> bool {
        if contact == 0 && x == 0 && y == 0 {
            // The framework emits a contact-0 move to (0, 0) before scroll. It
            // is not a user action, so the host records it and sends nothing.
            log::trace!(target: "cloudweb", "CloudWeb pre-scroll move recorded, not dispatched");
            return true;
        }
        self.with_client("touch_move", move |client| client.move_to(x, y))
            .is_ok()
    }

    fn touch_up(&self, contact: i32) -> bool {
        self.with_client("touch_up", move |client| client.release_button(contact))
            .is_ok()
    }

    fn click_key(&self, keycode: i32) -> bool {
        self.with_client("click_key", move |client| client.click_key(keycode))
            .is_ok()
    }

    fn input_text(&self, text: &str) -> bool {
        let text = text.to_string();
        self.with_client("input_text", move |client| client.input_text(&text))
            .is_ok()
    }

    fn key_down(&self, keycode: i32) -> bool {
        self.with_client("key_down", move |client| client.key_down(keycode))
            .is_ok()
    }

    fn key_up(&self, keycode: i32) -> bool {
        self.with_client("key_up", move |client| client.key_up(keycode))
            .is_ok()
    }

    fn scroll(&self, dx: i32, dy: i32) -> bool {
        self.with_client("scroll", move |client| client.scroll(dx, dy))
            .is_ok()
    }

    fn relative_move(&self, dx: i32, dy: i32) -> bool {
        self.with_client("relative_move", move |client| client.look(dx, dy))
            .is_ok()
    }

    fn inactive(&self) -> bool {
        // Give up ownership so the sidecar fences the generation and releases
        // held inputs. An unconfirmed release is reported as a failure: the
        // remote held state is then unknown and only the ledger is authoritative.
        self.with_client("release_owner", |client| client.release_owner())
            .is_ok()
    }

    fn get_info(&self) -> String {
        let guard = self
            .client
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        match guard.as_ref() {
            Some(client) => client.diagnostics(),
            None => {
                let error = self.take_connect_error();
                json!({
                    "backend": "cloud_web",
                    "implementation": "sidecar",
                    "protocol_version": wire::PROTOCOL_MAJOR,
                    "session_id": self.config.session_id,
                    "profile": self.config.profile,
                    "connected": false,
                    "connect_error": error.map(|error| {
                        json!({ "code": error.code, "message": error.message })
                    }),
                })
            }
        }
        .to_string()
    }
}

#[cfg(windows)]
impl Drop for SidecarCloudWebController {
    fn drop(&mut self) {
        // Ask the sidecar to close first so it stops itself, then make sure the
        // child is reaped.
        let client = self
            .client
            .get_mut()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
        if let Some(mut client) = client {
            if let Err(error) = client.close() {
                log::debug!(
                    target: "cloudweb",
                    "CloudWeb close was not acknowledged: {}",
                    error.code()
                );
            }
        }
        let mut slot = self
            .child
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(child) = slot.as_mut() {
            reap(child);
        }
        *slot = None;
    }
}

/// Windows named-pipe transport. Each call drives a private current-thread
/// runtime because MaaFramework invokes controller callbacks on its own
/// threads.
#[cfg(windows)]
pub struct PipeTransport {
    runtime: tokio::runtime::Runtime,
    control: tokio::net::windows::named_pipe::NamedPipeClient,
    frame: tokio::net::windows::named_pipe::NamedPipeClient,
}

#[cfg(windows)]
impl PipeTransport {
    /// Opens both pipes, retrying while the sidecar is still arming instances.
    pub fn connect(config: &LaunchConfig) -> Result<Self, SidecarError> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|error| SidecarError::local("cloudweb_runtime_failed", error.to_string()))?;
        let control = runtime
            .block_on(open_pipe(&config.control_pipe))
            .map_err(|error| SidecarError::new("cloudweb_control_pipe_failed", error))?;
        let frame = runtime
            .block_on(open_pipe(&config.frame_pipe))
            .map_err(|error| SidecarError::new("cloudweb_frame_pipe_failed", error))?;
        Ok(Self {
            runtime,
            control,
            frame,
        })
    }

    /// `Runtime::block_on` panics inside another runtime, so the callback
    /// threads are checked instead.
    fn guard_context() -> Result<(), SidecarError> {
        if tokio::runtime::Handle::try_current().is_ok() {
            return Err(SidecarError::local(
                "cloudweb_blocking_context",
                "the sidecar transport cannot block inside an async runtime",
            ));
        }
        Ok(())
    }
}

#[cfg(windows)]
impl Transport for PipeTransport {
    fn send_control(&mut self, frame: &[u8]) -> Result<(), SidecarError> {
        Self::guard_context()?;
        use tokio::io::AsyncWriteExt;
        let payload = frame.to_vec();
        let PipeTransport {
            runtime, control, ..
        } = self;
        runtime
            .block_on(async {
                // The timeout future must be created inside the runtime context.
                tokio::time::timeout(CONTROL_TIMEOUT, async {
                    control.write_all(&payload).await?;
                    control.flush().await
                })
                .await
            })
            .map_err(|_| {
                SidecarError::local("cloudweb_control_write_timeout", "control write timed out")
            })?
            .map_err(|error| SidecarError::new("cloudweb_control_write", error.to_string()))
    }

    fn recv_control(&mut self) -> Result<Option<Vec<u8>>, SidecarError> {
        Self::guard_context()?;
        use tokio::io::AsyncReadExt;
        let PipeTransport {
            runtime, control, ..
        } = self;
        let read = async {
            let mut length = [0u8; 4];
            match control.read_exact(&mut length).await {
                Ok(_) => {}
                Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
                Err(error) => return Err(error.to_string()),
            }
            let length = u32::from_le_bytes(length) as usize;
            if length == 0 || length > wire::MAX_CONTROL_BYTES {
                return Err(format!("control frame of {length} bytes is out of range"));
            }
            let mut payload = vec![0u8; length];
            control
                .read_exact(&mut payload)
                .await
                .map_err(|error| error.to_string())?;
            Ok(Some(payload))
        };
        runtime
            .block_on(async { tokio::time::timeout(CONTROL_TIMEOUT, read).await })
            .map_err(|_| {
                SidecarError::local("cloudweb_control_read_timeout", "control read timed out")
            })?
            .map_err(|error| SidecarError::new("cloudweb_control_read", error))
    }

    fn recv_frame(&mut self, timeout: Duration) -> Result<Option<RawFrame>, SidecarError> {
        Self::guard_context()?;
        use tokio::io::AsyncReadExt;
        let PipeTransport { runtime, frame, .. } = self;
        let read = async {
            let mut length = [0u8; 4];
            match frame.read_exact(&mut length).await {
                Ok(_) => {}
                Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
                Err(error) => return Err(error.to_string()),
            }
            let header_length = u32::from_le_bytes(length) as usize;
            if header_length != wire::FRAME_HEADER_BYTES {
                return Err(format!("unexpected frame header length {header_length}"));
            }
            let mut header = [0u8; wire::FRAME_HEADER_BYTES];
            frame
                .read_exact(&mut header)
                .await
                .map_err(|error| error.to_string())?;
            let mut payload_length = [0u8; 4];
            frame
                .read_exact(&mut payload_length)
                .await
                .map_err(|error| error.to_string())?;
            let payload_length = u32::from_le_bytes(payload_length) as usize;
            if payload_length == 0 || payload_length > wire::MAX_FRAME_BYTES {
                return Err(format!("invalid frame payload length {payload_length}"));
            }
            let mut bytes = vec![0u8; payload_length];
            frame
                .read_exact(&mut bytes)
                .await
                .map_err(|error| error.to_string())?;
            Ok(Some(RawFrame { header, bytes }))
        };
        runtime
            .block_on(async { tokio::time::timeout(timeout, read).await })
            .map_err(|_| {
                SidecarError::local("cloudweb_frame_read_timeout", "frame read timed out")
            })?
            .map_err(|error| SidecarError::new("cloudweb_frame_read", error))
    }
}

/// Opens a pipe client, retrying while the sidecar is still arming instances.
/// A tokio named-pipe client is connected by `open` itself, so an unavailable
/// pipe surfaces as `NotFound` or `ERROR_PIPE_BUSY` (231) and is retried until
/// the readiness deadline.
#[cfg(windows)]
async fn open_pipe(name: &str) -> Result<tokio::net::windows::named_pipe::NamedPipeClient, String> {
    use tokio::net::windows::named_pipe::ClientOptions;
    let deadline = Instant::now() + READINESS_TIMEOUT;
    loop {
        match ClientOptions::new().open(name) {
            Ok(client) => return Ok(client),
            Err(error)
                if (error.kind() == std::io::ErrorKind::NotFound
                    || error.raw_os_error() == Some(231))
                    && Instant::now() < deadline =>
            {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            Err(error) => return Err(format!("open pipe: {error}")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;

    const TEST_EPOCH: &str = "epoch-test";
    const ALL_CAPS: [&str; 5] = HOST_CAPABILITIES;

    /// Scripted control/frame plane used instead of real pipes.
    struct Scripted {
        responses: VecDeque<Value>,
        frames: VecDeque<RawFrame>,
        sent: Vec<Value>,
        /// Number of sent frames after which the control pipe reports EOF.
        close_after: Option<usize>,
    }

    impl Scripted {
        fn new(responses: Vec<Value>) -> Self {
            Self {
                responses: responses.into_iter().collect(),
                frames: VecDeque::new(),
                sent: Vec::new(),
                close_after: None,
            }
        }

        fn push(&mut self, values: Vec<Value>) {
            self.responses.extend(values);
        }

        /// Commands only: the hello frame carries no `op`.
        fn commands(&self) -> Vec<&Value> {
            self.sent
                .iter()
                .filter(|frame| frame["op"].is_string())
                .collect()
        }

        fn ops(&self) -> Vec<&str> {
            self.commands()
                .iter()
                .map(|frame| frame["op"].as_str().expect("op"))
                .collect()
        }

        fn cmd(&self, index: usize) -> &Value {
            self.commands()[index]
        }
    }

    /// Convenience accessors so tests read as protocol behaviour.
    impl SidecarClient<Scripted> {
        fn push(&mut self, values: Vec<Value>) {
            self.transport.push(values);
        }

        fn ops(&self) -> Vec<&str> {
            self.transport.ops()
        }

        fn cmd(&self, index: usize) -> &Value {
            self.transport.cmd(index)
        }

        fn commands_only(&self) -> Vec<&Value> {
            self.transport.commands()
        }
    }

    impl Transport for Scripted {
        fn send_control(&mut self, frame: &[u8]) -> Result<(), SidecarError> {
            let payload = wire::decode_control(&frame[4..])?;
            self.sent.push(payload);
            let commands = self
                .sent
                .iter()
                .filter(|frame| frame["op"].is_string())
                .count();
            if self.close_after == Some(commands) {
                self.responses.clear();
                self.frames.clear();
            }
            Ok(())
        }

        fn recv_control(&mut self) -> Result<Option<Vec<u8>>, SidecarError> {
            match self.responses.pop_front() {
                Some(value) => {
                    Ok(Some(serde_json::to_vec(&value).map_err(|error| {
                        SidecarError::local("scripted", error.to_string())
                    })?))
                }
                None => Ok(None),
            }
        }

        fn recv_frame(&mut self, _timeout: Duration) -> Result<Option<RawFrame>, SidecarError> {
            Ok(self.frames.pop_front())
        }
    }

    fn frame_packet(frame_id: u64, generation: u64, freshness: u8) -> RawFrame {
        let mut header = [0u8; wire::FRAME_HEADER_BYTES];
        header[0..4].copy_from_slice(&wire::FRAME_MAGIC);
        header[4] = wire::FRAME_VERSION;
        header[5] = freshness;
        header[8..16].copy_from_slice(&frame_id.to_le_bytes());
        header[16..24].copy_from_slice(&generation.to_le_bytes());
        header[24..26].copy_from_slice(&wire::CANONICAL_WIDTH.to_le_bytes());
        header[26..28].copy_from_slice(&wire::CANONICAL_HEIGHT.to_le_bytes());
        RawFrame {
            header,
            bytes: b"fake-png".to_vec(),
        }
    }

    fn hello_ok(generation: u64, capabilities: &[&str]) -> Value {
        json!({
            "type": "hello_ok",
            "protocol_major": 1,
            "protocol_minor": 0,
            "backend_version": "0.1.0",
            "backend_epoch": TEST_EPOCH,
            "generation": generation,
            "state": "ready",
            "capabilities": capabilities,
            "limits": {
                "lease_ms": 350,
                // Renewals are driven explicitly in tests so a scheduling delay
                // cannot consume a scripted response.
                "renew_ms": 100_000,
                "max_pending": 64,
                "max_frame_bytes": 16777216,
                "ack_ttl_ms": 30_000,
                "max_ack_entries": 1024,
                "observation_max_age_ms": 1000
            }
        })
    }

    fn receipt(seq: usize, generation: u64, detail: Option<Value>) -> Value {
        json!({
            "type": "receipt", "seq": seq, "status": "accepted",
            "generation": generation, "detail": detail
        })
    }

    fn dispatched(seq: usize, generation: u64) -> Value {
        json!({
            "type": "receipt", "seq": seq, "status": "dispatched",
            "generation": generation, "detail": null
        })
    }

    fn rejected(seq: usize, generation: u64, code: &str, message: &str) -> Value {
        json!({
            "type": "receipt", "seq": seq, "status": "rejected",
            "generation": generation, "error": code, "message": message
        })
    }

    fn unknown(seq: usize, generation: u64, code: &str, message: &str) -> Value {
        json!({
            "type": "receipt", "seq": seq, "status": "unknown",
            "generation": generation, "error": code, "message": message
        })
    }

    fn metadata(frame_id: u64, generation: u64, freshness: &str) -> Value {
        json!({
            "frame_id": frame_id,
            "session_id": "session-test",
            "generation": generation,
            "width": 1280,
            "height": 720,
            "capture_monotonic_ns": 10,
            "media": {"source": "fake", "decoded": 1, "presented": 1, "last_progress_monotonic_ns": 10},
            "freshness": freshness,
            "geometry_revision": 3,
            "layout": {
                "content_rect_css": [0, 0, 1280, 720],
                "viewport_css": [1280, 720],
                "dpr": 1.0,
                "frame_offsets": []
            }
        })
    }

    fn config() -> LaunchConfig {
        LaunchConfig {
            exec: PathBuf::from("cloudweb-sidecar.exe"),
            extra_args: vec!["--backend".to_string(), "fake".to_string()],
            session_id: "session-test".to_string(),
            profile: "default".to_string(),
            owner_id: "mxu:session-test".to_string(),
            backend_epoch: TEST_EPOCH.to_string(),
            control_pipe: r"\\.\pipe\ctl-test".to_string(),
            frame_pipe: r"\\.\pipe\frame-test".to_string(),
            token: "token-fixture".to_string(),
        }
    }

    /// A client that only completed the handshake (generation 1, no owner).
    fn handshaked(capabilities: &[&str]) -> SidecarClient<Scripted> {
        let mut client =
            SidecarClient::new(&config(), Scripted::new(vec![hello_ok(1, capabilities)]));
        client.handshake().expect("handshake completes");
        client
    }

    /// A client with handshake and ownership done (generation 2).
    fn owned(capabilities: &[&str]) -> SidecarClient<Scripted> {
        let mut client = handshaked(capabilities);
        client
            .transport
            .push(vec![receipt(1, 2, Some(json!({ "lease_ms": 350 })))]);
        client.acquire_owner().expect("owner acquired");
        client
    }

    /// A client that also holds a fresh observation of frame 7.
    fn observing(capabilities: &[&str]) -> SidecarClient<Scripted> {
        let mut client = owned(capabilities);
        client
            .transport
            .push(vec![receipt(2, 2, Some(metadata(7, 2, "fresh")))]);
        client.transport.frames.push_back(frame_packet(7, 2, 0));
        client.capture().expect("capture");
        client
    }

    #[test]
    fn handshake_records_identity_limits_and_capabilities() {
        let client = handshaked(&ALL_CAPS);
        assert_eq!(client.generation, 1);
        assert_eq!(client.lease_ms, 350);
        assert_eq!(client.observation_max_age_ms, 1000);
        assert!(client.has_capability("geometry.v1"));
        assert_eq!(client.state, "ready");
        let hello = &client.transport.sent[0];
        assert_eq!(hello["type"], "hello");
        assert_eq!(hello["protocol_major"], 1);
        assert_eq!(hello["host"]["name"], "MXU");
        assert_eq!(hello["ui_profile"], "cloud_web.default");
        assert_eq!(hello["token"], "token-fixture");
    }

    #[test]
    fn handshake_is_not_repeated_and_identity_is_namespaced_per_backend() {
        let mut client = handshaked(&ALL_CAPS);
        assert_eq!(
            client.handshake().unwrap_err().code(),
            "cloudweb_already_handshaked"
        );
        let sidecar_uuid = stable_uuid(&format!("sidecar:{}:default", client.session_id()));
        assert_ne!(sidecar_uuid, stable_uuid("fake:session-test:default"));
        assert_eq!(sidecar_uuid.len(), 36);
    }

    #[test]
    fn handshake_fails_loudly_on_auth_protocol_and_epoch_mismatch() {
        let auth = Scripted::new(vec![json!({
            "type": "error", "code": "auth_failed", "message": "launch token mismatch"
        })]);
        let mut client = SidecarClient::new(&config(), auth);
        let error = client.handshake().unwrap_err();
        assert_eq!(error.code(), "auth_failed");
        assert!(client.is_faulted());

        let mut wrong_major = hello_ok(1, &ALL_CAPS);
        wrong_major["protocol_major"] = json!(2);
        let mut client = SidecarClient::new(&config(), Scripted::new(vec![wrong_major]));
        assert_eq!(
            client.handshake().unwrap_err().code(),
            "cloudweb_protocol_mismatch"
        );

        let mut wrong_epoch = hello_ok(1, &ALL_CAPS);
        wrong_epoch["backend_epoch"] = json!("someone-elses-epoch");
        let mut client = SidecarClient::new(&config(), Scripted::new(vec![wrong_epoch]));
        assert_eq!(
            client.handshake().unwrap_err().code(),
            "cloudweb_epoch_mismatch"
        );

        let mut not_ready = hello_ok(1, &ALL_CAPS);
        not_ready["state"] = json!("faulted");
        let mut client = SidecarClient::new(&config(), Scripted::new(vec![not_ready]));
        assert_eq!(
            client.handshake().unwrap_err().code(),
            "cloudweb_sidecar_not_ready"
        );
        assert!(client.is_faulted());
    }

    #[test]
    fn owner_conflict_is_surfaced_and_the_lease_is_dropped() {
        let mut client = owned(&ALL_CAPS);
        client.renew_ms = 0;
        client.transport.push(vec![rejected(
            2,
            2,
            "owner_conflict",
            "owner x still holds the lease",
        )]);
        let error = client.renew_lease().unwrap_err();
        assert_eq!(error.code(), "owner_conflict");
        assert!(client.owner_renewed_at.is_none());
        // A conflicting owner counts as a lease loss: the host no longer holds
        // the generation it acquired.
        assert_eq!(client.lease_losses, 1);

        // The next acquisition starts a new generation.
        client
            .transport
            .push(vec![receipt(3, 3, Some(json!({ "lease_ms": 350 })))]);
        client.acquire_owner().expect("a new owner was acquired");
        assert_eq!(client.generation, 3);
        assert_eq!(
            client.ops(),
            ["acquire_owner", "renew_lease", "acquire_owner"]
        );
    }

    #[test]
    fn stale_generation_is_resynchronised_before_the_next_command() {
        let mut client = owned(&ALL_CAPS);
        // Force the host view behind the sidecar and let it answer with the
        // current generation; the client adopts it instead of replaying.
        client.generation = 7;
        client.renew_ms = 0;
        client.transport.push(vec![
            rejected(
                2,
                11,
                "generation_stale",
                "command generation 7 != current 11",
            ),
            receipt(3, 12, Some(json!({ "lease_ms": 350 }))),
        ]);
        client
            .ensure_lease()
            .expect("recovered with a new generation");
        assert_eq!(client.generation, 12);
        assert_eq!(
            client.ops(),
            ["acquire_owner", "renew_lease", "acquire_owner"]
        );
        assert_eq!(client.commands, 3);
    }

    #[test]
    fn input_mapping_sends_the_expected_ops_and_observation() {
        let mut client = observing(&ALL_CAPS);
        client.transport.push(vec![
            dispatched(3, 2),
            dispatched(4, 2),
            dispatched(5, 2),
            dispatched(6, 2),
            dispatched(7, 2),
            dispatched(8, 2),
        ]);
        client.tap(640, 360).expect("tap");
        client.key_down(65).expect("key down");
        client.key_up(65).expect("key up");
        client.scroll(0, -300).expect("scroll");
        client.input_text("endfield").expect("text");

        assert_eq!(
            client.ops(),
            vec![
                "acquire_owner",
                "get_latest_frame",
                "mouse_down",
                "mouse_up",
                "key_down",
                "key_up",
                "wheel",
                "insert_text"
            ]
        );
        let down = client.cmd(2);
        assert_eq!(down["params"]["x"], 640);
        assert_eq!(down["params"]["y"], 360);
        assert_eq!(down["params"]["button"], 0);
        assert_eq!(down["based_on_frame_id"], 7);
        assert_eq!(down["geometry_revision"], 3);
        assert!(down["based_on_age_ms"].as_u64().expect("age") < 1000);
        assert_eq!(down["session_id"], "session-test");
        assert_eq!(down["backend_epoch"], TEST_EPOCH);
        assert_eq!(down["generation"], 2);
        assert_eq!(client.cmd(3)["params"]["button"], 0);
        assert_eq!(client.cmd(4)["params"]["key"], 65);
        assert_eq!(client.cmd(6)["params"]["dy"], -300);
        assert_eq!(client.cmd(7)["params"]["text"], "endfield");
        assert_eq!(client.commands, 8);
    }

    #[test]
    fn touch_and_click_key_paths_map_to_sidecar_ops() {
        let mut client = observing(&ALL_CAPS);
        client.transport.push(vec![
            dispatched(3, 2),
            dispatched(4, 2),
            dispatched(5, 2),
            dispatched(6, 2),
            dispatched(7, 2),
        ]);
        client.press(0, 12, 34).expect("press");
        client.move_to(56, 78).expect("move");
        client.release_button(0).expect("release");
        client.click_key(65).expect("click key");
        assert_eq!(
            client.ops()[2..],
            vec![
                "mouse_down",
                "move_absolute",
                "mouse_up",
                "key_down",
                "key_up"
            ][..]
        );
        assert_eq!(client.cmd(3)["params"]["x"], 56);
        assert_eq!(client.cmd(3)["params"]["y"], 78);
    }

    #[test]
    fn swipe_uses_a_full_down_move_up_path() {
        let mut client = observing(&ALL_CAPS);
        client
            .transport
            .push(vec![dispatched(3, 2), dispatched(4, 2), dispatched(5, 2)]);
        client.swipe(100, 100, 200, 200, 300).expect("swipe");
        assert_eq!(
            client.ops()[2..],
            ["mouse_down", "move_absolute", "mouse_up"]
        );
        assert_eq!(client.cmd(3)["params"]["x"], 200);
        assert_eq!(client.cmd(3)["based_on_frame_id"], 7);
        assert_eq!(client.cmd(4)["params"]["button"], 0);
    }

    #[test]
    fn absolute_input_without_an_observation_is_refused_not_faked() {
        let mut client = owned(&ALL_CAPS);
        let error = client.tap(10, 20).unwrap_err();
        assert_eq!(error.code(), "cloudweb_no_observation");
        // Nothing was sent for the refused action.
        assert_eq!(client.ops(), ["acquire_owner"]);
    }

    #[test]
    fn expired_observation_is_refused_before_sending() {
        let mut client = observing(&ALL_CAPS);
        // Age the stored observation past the limit instead of relying on a
        // zero-millisecond window.
        let frame = client.observation.as_mut().expect("observation");
        frame.captured_at = frame
            .captured_at
            .checked_sub(Duration::from_secs(5))
            .expect("a past instant");
        let error = client.key_down(65).unwrap_err();
        assert_eq!(error.code(), "cloudweb_observation_expired");
        assert_eq!(client.ops(), ["acquire_owner", "get_latest_frame"]);
    }

    #[test]
    fn frames_run_without_ownership_and_the_first_input_acquires_it() {
        // Cold-start order used by the real sidecar: hello reports generation 0
        // in BrowserReady, a frame makes it Ready, and only input needs an owner.
        // Mirror the real process: hello_ok reports generation 0 in BrowserReady.
        let mut client = SidecarClient::new(&config(), Scripted::new(vec![hello_ok(0, &ALL_CAPS)]));
        client.handshake().expect("handshake at generation 0");
        assert_eq!(client.generation, 0);
        client.push(vec![receipt(1, 0, Some(metadata(7, 0, "fresh")))]);
        client.transport.frames.push_back(frame_packet(7, 0, 0));
        let frame = client.capture().expect("a frame before ownership");
        assert_eq!(frame.frame_id, 7);
        // The capture did not acquire or renew a lease.
        assert_eq!(client.ops(), ["get_latest_frame"]);
        assert!(!client.diagnostics()["owner_lease"]["held"]
            .as_bool()
            .unwrap());

        client.push(vec![
            receipt(2, 1, Some(json!({ "lease_ms": 350 }))),
            dispatched(3, 1),
        ]);
        client
            .key_down(65)
            .expect("the first input acquires ownership");
        assert_eq!(
            client.ops(),
            ["get_latest_frame", "acquire_owner", "key_down"]
        );
        assert_eq!(client.generation, 1);
    }

    #[test]
    fn a_release_still_works_without_an_observation() {
        let mut client = owned(&ALL_CAPS);
        client.transport.push(vec![receipt(2, 2, None)]);
        // No capture happened, so the safety path must not be blocked by it.
        client
            .release_button(0)
            .expect("release without observation");
        assert_eq!(client.ops(), ["acquire_owner", "mouse_up"]);
        assert!(client
            .commands_only()
            .iter()
            .all(|frame| frame.get("based_on_frame_id").is_none()));
    }

    #[test]
    fn frame_stale_rejection_gets_one_refresh_then_retry() {
        let mut client = observing(&ALL_CAPS);
        client.transport.push(vec![
            rejected(3, 2, "frame_stale", "latest frame is not fresh"),
            receipt(4, 2, Some(metadata(8, 2, "fresh"))),
            dispatched(5, 2),
        ]);
        client.transport.frames.push_back(frame_packet(8, 2, 0));

        client.key_down(65).expect("key down after one refresh");
        assert_eq!(
            client.ops()[2..],
            ["key_down", "get_latest_frame", "key_down"][..]
        );
        assert_eq!(client.cmd(4)["based_on_frame_id"], 8);
        assert_eq!(client.cmd(4)["params"]["key"], 65);
    }

    #[test]
    fn unknown_receipt_is_never_retried() {
        let mut client = observing(&ALL_CAPS);
        client
            .transport
            .push(vec![unknown(3, 2, "input_unknown", "cdp dispatch failed")]);
        let error = client.key_down(65).unwrap_err();
        assert_eq!(error.code(), "input_unknown");
        let key_downs = client.ops().iter().filter(|op| **op == "key_down").count();
        assert_eq!(key_downs, 1, "an unknown result must not be replayed");
    }

    #[test]
    fn capture_rejects_non_canonical_stale_and_unpaired_frames() {
        // The client is `owned`, so captures start at seq 2.
        let mut client = owned(&ALL_CAPS);

        // Wrong size.
        let mut small = metadata(7, 2, "fresh");
        small["width"] = json!(640);
        client.push(vec![receipt(2, 2, Some(small))]);
        assert_eq!(
            client.capture().unwrap_err().code(),
            "cloudweb_frame_not_canonical"
        );

        // Not fresh, and freshness unknown is refused as well.
        client.push(vec![
            receipt(3, 2, Some(metadata(8, 2, "stale"))),
            receipt(4, 2, Some(metadata(8, 2, "unknown"))),
        ]);
        assert_eq!(
            client.capture().unwrap_err().code(),
            "cloudweb_frame_not_fresh"
        );
        assert_eq!(
            client.capture().unwrap_err().code(),
            "cloudweb_frame_not_fresh"
        );

        // Fresh metadata but the frame plane has nothing to pair with it.
        client.push(vec![receipt(5, 2, Some(metadata(9, 2, "fresh")))]);
        assert_eq!(
            client.capture().unwrap_err().code(),
            "cloudweb_frame_closed"
        );

        // A packet carrying a different frame id must not be passed off as new.
        client.push(vec![receipt(6, 2, Some(metadata(10, 2, "fresh")))]);
        client.transport.frames.push_back(frame_packet(99, 2, 0));
        assert_eq!(
            client.capture().unwrap_err().code(),
            "cloudweb_frame_desynced"
        );

        // A packet from an older generation.
        client.push(vec![receipt(7, 2, Some(metadata(11, 2, "fresh")))]);
        client.transport.frames.push_back(frame_packet(11, 1, 0));
        assert_eq!(
            client.capture().unwrap_err().code(),
            "cloudweb_frame_generation"
        );

        // A stale header tag on the matching packet.
        client.push(vec![receipt(8, 2, Some(metadata(12, 2, "fresh")))]);
        client
            .transport
            .frames
            .push_back(frame_packet(12, 2, 1 /* stale */));
        assert_eq!(
            client.capture().unwrap_err().code(),
            "cloudweb_frame_not_fresh"
        );

        // An older packet is skipped for the matching one.
        client.push(vec![receipt(9, 2, Some(metadata(13, 2, "fresh")))]);
        client.transport.frames.push_back(frame_packet(12, 2, 0));
        client.transport.frames.push_back(frame_packet(13, 2, 0));
        let frame = client.capture().expect("matching frame after a skip");
        assert_eq!(frame.frame_id, 13);
        assert_eq!(frame.bytes, b"fake-png".to_vec());
    }

    #[test]
    fn unsupported_capabilities_return_an_explicit_error() {
        let mut client = handshaked(&["frame.latest", "input.keyboard", "geometry.v1"]);
        client
            .transport
            .push(vec![receipt(1, 2, Some(json!({ "lease_ms": 350 })))]);
        client.acquire_owner().expect("owner");
        client
            .transport
            .push(vec![receipt(2, 2, Some(metadata(7, 2, "fresh")))]);
        client.transport.frames.push_back(frame_packet(7, 2, 0));
        client.capture().expect("capture");
        let before = client.transport.sent.len();

        // The declared set has neither input.relative nor input.text.
        assert_eq!(
            client.look(10, 10).unwrap_err().code(),
            "cloudweb_capability_missing"
        );
        assert_eq!(
            client.input_text("hi").unwrap_err().code(),
            "cloudweb_capability_missing"
        );
        assert_eq!(client.transport.sent.len(), before);
    }

    #[test]
    fn multi_touch_is_refused() {
        let mut client = handshaked(&ALL_CAPS);
        assert_eq!(
            client.press(1, 10, 10).unwrap_err().code(),
            "cloudweb_unsupported_multitouch"
        );
        assert_eq!(
            client.release_button(2).unwrap_err().code(),
            "cloudweb_unsupported_multitouch"
        );
        // Only the hello frame was sent; no command reached the control plane.
        assert!(client.commands_only().is_empty());
    }

    #[test]
    fn lease_loss_during_a_slow_command_is_recorded_and_reacquired() {
        let mut client = owned(&ALL_CAPS);
        client.renew_ms = 0;
        client.transport.push(vec![
            rejected(2, 3, "lease_expired", "lease expired"),
            receipt(3, 4, Some(json!({ "lease_ms": 350 }))),
        ]);
        client
            .ensure_lease()
            .expect("a new generation is acquired after the lease loss");
        assert_eq!(client.generation, 4);
        assert_eq!(client.lease_losses, 1);
        // The host records the gap it observed rather than claiming the lease
        // held; a real p95 still needs a live session (N1 stays open).
        assert!(client.max_renew_gap_ms < 1000);
    }

    #[test]
    fn a_failed_lease_recovery_is_not_masked_as_success() {
        let mut client = owned(&ALL_CAPS);
        client.renew_ms = 0;
        client.transport.push(vec![
            rejected(2, 3, "lease_expired", "lease expired"),
            rejected(
                3,
                3,
                "site_not_ready",
                "cannot acquire owner in state faulted",
            ),
        ]);
        let error = client.ensure_lease().unwrap_err();
        assert_eq!(error.code(), "site_not_ready");
        assert_eq!(client.generation, 3);
    }

    #[test]
    fn close_reports_outcome_and_blocks_further_commands() {
        let mut client = owned(&ALL_CAPS);
        client.transport.push(vec![receipt(
            2,
            3,
            Some(json!({ "released": true, "old_generation": 2 })),
        )]);
        client.close().expect("close acknowledged");
        assert!(client.is_closed());
        assert_eq!(client.generation, 3);
        assert_eq!(client.state, "closed");

        let error = client.key_down(65).unwrap_err();
        assert_eq!(error.code(), "cloudweb_closed");
        // A second close is a no-op.
        client.close().expect("close is idempotent");
        assert_eq!(client.ops(), ["acquire_owner", "close"]);
    }

    #[test]
    fn a_faulted_session_still_allows_close_and_blocks_input() {
        let mut client = owned(&ALL_CAPS);
        client.transport.push(vec![unknown(
            2,
            3,
            "faulted",
            "CDP cannot confirm release_all",
        )]);
        let error = client.release_owner().unwrap_err();
        assert_eq!(error.code(), "faulted");
        assert!(client.is_faulted());

        assert_eq!(client.key_down(65).unwrap_err().code(), "cloudweb_faulted");
        // Only a close may leave Faulted, so it must stay reachable.
        client
            .transport
            .push(vec![receipt(3, 4, Some(json!({ "released": true })))]);
        client.close().expect("close on a faulted session");
        assert!(client.is_closed());
    }

    #[test]
    fn a_closed_control_pipe_is_reported_instead_of_a_fake_success() {
        let mut client = handshaked(&ALL_CAPS);
        client.transport.close_after = Some(1);
        let error = client.acquire_owner().unwrap_err();
        assert_eq!(error.code(), "cloudweb_control_closed");
        assert!(client.is_faulted());

        // EOF in the middle of a command is equally explicit.
        let mut client = owned(&ALL_CAPS);
        client.transport.close_after = Some(2);
        assert_eq!(
            client.renew_lease().unwrap_err().code(),
            "cloudweb_control_closed"
        );
    }

    #[test]
    fn receipt_shape_and_seq_are_validated() {
        let mut client = owned(&ALL_CAPS);
        client.transport.push(vec![json!({
            "type": "greeting", "seq": 2, "status": "accepted", "generation": 2
        })]);
        assert_eq!(
            client.renew_lease().unwrap_err().code(),
            "cloudweb_protocol_shape"
        );

        client.transport.push(vec![receipt(99, 2, None)]);
        assert_eq!(
            client.renew_lease().unwrap_err().code(),
            "cloudweb_seq_mismatch"
        );
    }

    #[test]
    fn launch_config_requires_an_executable_and_hides_the_token() {
        let missing = LaunchConfig::resolve("session-a", "default", &|_| None)
            .expect_err("no executable must fail");
        assert_eq!(missing.code(), "cloudweb_sidecar_exec_missing");

        let empty_session = LaunchConfig::resolve("  ", "default", &|_| {
            Some("cloudweb-sidecar.exe".to_string())
        })
        .expect_err("an empty session id must fail");
        assert_eq!(empty_session.code(), "cloudweb_session_required");

        let config = LaunchConfig::resolve("session-a", "default", &|key| match key {
            SIDECAR_PATH_ENV => Some("cloudweb-sidecar.exe".to_string()),
            SIDECAR_ARGS_ENV => Some("--backend fake".to_string()),
            _ => None,
        })
        .expect("resolved config");
        assert_eq!(config.extra_args, vec!["--backend", "fake"]);
        assert_eq!(config.owner_id, "mxu:session-a");
        assert!(config
            .control_pipe
            .starts_with(r"\\.\pipe\cloudweb-mxu-ctl-"));
        assert!(config
            .frame_pipe
            .starts_with(r"\\.\pipe\cloudweb-mxu-frame-"));
        assert_eq!(config.backend_epoch.len(), 32);
        assert_eq!(config.token().len(), 64);

        // The token travels only through the child environment.
        let arguments = config.arguments();
        assert!(!arguments.iter().any(|item| item == "--token"));
        assert!(!arguments.iter().any(|item| *item == config.token()));
        assert!(!format!("{config:?}").contains(config.token()));
        assert!(!config
            .arguments()
            .iter()
            .any(|item| item.contains(config.token())));
    }

    #[test]
    fn readiness_identity_is_verified_against_the_launch_request() {
        let matches = |line: &str, expected_session: &str, expected_epoch: &str| {
            let mut session = None;
            let mut epoch = None;
            for token in line.split_whitespace() {
                if let Some((key, value)) = token.split_once('=') {
                    match key {
                        "session_id" => session = Some(value),
                        "backend_epoch" => epoch = Some(value),
                        _ => {}
                    }
                }
            }
            session == Some(expected_session) && epoch == Some(expected_epoch)
        };
        let ready = "cloudweb-sidecar ready control_pipe=x frame_pipe=y \
                     session_id=session-a backend_epoch=epoch-a backend=Fake protocol=1.0";
        assert!(matches(ready, "session-a", "epoch-a"));
        assert!(!matches(ready, "other", "epoch-a"));
        assert!(!matches(ready, "session-a", "epoch-b"));
        assert!(!matches(
            "cloudweb-sidecar ready session_id=session-a",
            "session-a",
            "epoch-a"
        ));
    }

    #[test]
    fn diagnostics_describe_the_session_without_secrets() {
        let mut client = owned(&ALL_CAPS);
        client
            .transport
            .push(vec![receipt(2, 2, Some(metadata(7, 2, "fresh")))]);
        client.transport.frames.push_back(frame_packet(7, 2, 0));
        client.capture().expect("capture");
        let diagnostics = client.diagnostics();
        assert_eq!(diagnostics["implementation"], "sidecar");
        assert_eq!(diagnostics["backend"], "cloud_web");
        assert_eq!(diagnostics["session_id"], "session-test");
        assert_eq!(diagnostics["generation"], 2);
        assert_eq!(diagnostics["owner_lease"]["lease_ms"], 350);
        assert_eq!(diagnostics["commands"], 2);
        assert!(diagnostics["observation_age_ms"].is_number());
        let text = diagnostics.to_string();
        assert!(!text.contains("token-fixture"));
        assert!(!text.contains("pipe"));
        assert!(!text.contains("frame-test"));
    }

    /// Real-process integration: launches the sidecar executable with its fake
    /// actuator over named pipes and runs
    /// hello -> owner -> frame -> input -> release -> close.
    ///
    /// Ignored by default because it needs a built sidecar binary; run it with
    /// `CLOUDWEB_SIDECAR_PATH` set and `--ignored`. It never touches Chrome or a
    /// CDP port: the child runs with `--backend fake`.
    #[cfg(windows)]
    #[test]
    #[ignore = "requires CLOUDWEB_SIDECAR_PATH pointing at a built cloudweb-sidecar binary"]
    fn a_real_sidecar_process_runs_the_session_end_to_end() {
        use std::process::Command as StdCommand;

        let exec = std::env::var(SIDECAR_PATH_ENV)
            .unwrap_or_else(|_| panic!("{SIDECAR_PATH_ENV} must point at the sidecar binary"));
        let config = LaunchConfig::resolve("session-integration", "default", &|key| match key {
            SIDECAR_PATH_ENV => Some(exec.clone()),
            SIDECAR_ARGS_ENV => Some("--backend fake".to_string()),
            _ => None,
        })
        .expect("launch config resolves");
        let controller = SidecarCloudWebController::new(config);
        let pid = controller
            .launch_for_test()
            .expect("the sidecar process launches, handshakes and acquires ownership");

        let info: Value =
            serde_json::from_str(&CustomControllerCallback::get_info(&controller)).expect("info");
        assert_eq!(info["implementation"], "sidecar");
        assert_eq!(info["backend"], "cloud_web");
        assert_eq!(info["session_id"], "session-integration");
        // The handshake inherits the sidecar's generation; ownership has not
        // been taken yet, so frames alone do not advance it.
        assert_eq!(info["generation"].as_u64().expect("generation"), 0);
        assert_eq!(info["owner_lease"]["held"], false);
        assert!(
            info["capabilities"]
                .as_array()
                .expect("capabilities")
                .iter()
                .any(|value| value == "frame.latest"),
            "the sidecar declares its own capability set: {info}"
        );

        let frame = CustomControllerCallback::screencap(&controller).expect("a real frame");
        assert!(frame.starts_with(b"\x89PNG\r\n\x1a\n"));

        // Inputs after a fresh observation must reach the running session.
        assert!(CustomControllerCallback::key_down(&controller, 65));
        assert!(CustomControllerCallback::key_up(&controller, 65));
        assert!(CustomControllerCallback::touch_down(
            &controller,
            0,
            100,
            200,
            1
        ));
        assert!(CustomControllerCallback::touch_up(&controller, 0));
        assert!(CustomControllerCallback::input_text(
            &controller,
            "cloudweb"
        ));
        // A capability the fake backend does not declare must not report success.
        assert!(!CustomControllerCallback::touch_down(
            &controller,
            2,
            1,
            1,
            1
        ));

        let after: Value =
            serde_json::from_str(&CustomControllerCallback::get_info(&controller)).expect("info");
        assert!(after["commands"].as_u64().expect("commands") >= 7);
        assert!(after["observation_age_ms"].is_number());
        // The first input acquired ownership, which advanced the generation.
        assert!(after["owner_lease"]["held"].as_bool().expect("held"));
        assert!(after["generation"].as_u64().expect("generation") >= 1);

        // Give up ownership, then drop: close is acknowledged and the child exits.
        assert!(CustomControllerCallback::inactive(&controller));
        drop(controller);

        // The child process must be gone; poll briefly because the OS may report
        // the pid for a few milliseconds after the last wait.
        let mut gone = false;
        for _ in 0..20 {
            let output = StdCommand::new("tasklist")
                .args(["/FI", &format!("PID eq {pid}"), "/FO", "CSV", "/NH"])
                .output()
                .expect("tasklist is available");
            let text = String::from_utf8_lossy(&output.stdout);
            if !text.contains(&pid.to_string()) {
                gone = true;
                break;
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        assert!(gone, "the sidecar child (pid {pid}) survived the host drop");
    }
}
