//! Wire helpers for the CloudWeb host <-> sidecar protocol.
//!
//! The shapes here mirror `contract-v0.1`: the control plane is
//! length-prefixed JSON (`u32` little-endian length + UTF-8, 64 KiB max) and
//! the frame plane is a length-prefixed 32-byte header plus a PNG payload.
//! Only the payloads are constructed and parsed here; all I/O stays in
//! `sidecar`.

use serde_json::{json, Value};

/// Maximum control message size (contract section 9).
pub const MAX_CONTROL_BYTES: usize = 64 * 1024;
/// Maximum frame payload size (contract section 9).
pub const MAX_FRAME_BYTES: usize = 16 * 1024 * 1024;
/// Fixed frame header length; the wire also prefixes this with a `u32`.
pub const FRAME_HEADER_BYTES: usize = 32;
/// Frame header magic.
pub const FRAME_MAGIC: [u8; 4] = *b"CWFR";
/// Frame header version.
pub const FRAME_VERSION: u8 = 1;
/// Canonical frame size required by the contract.
pub const CANONICAL_WIDTH: u16 = 1280;
/// Canonical frame size required by the contract.
pub const CANONICAL_HEIGHT: u16 = 720;
/// Protocol major version implemented by this host.
pub const PROTOCOL_MAJOR: u16 = 1;
/// Protocol minor version implemented by this host.
pub const PROTOCOL_MINOR: u16 = 0;

/// Freshness tag carried by every frame header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Freshness {
    Fresh,
    Stale,
    Unknown,
}

impl Freshness {
    fn from_tag(tag: u8) -> Option<Self> {
        match tag {
            0 => Some(Self::Fresh),
            1 => Some(Self::Stale),
            2 => Some(Self::Unknown),
            _ => None,
        }
    }

    /// Parses the `freshness` string used by frame metadata receipts.
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "fresh" => Some(Self::Fresh),
            "stale" => Some(Self::Stale),
            "unknown" => Some(Self::Unknown),
            _ => None,
        }
    }
}

/// Decoded 32-byte frame header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FrameHeader {
    pub version: u8,
    pub freshness: Freshness,
    pub frame_id: u64,
    pub generation: u64,
    pub width: u16,
    pub height: u16,
}

impl FrameHeader {
    /// Decodes a full header. A short buffer, wrong magic or unsupported
    /// version is a protocol error, never a silently accepted frame.
    pub fn decode(bytes: &[u8]) -> Result<Self, SidecarError> {
        if bytes.len() != FRAME_HEADER_BYTES {
            return Err(SidecarError::local(
                "frame_header_length",
                format!("frame header is {} bytes", bytes.len()),
            ));
        }
        if bytes[0..4] != FRAME_MAGIC {
            return Err(SidecarError::local(
                "frame_header_magic",
                "frame header magic mismatch",
            ));
        }
        if bytes[4] != FRAME_VERSION {
            return Err(SidecarError::local(
                "frame_header_version",
                format!("unsupported frame header version {}", bytes[4]),
            ));
        }
        let freshness = Freshness::from_tag(bytes[5]).ok_or_else(|| {
            SidecarError::local("frame_header_freshness", "invalid freshness tag")
        })?;
        let frame_id = u64::from_le_bytes(bytes[8..16].try_into().expect("fixed slice"));
        let generation = u64::from_le_bytes(bytes[16..24].try_into().expect("fixed slice"));
        let width = u16::from_le_bytes(bytes[24..26].try_into().expect("fixed slice"));
        let height = u16::from_le_bytes(bytes[26..28].try_into().expect("fixed slice"));
        Ok(Self {
            version: bytes[4],
            freshness,
            frame_id,
            generation,
            width,
            height,
        })
    }
}

/// Encodes one control frame: `u32` little-endian length + JSON payload.
pub fn encode_control(payload: &Value) -> Result<Vec<u8>, SidecarError> {
    let bytes = serde_json::to_vec(payload)
        .map_err(|error| SidecarError::local("control_encode", error.to_string()))?;
    if bytes.is_empty() || bytes.len() > MAX_CONTROL_BYTES {
        return Err(SidecarError::local(
            "control_size",
            format!("control frame of {} bytes is out of range", bytes.len()),
        ));
    }
    let length = u32::try_from(bytes.len())
        .map_err(|_| SidecarError::local("control_size", "control frame length overflow"))?;
    let mut frame = Vec::with_capacity(bytes.len() + 4);
    frame.extend_from_slice(&length.to_le_bytes());
    frame.extend_from_slice(&bytes);
    Ok(frame)
}

/// Decodes the payload of one received control frame.
pub fn decode_control(bytes: &[u8]) -> Result<Value, SidecarError> {
    if bytes.is_empty() || bytes.len() > MAX_CONTROL_BYTES {
        return Err(SidecarError::local(
            "control_size",
            format!("control frame of {} bytes is out of range", bytes.len()),
        ));
    }
    let value: Value = serde_json::from_slice(bytes)
        .map_err(|error| SidecarError::local("control_decode", error.to_string()))?;
    if value.get("type").and_then(Value::as_str).is_none() {
        return Err(SidecarError::local(
            "control_decode",
            "control frame has no string field `type`",
        ));
    }
    Ok(value)
}

/// Builds the `hello` first frame. The token is embedded only in this frame and
/// is never logged; callers must not print the returned value.
pub fn hello_frame(
    token: &str,
    host_name: &str,
    host_version: &str,
    maa_framework: &str,
    capabilities: &[&str],
    ui_profile: &str,
) -> Value {
    json!({
        "type": "hello",
        "protocol_major": PROTOCOL_MAJOR,
        "protocol_minor": PROTOCOL_MINOR,
        "token": token,
        "host": {
            "name": host_name,
            "version": host_version,
            "maa_framework": maa_framework,
        },
        "capabilities": capabilities,
        "ui_profile": ui_profile,
    })
}

/// Builds a `command` frame for the control plane.
#[allow(clippy::too_many_arguments)]
pub fn command_frame(
    op: &str,
    session_id: &str,
    backend_epoch: &str,
    generation: u64,
    seq: u64,
    params: Value,
    deadline_ms: u64,
    observation: Option<&Observation>,
) -> Value {
    let mut frame = json!({
        "type": "command",
        "op": op,
        "session_id": session_id,
        "backend_epoch": backend_epoch,
        "generation": generation,
        "seq": seq,
        "deadline_ms": deadline_ms,
        "params": params,
    });
    if let Some(observation) = observation {
        frame["based_on_frame_id"] = json!(observation.frame_id);
        frame["based_on_age_ms"] = json!(observation.age_ms);
        frame["geometry_revision"] = json!(observation.geometry_revision);
    }
    frame
}

/// Observation reference an input command is justified by.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Observation {
    pub frame_id: u64,
    pub age_ms: u64,
    pub geometry_revision: u64,
}

/// A protocol failure. `code` is a machine-readable identifier: either a sidecar
/// error code (`owner_conflict`, `generation_stale`, ...) or a host-side code
/// prefixed with `cloudweb_`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SidecarError {
    pub code: String,
    pub message: String,
}

impl SidecarError {
    pub fn local(code: &str, message: impl Into<String>) -> Self {
        Self {
            code: code.to_string(),
            message: message.into(),
        }
    }

    pub fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
        }
    }

    /// Reads the error from a `rejected`/`unknown` receipt or an `error` frame.
    pub fn from_frame(value: &Value) -> Self {
        let code = value
            .get("error")
            .or_else(|| value.get("code"))
            .and_then(Value::as_str)
            .unwrap_or("internal")
            .to_string();
        let message = value
            .get("message")
            .and_then(Value::as_str)
            .unwrap_or("no message")
            .to_string();
        Self { code, message }
    }

    /// The owner lease is gone; a new owner must be acquired with a new
    /// generation before further input (contract section 4).
    pub fn is_lease_loss(&self) -> bool {
        matches!(self.code.as_str(), "lease_expired" | "owner_conflict")
    }

    /// The sidecar advanced its generation; resynchronise before the next
    /// command instead of replaying the rejected one.
    pub fn is_generation_stale(&self) -> bool {
        self.code == "generation_stale"
    }

    /// The sidecar has no fresh observation for this action; a capture and one
    /// retry are allowed because a `rejected` receipt never dispatched.
    pub fn is_frame_stale(&self) -> bool {
        self.code == "frame_stale"
    }

    /// The session faulted and only a close or manual recovery may leave it.
    pub fn is_faulted(&self) -> bool {
        matches!(self.code.as_str(), "faulted" | "browser_lost")
    }

    pub fn code(&self) -> &str {
        &self.code
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn header_bytes(freshness: u8, frame_id: u64, generation: u64, w: u16, h: u16) -> [u8; 32] {
        let mut bytes = [0u8; 32];
        bytes[0..4].copy_from_slice(&FRAME_MAGIC);
        bytes[4] = FRAME_VERSION;
        bytes[5] = freshness;
        bytes[8..16].copy_from_slice(&frame_id.to_le_bytes());
        bytes[16..24].copy_from_slice(&generation.to_le_bytes());
        bytes[24..26].copy_from_slice(&w.to_le_bytes());
        bytes[26..28].copy_from_slice(&h.to_le_bytes());
        bytes
    }

    #[test]
    fn frame_header_decodes_its_own_encoding() {
        let header = FrameHeader::decode(&header_bytes(0, 4242, 7, 1280, 720)).expect("header");
        assert_eq!(header.frame_id, 4242);
        assert_eq!(header.generation, 7);
        assert_eq!(header.width, CANONICAL_WIDTH);
        assert_eq!(header.height, CANONICAL_HEIGHT);
        assert_eq!(header.freshness, Freshness::Fresh);
    }

    #[test]
    fn frame_header_rejects_bad_magic_length_and_freshness() {
        let mut bytes = header_bytes(0, 1, 1, 1280, 720);
        bytes[0] = b'X';
        assert_eq!(
            FrameHeader::decode(&bytes).unwrap_err().code,
            "frame_header_magic"
        );
        assert_eq!(
            FrameHeader::decode(&bytes[..16]).unwrap_err().code,
            "frame_header_length"
        );
        assert_eq!(
            FrameHeader::decode(&header_bytes(9, 1, 1, 1280, 720))
                .unwrap_err()
                .code,
            "frame_header_freshness"
        );
    }

    #[test]
    fn control_frames_round_trip_with_length_prefix() {
        let frame = encode_control(&json!({"type": "hello", "token": "t"})).expect("encode");
        assert_eq!(
            &u32::from_le_bytes(frame[..4].try_into().unwrap()).to_le_bytes(),
            &frame[..4]
        );
        let payload = &frame[4..];
        let decoded = decode_control(payload).expect("decode");
        assert_eq!(decoded["type"], "hello");
    }

    #[test]
    fn control_frames_reject_oversize_and_missing_type() {
        let big =
            serde_json::to_vec(&json!({"type": "command", "blob": "x".repeat(MAX_CONTROL_BYTES)}))
                .expect("serialize");
        assert_eq!(
            encode_control(&json!({"type": "command", "blob": "x".repeat(MAX_CONTROL_BYTES)}))
                .unwrap_err()
                .code,
            "control_size"
        );
        assert!(big.len() > MAX_CONTROL_BYTES);
        assert_eq!(
            decode_control(br#"{"no_type":1}"#).unwrap_err().code,
            "control_decode"
        );
    }

    #[test]
    fn command_frame_only_carries_observation_when_supplied() {
        let plain = command_frame("key_down", "s", "e", 3, 9, json!({"key": 65}), 1000, None);
        assert!(plain.get("based_on_frame_id").is_none());
        assert_eq!(plain["op"], "key_down");
        assert_eq!(plain["seq"], 9);

        let observed = command_frame(
            "mouse_down",
            "s",
            "e",
            3,
            10,
            json!({"button": 0, "x": 10, "y": 20}),
            1000,
            Some(&Observation {
                frame_id: 42,
                age_ms: 12,
                geometry_revision: 1,
            }),
        );
        assert_eq!(observed["based_on_frame_id"], 42);
        assert_eq!(observed["based_on_age_ms"], 12);
        assert_eq!(observed["geometry_revision"], 1);
    }

    #[test]
    fn error_codes_classify_lease_and_staleness() {
        let receipt = json!({"type": "receipt", "seq": 1, "status": "rejected",
            "generation": 5, "error": "lease_expired", "message": "lease expired"});
        let error = SidecarError::from_frame(&receipt);
        assert!(error.is_lease_loss());
        assert!(!error.is_generation_stale());
        assert_eq!(error.message, "lease expired");

        let frame = json!({"type": "error", "code": "frame_stale", "message": "no fresh frame"});
        let error = SidecarError::from_frame(&frame);
        assert!(error.is_frame_stale());
        assert!(!error.is_faulted());

        // A receipt without an error field still yields a usable code.
        let bare = json!({"type": "receipt", "seq": 2, "status": "unknown", "generation": 5});
        assert_eq!(SidecarError::from_frame(&bare).code, "internal");
    }
}
