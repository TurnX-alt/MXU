//! CloudWeb Host bridge placeholder.
//!
//! Uses a deterministic fake backend to qualify the controller factory, pool
//! identity and callback lifetime before the real sidecar/browser exists.

use base64::{engine::general_purpose::STANDARD, Engine as _};
use sha2::{Digest, Sha256};
use std::sync::atomic::{AtomicBool, Ordering};

use maa_framework::{
    controller::Controller, custom_controller::CustomControllerCallback, ControllerFeature,
};
use serde_json::json;

/// 16:9 red PNG. MaaFramework scales it to the configured screenshot target.
const FAKE_PNG_B64: &str =
    "iVBORw0KGgoAAAANSUhEUgAAABAAAAAJCAIAAAC0SDtlAAAAFUlEQVR4nGP4z8BAEiJN9agGWmkAACRlj3GqMTmoAAAAAElFTkSuQmCC";

/// Deterministic fake controller used by the CloudWeb Host bridge.
pub struct FakeCloudWebController {
    session_id: String,
    profile: String,
    uuid: String,
    connected: AtomicBool,
    frame: Vec<u8>,
}

impl FakeCloudWebController {
    pub fn new(session_id: impl Into<String>, profile: impl Into<String>) -> Self {
        let session_id = session_id.into();
        let profile = profile.into();
        let uuid = stable_uuid(&format!("{session_id}:{profile}"));
        Self {
            session_id,
            profile,
            uuid,
            connected: AtomicBool::new(false),
            frame: STANDARD
                .decode(FAKE_PNG_B64)
                .expect("embedded fake PNG must be valid base64"),
        }
    }

    pub fn session_id(&self) -> &str {
        &self.session_id
    }
}

fn stable_uuid(seed: &str) -> String {
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

/// Build a CloudWeb controller for the configured backend.
pub fn create_cloudweb_controller(
    backend: &str,
    session_id: &str,
    profile: &str,
) -> Result<Controller, String> {
    match backend {
        "fake" => Controller::new_custom(FakeCloudWebController::new(session_id, profile))
            .map_err(|error| error.to_string()),
        other => Err(format!("cloudweb_backend_unsupported:{other}")),
    }
}

impl CustomControllerCallback for FakeCloudWebController {
    fn connect(&self) -> bool {
        self.connected.store(true, Ordering::SeqCst);
        log::info!(
            target: "cloudweb",
            "fake CloudWeb controller connected: {}",
            self.session_id
        );
        true
    }

    fn connected(&self) -> bool {
        self.connected.load(Ordering::SeqCst)
    }

    fn request_uuid(&self) -> Option<String> {
        Some(self.uuid.clone())
    }

    fn get_features(&self) -> ControllerFeature {
        ControllerFeature::USE_MOUSE_DOWN_UP_INSTEAD_OF_CLICK
            | ControllerFeature::USE_KEY_DOWN_UP_INSTEAD_OF_CLICK
    }

    fn screencap(&self) -> Option<Vec<u8>> {
        Some(self.frame.clone())
    }

    fn click(&self, x: i32, y: i32) -> bool {
        log::debug!(
            target: "cloudweb",
            "fake click: session_id={} x={} y={}",
            self.session_id,
            x,
            y
        );
        true
    }

    fn swipe(&self, x1: i32, y1: i32, x2: i32, y2: i32, duration: i32) -> bool {
        log::debug!(
            target: "cloudweb",
            "fake swipe: session_id={} from=({}, {}) to=({}, {}) duration={}",
            self.session_id,
            x1,
            y1,
            x2,
            y2,
            duration
        );
        true
    }

    fn touch_down(&self, contact: i32, x: i32, y: i32, pressure: i32) -> bool {
        log::debug!(
            target: "cloudweb",
            "fake touch down: session_id={} contact={} x={} y={} pressure={}",
            self.session_id,
            contact,
            x,
            y,
            pressure
        );
        true
    }

    fn touch_move(&self, contact: i32, x: i32, y: i32, pressure: i32) -> bool {
        log::debug!(
            target: "cloudweb",
            "fake touch move: session_id={} contact={} x={} y={} pressure={}",
            self.session_id,
            contact,
            x,
            y,
            pressure
        );
        true
    }

    fn touch_up(&self, contact: i32) -> bool {
        log::debug!(
            target: "cloudweb",
            "fake touch up: session_id={} contact={}",
            self.session_id,
            contact
        );
        true
    }

    fn click_key(&self, keycode: i32) -> bool {
        log::debug!(
            target: "cloudweb",
            "fake click key: session_id={} keycode={}",
            self.session_id,
            keycode
        );
        true
    }

    fn input_text(&self, text: &str) -> bool {
        log::debug!(
            target: "cloudweb",
            "fake input text: session_id={} chars={}",
            self.session_id,
            text.chars().count()
        );
        true
    }

    fn key_down(&self, keycode: i32) -> bool {
        log::debug!(
            target: "cloudweb",
            "fake key down: session_id={} keycode={}",
            self.session_id,
            keycode
        );
        true
    }

    fn key_up(&self, keycode: i32) -> bool {
        log::debug!(
            target: "cloudweb",
            "fake key up: session_id={} keycode={}",
            self.session_id,
            keycode
        );
        true
    }

    fn scroll(&self, dx: i32, dy: i32) -> bool {
        log::debug!(
            target: "cloudweb",
            "fake scroll: session_id={} dx={} dy={}",
            self.session_id,
            dx,
            dy
        );
        true
    }

    fn relative_move(&self, dx: i32, dy: i32) -> bool {
        log::debug!(
            target: "cloudweb",
            "fake relative move: session_id={} dx={} dy={}",
            self.session_id,
            dx,
            dy
        );
        true
    }

    fn get_info(&self) -> String {
        json!({
            "backend": "cloud_web",
            "backend_version": "0.1.0",
            "protocol_version": 1,
            "implementation": "fake",
            "session_id": self.session_id,
            "profile": self.profile,
            "ui_profile": "cloud_web.default",
            "capabilities": [
                "frame.latest",
                "input.keyboard",
                "input.relative",
                "input.text",
                "geometry.v1"
            ],
        })
        .to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::FakeCloudWebController;
    use maa_framework::custom_controller::CustomControllerCallback;

    #[test]
    fn fake_cloudweb_controller_reports_identity_and_frame() {
        let controller = FakeCloudWebController::new("session-a", "default");
        assert!(!controller.connected());
        assert!(controller.connect());
        assert!(controller.connected());
        let uuid = controller.request_uuid().expect("stable request uuid");
        assert_eq!(uuid.len(), 36);
        assert_eq!(controller.request_uuid(), Some(uuid.clone()));
        assert_ne!(
            uuid,
            FakeCloudWebController::new("session-b", "default")
                .request_uuid()
                .expect("other uuid")
        );

        let frame = controller.screencap().expect("fake frame");
        assert!(frame.starts_with(b"\x89PNG\r\n\x1a\n"));

        let info = controller.get_info();
        assert!(info.contains("\"backend\":\"cloud_web\""));
        assert!(info.contains("\"session_id\":\"session-a\""));
        assert!(controller.click(100, 200));
        assert!(controller.touch_move(1, 10, 20, 1));
        assert!(controller.input_text("cloudweb"));
    }

    #[test]
    fn fake_cloudweb_controller_connects_and_captures_native_frame() {
        let Ok(sdk) = std::env::var("MAA_SDK_PATH") else {
            return;
        };
        let sdk = std::path::PathBuf::from(sdk);
        let dll = [
            sdk.join("bin").join("MaaFramework.dll"),
            sdk.join("MaaFramework.dll"),
        ]
        .into_iter()
        .find(|path| path.exists());
        let Some(dll) = dll else {
            return;
        };
        if maa_framework::load_library(&dll).is_err() {
            return;
        }

        let controller = super::create_cloudweb_controller("fake", "native-test", "default")
            .expect("create fake controller");
        controller
            .set_screenshot_target_short_side(720)
            .expect("set target short side");

        let connect_id = controller.post_connection().expect("post connection");
        assert_eq!(controller.wait(connect_id).0, 3000);

        let capture_id = controller.post_screencap().expect("post screencap");
        assert_eq!(controller.wait(capture_id).0, 3000);

        let image = controller.cached_image().expect("cached image");
        assert_eq!(image.width(), 1280);
        assert_eq!(image.height(), 720);
    }
}
