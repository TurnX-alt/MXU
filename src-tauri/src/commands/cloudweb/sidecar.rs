//! Real CloudWeb sidecar backend: process lifecycle, named-pipe client,
//! owner/lease handling, frame consumption and input mapping.
//!
//! Layout:
//!
//! * [`SidecarClient`] keeps the protocol state machine and drives every
//!   command through [`ControlLink`], so it is unit-tested against a scripted
//!   test double. The link multiplexes the single control pipe: one whole-frame
//!   writer with a short critical section, one reader thread routing receipts
//!   by seq, and no client-state lock held while waiting for a receipt.
//! * [`SidecarCloudWebController`] is the `CustomControllerCallback` used by
//!   MaaFramework. It launches the sidecar executable, owns the child process
//!   and the Windows named pipes, and never falls back to the fake stub.
//!
//! Lease model (contract sections 4/9, review-4 N1, CW2 D1): the control plane
//! is multiplexed by [`ControlLink`]: one writer with a short frame-lock, one
//! reader thread routing receipts by seq, and a pending map so a normal command
//! never holds a lock the urgent Stop path needs. The client renews before and
//! after slow operations, re-acquires a new generation when the sidecar reports
//! the lease is gone, and records the observed renewal gap and lease losses
//! instead of claiming the 350 ms lease is guaranteed.

use std::cmp::Ordering;
use std::path::PathBuf;
use std::process::Command;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering as AtomicOrdering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

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
/// Environment key selecting the inner actuator (`fake` or `cdp`) when the
/// launch arguments do not contain `--backend`.
pub const BACKEND_ENV: &str = "CLOUDWEB_BACKEND";
/// Development-only opt-in for the inner `fake` actuator.
pub const ALLOW_FAKE_ENV: &str = "CLOUDWEB_ALLOW_FAKE";

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
/// Control-plane wait budget for one command round trip.
// The first GetLatestFrame after connect can take several seconds while the
// cloud page warms its video pipeline (observed 3.5 s on a hidden tab). With
// the [`ControlLink`] a late receipt no longer desyncs the pipe: the reader
// routes by seq, so a wait timeout fails only that command.
const CONTROL_TIMEOUT: Duration = Duration::from_secs(10);
/// Urgent (Stop) receipt budget. CW2 promises ReleaseAll acknowledged within
/// one second end to end; the host wait and the wire `deadline_ms` for the
/// urgent path both carry this budget.
const URGENT_RECEIPT_TIMEOUT: Duration = Duration::from_millis(1000);
/// Bound for out-of-order receipts that arrive before their waiter registered
/// (unit-test fakes pre-push, a late receipt after a wait timeout). Overflow
/// drops the oldest with a warning so the map cannot grow without limit.
const EARLY_RECEIPT_CAPACITY: usize = 64;
/// Frame-plane read budget for one capture.
const FRAME_TIMEOUT: Duration = Duration::from_millis(1000);
/// The sidecar closes the frame pipe when a client does not drain it within
/// its write budget (observed during the gap between connect and the first
/// task capture). A capture reopens the plane and re-requests a fresh frame.
const FRAME_RECONNECTS: u32 = 2;
/// Grace period between asking the child to stop and forcing a kill.
const CLOSE_GRACE: Duration = Duration::from_millis(1500);

/// Lease values assumed until the sidecar reports its limits.
const FALLBACK_LEASE_MS: u64 = 350;
/// Upper bound the Host asks the sidecar to grant when its observed command
/// RTT approaches the default lease. Matches the sidecar clamp.
const MAX_REQUESTED_LEASE_MS: u64 = 3_000;
const FALLBACK_RENEW_MS: u64 = 100;
const FALLBACK_OBSERVATION_MS: u64 = 1000;

/// One frame-plane packet: the fixed header plus the PNG payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawFrame {
    pub header: [u8; wire::FRAME_HEADER_BYTES],
    pub bytes: Vec<u8>,
}

/// Blocking byte writer for one control-plane half. The [`ControlLink`] holds
/// it behind a mutex only for the duration of one whole length-prefixed frame,
/// so an urgent write never queues behind a command waiting for its receipt.
pub trait ControlWriter: Send {
    /// Sends one length-prefixed control frame.
    fn send_control(&mut self, frame: &[u8]) -> Result<(), SidecarError>;
}

/// Blocking byte reader for one control-plane half. The link's single reader
/// thread owns it for the session lifetime; a `None` read means the peer
/// closed the pipe.
pub trait ControlReader: Send {
    /// Reads one control payload. `None` means the peer closed the pipe.
    fn recv_control(&mut self) -> Result<Option<Vec<u8>>, SidecarError>;
}

/// Frame-plane transport, independent of the control link.
pub trait FrameTransport: Send {
    /// Reads one frame packet, waiting at most `timeout` for it.
    fn recv_frame(&mut self, timeout: Duration) -> Result<Option<RawFrame>, SidecarError>;
    /// Whether this transport can reopen the frame plane after the sidecar
    /// closed a stalled connection.
    fn can_reconnect_frame(&self) -> bool {
        false
    }
    /// Reopens the frame plane; only called when `can_reconnect_frame`.
    fn reconnect_frame(&mut self, _frame_pipe: &str) -> Result<(), SidecarError> {
        Err(SidecarError::local(
            "cloudweb_frame_reconnect_unsupported",
            "transport cannot reopen the frame pipe",
        ))
    }
}

/// The three halves a [`SidecarClient`] is built from: the two control-plane
/// halves the [`ControlLink`] consumes and the frame plane the client reads
/// synchronously.
pub struct ControlParts {
    pub writer: Box<dyn ControlWriter>,
    pub reader: Box<dyn ControlReader>,
    pub frame: Box<dyn FrameTransport>,
}

/// Host-side session flags shared between the client (under the client lock),
/// the link reader thread and the controller, so no background path has to
/// wait for the client lock to read them.
pub(crate) struct SessionFlags {
    handshaked: AtomicBool,
    faulted: AtomicBool,
    closed: AtomicBool,
    last_error: Mutex<Option<String>>,
}

impl SessionFlags {
    fn new() -> Self {
        Self {
            handshaked: AtomicBool::new(false),
            faulted: AtomicBool::new(false),
            closed: AtomicBool::new(false),
            last_error: Mutex::new(None),
        }
    }

    fn record_error(&self, error: &SidecarError) {
        *self
            .last_error
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(error.code.clone());
    }

    fn last_error(&self) -> Option<String> {
        self.last_error
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }
}

/// Identity and lease counters shared the same way as [`SessionFlags`]. The
/// renewal thread and the urgent ReleaseAll path update these directly; the
/// client only mirrors them into its diagnostics under the client lock.
pub(crate) struct LeaseState {
    session_id: String,
    backend_epoch: String,
    owner_id: String,
    /// Unix epoch milliseconds of the last successful renewal; negative means
    /// no lease is held. Instant is not convertible to an atomic, so the lease
    /// clock is kept in epoch millis.
    renewed_at_epoch_ms: AtomicI64,
    renew_ms: AtomicU64,
    max_renew_gap_ms: AtomicU64,
    lease_losses: AtomicU64,
}

impl LeaseState {
    fn new(config: &LaunchConfig) -> Self {
        Self {
            session_id: config.session_id.clone(),
            backend_epoch: config.backend_epoch.clone(),
            owner_id: config.owner_id.clone(),
            renewed_at_epoch_ms: AtomicI64::new(-1),
            renew_ms: AtomicU64::new(FALLBACK_RENEW_MS),
            max_renew_gap_ms: AtomicU64::new(0),
            lease_losses: AtomicU64::new(0),
        }
    }
}

fn epoch_ms_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|since| since.as_millis() as i64)
        .unwrap_or(0)
}

fn epoch_ms_to_instant(epoch_ms: i64) -> Instant {
    let elapsed_ms = (epoch_ms_now() - epoch_ms).max(0) as u64;
    Instant::now().checked_sub(Duration::from_millis(elapsed_ms)).unwrap_or_else(Instant::now)
}

/// One entry of the link's pending map. A waiter registers before writing, so
/// a receipt that arrives first (possible with the scripted test double and
/// after a wait timeout) is parked as `Early` and handed over on registration
/// completion.
enum PendingSlot {
    Waiting {
        seq: u64,
        tx: Sender<Result<Value, SidecarError>>,
    },
    Early {
        seq: u64,
        value: Value,
    },
}

impl PendingSlot {
    fn seq(&self) -> u64 {
        match self {
            PendingSlot::Waiting { seq, .. } | PendingSlot::Early { seq, .. } => *seq,
        }
    }
}

/// The multiplexed control plane (CW2 D1). Normal commands, lease renewals
/// and the urgent ReleaseAll path all share this one writer/reader pair; the
/// only serialization point is the whole-frame write, which never spans a
/// receipt wait.
pub struct ControlLink {
    writer: Mutex<Box<dyn ControlWriter>>,
    /// The reader half lives here until [`ControlLink::ensure_reader`] moves
    /// it into the routing thread. Before that the handshake reads it
    /// directly, preserving the synchronous hello/hello_ok exchange.
    reader_slot: Mutex<Option<Box<dyn ControlReader>>>,
    reader_started: AtomicBool,
    shutdown: AtomicBool,
    next_seq: AtomicU64,
    pending: Mutex<Vec<PendingSlot>>,
    generation: AtomicU64,
    lease: Arc<LeaseState>,
    flags: Arc<SessionFlags>,
    /// Urgent-path diagnostics (acceptance evidence for CW2 D1).
    urgent_write_ms: AtomicU64,
    release_all_rtt_ms: AtomicU64,
    early_drops: AtomicU64,
    /// Successful control round trips routed by this link. The counter lives
    /// here because renewals and urgent commands no longer pass through the
    /// client's `send`.
    commands_total: AtomicU64,
    slowest_command_ms: AtomicU64,
}

impl ControlLink {
    /// Builds the link. The reader thread is spawned lazily so the handshake
    /// can read the raw hello_ok before any routing begins.
    pub(crate) fn new(
        writer: Box<dyn ControlWriter>,
        reader: Box<dyn ControlReader>,
        lease: Arc<LeaseState>,
        flags: Arc<SessionFlags>,
    ) -> Arc<Self> {
        Arc::new_cyclic(|_weak| Self {
            writer: Mutex::new(writer),
            reader_slot: Mutex::new(Some(reader)),
            reader_started: AtomicBool::new(false),
            shutdown: AtomicBool::new(false),
            next_seq: AtomicU64::new(0),
            pending: Mutex::new(Vec::new()),
            generation: AtomicU64::new(0),
            lease,
            flags,
            urgent_write_ms: AtomicU64::new(0),
            release_all_rtt_ms: AtomicU64::new(0),
            early_drops: AtomicU64::new(0),
            commands_total: AtomicU64::new(0),
            slowest_command_ms: AtomicU64::new(0),
        })
    }

    pub fn generation(&self) -> u64 {
        self.generation.load(AtomicOrdering::SeqCst)
    }

    pub fn set_generation(&self, generation: u64) {
        self.generation.store(generation, AtomicOrdering::SeqCst);
    }

    pub fn is_closed(&self) -> bool {
        self.flags.closed.load(AtomicOrdering::SeqCst)
            || self.shutdown.load(AtomicOrdering::SeqCst)
    }

    fn is_faulted(&self) -> bool {
        self.flags.faulted.load(AtomicOrdering::SeqCst)
    }

    /// Whether the shared lease clock believes an owner lease is held.
    fn lease_held(&self) -> bool {
        self.renewed_at_instant().is_some()
    }

    /// Renewal cadence from the shared mirror, clamped so a bad peer cannot
    /// spin the renewal thread.
    fn renew_interval(&self) -> Duration {
        Duration::from_millis(
            self.lease
                .renew_ms
                .load(AtomicOrdering::SeqCst)
                .max(20),
        )
    }

    /// Number of undelivered pending entries; used by the leak tests.
    fn pending_count(&self) -> usize {
        self.pending
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .len()
    }

    fn lock_pending(&self) -> MutexGuard<'_, Vec<PendingSlot>> {
        self.pending
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Moves the reader half into the routing thread. Idempotent.
    fn ensure_reader(self: &Arc<Self>) -> Result<(), SidecarError> {
        if self.reader_started.load(AtomicOrdering::SeqCst) {
            return Ok(());
        }
        let reader = {
            let mut slot = self
                .reader_slot
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            slot.take()
        };
        let Some(reader) = reader else {
            // The handshake still holds the reader: the caller must finish
            // the hello exchange first. Nothing to start for this link.
            return Ok(());
        };
        let weak = Arc::downgrade(self);
        let spawned = std::thread::Builder::new()
            .name("cloudweb-control-reader".to_string())
            .spawn(move || Self::reader_loop(weak, reader));
        if spawned.is_err() {
            // The moved reader cannot be recovered from a failed spawn; the
            // control plane is then unusable, so settle it like an EOF
            // instead of leaving future commands to hang.
            self.fail_open_waiters(&SidecarError::local(
                "cloudweb_control_reader_spawn_failed",
                "the control reader thread could not be started",
            ));
        } else {
            self.reader_started.store(true, AtomicOrdering::SeqCst);
        }
        spawned
            .map(|handle| {
                let _ = handle;
            })
            .map_err(|error| {
                SidecarError::local(
                    "cloudweb_control_reader_spawn_failed",
                    error.to_string(),
                )
            })
    }

    /// The single reader: routes receipts by seq, parks unknown-seq receipts
    /// as early arrivals, and settles every pending waiter with
    /// `cloudweb_control_closed` on EOF or a fatal read error.
    fn reader_loop(weak: std::sync::Weak<Self>, mut reader: Box<dyn ControlReader>) {
        loop {
            let Some(link) = weak.upgrade() else {
                return;
            };
            if link.shutdown.load(AtomicOrdering::SeqCst) {
                return;
            }
            match reader.recv_control() {
                Ok(Some(payload)) => match wire::decode_control(&payload) {
                    Ok(value) => link.route(value),
                    Err(error) => link.fail_open_waiters(&error),
                },
                Ok(None) => {
                    link.fail_open_waiters(&SidecarError::local(
                        "cloudweb_control_closed",
                        "control pipe closed",
                    ));
                    return;
                }
                Err(error) => {
                    // A read timeout only means no frame arrived within the
                    // transport budget; the pipe is still alive.
                    if error.code() == "cloudweb_control_read_timeout" {
                        continue;
                    }
                    link.fail_open_waiters(&error);
                    return;
                }
            }
        }
    }

    fn route(self: &Arc<Self>, value: Value) {
        let kind = value["type"].as_str().unwrap_or_default().to_string();
        match value["seq"].as_u64() {
            Some(seq) => self.deliver(seq, kind, value),
            None => {
                // Once the reader thread owns the pipe, every routed frame
                // must carry a seq; anything else cannot be attributed.
                log::warn!(target: "cloudweb", "CloudWeb control frame without seq dropped: {kind}");
            }
        }
    }

    fn deliver(self: &Arc<Self>, seq: u64, kind: String, value: Value) {
        let mut pending = self.lock_pending();
        // A terminal `error` frame for a live seq is always fatal (the v1
        // sidecar only sends it during handshake, but honour it if it lands).
        if kind == "error" {
            let error = SidecarError::from_frame(&value);
            drop(pending);
            self.fail_open_waiters(&error);
            return;
        }
        if let Some(index) = pending.iter().position(|slot| slot.seq() == seq) {
            if matches!(pending[index], PendingSlot::Waiting { .. }) {
                let slot = pending.remove(index);
                drop(pending);
                if let PendingSlot::Waiting { tx, .. } = slot {
                    let _ = tx.send(Ok(value));
                }
                return;
            }
            // Replace a stale early entry with the freshest arrival.
            pending[index] = PendingSlot::Early { seq, value };
            return;
        }
        // Park as an early arrival, bounded so nothing leaks.
        let evict = pending
            .iter()
            .filter(|slot| matches!(slot, PendingSlot::Early { .. }))
            .count()
            >= EARLY_RECEIPT_CAPACITY;
        if evict {
            if let Some(index) = pending
                .iter()
                .position(|slot| matches!(slot, PendingSlot::Early { .. }))
            {
                let dropped = pending.remove(index);
                log::warn!(
                    target: "cloudweb",
                    "CloudWeb early receipt buffer overflow, dropped seq {}",
                    dropped.seq()
                );
                self.early_drops.fetch_add(1, AtomicOrdering::SeqCst);
            }
        }
        pending.push(PendingSlot::Early { seq, value });
    }

    /// Completes a registered waiter: delivers an early receipt that already
    /// arrived, otherwise keeps the channel until the reader routes the seq.
    fn finish_register(self: &Arc<Self>, seq: u64, tx: Sender<Result<Value, SidecarError>>) {
        let mut pending = self.lock_pending();
        let mut early: Option<Value> = None;
        if let Some(index) = pending.iter().position(|slot| slot.seq() == seq) {
            if let PendingSlot::Early { value, .. } = pending.remove(index) {
                early = Some(value);
            }
        }
        match early {
            Some(value) => {
                // The receipt already arrived: settle the waiter right here.
                drop(pending);
                let _ = tx.send(Ok(value));
            }
            None => pending.push(PendingSlot::Waiting { seq, tx }),
        }
    }

    fn fail_open_waiters(self: &Arc<Self>, error: &SidecarError) {
        self.shutdown.store(true, AtomicOrdering::SeqCst);
        let slots: Vec<PendingSlot> = {
            let mut pending = self.lock_pending();
            std::mem::take(&mut *pending)
        };
        for slot in slots {
            if let PendingSlot::Waiting { tx, .. } = slot {
                let _ = tx.send(Err(error.clone()));
            }
        }
    }

    /// Marks the link closed so host-side teardown (Drop) cannot hang on a
    /// receipt that will never be routed.
    fn mark_closed(self: &Arc<Self>) {
        self.fail_open_waiters(&SidecarError::local(
            "cloudweb_control_closed",
            "control link torn down",
        ));
    }

    /// Writes one whole frame under the writer lock. The critical section is
    /// the write itself: no receipt wait ever holds it, so urgent frames are
    /// bounded by the pipe, not by other commands.
    fn write_frame(&self, value: &Value) -> Result<Duration, SidecarError> {
        let frame = wire::encode_control(value)?;
        let started = Instant::now();
        let mut writer = self
            .writer
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let result = writer.send_control(&frame);
        let elapsed = started.elapsed();
        drop(writer);
        result?;
        Ok(elapsed)
    }

    /// Raw synchronous read used before the reader thread starts (hello /
    /// hello_ok exchange only): the transport applies its own control-read
    /// budget, matching the previous synchronous handshake behaviour.
    fn read_raw(&self) -> Result<Option<Value>, SidecarError> {
        let mut slot = self
            .reader_slot
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let Some(reader) = slot.as_mut() else {
            return Err(SidecarError::local(
                "cloudweb_control_closed",
                "the handshake must complete before commands route",
            ));
        };
        match reader.recv_control()? {
            Some(payload) => Ok(Some(wire::decode_control(&payload)?)),
            None => Ok(None),
        }
    }

    /// Registers a waiter for `seq` and returns its channel. The waiter is
    /// registered before the frame is written, so an early receipt handed
    /// over by [`ControlLink::finish_register`] closes the race window.
    fn request(self: &Arc<Self>, seq: u64) -> Receiver<Result<Value, SidecarError>> {
        let (tx, rx) = mpsc::channel();
        self.finish_register(seq, tx);
        rx
    }

    fn await_receipt(
        self: &Arc<Self>,
        seq: u64,
        rx: Receiver<Result<Value, SidecarError>>,
        timeout: Duration,
    ) -> Result<Value, SidecarError> {
        let result = match rx.recv_timeout(timeout) {
            Ok(result) => result,
            Err(mpsc::RecvTimeoutError::Timeout) => Err(SidecarError::local(
                "cloudweb_control_wait_timeout",
                "no receipt arrived in time",
            )),
            Err(mpsc::RecvTimeoutError::Disconnected) => Err(SidecarError::local(
                "cloudweb_control_closed",
                "the control link was torn down",
            )),
        };
        if result.is_err() {
            // A failed wait removes its slot entirely: a disconnected waiter
            // or a parked early receipt must never linger in the map.
            self.remove_slot(seq);
        }
        result
    }

    /// Removes the slot registered for `seq`, if any. Dropping the channel of
    /// a removed waiter is safe: a later `deliver` for the seq then parks an
    /// early receipt, and the early buffer is capacity-bound.
    fn remove_slot(&self, seq: u64) {
        let mut pending = self.lock_pending();
        if let Some(index) = pending.iter().position(|slot| slot.seq() == seq) {
            pending.remove(index);
        }
    }

    /// One transport-level command round trip: allocate seq, register the
    /// waiter, write the whole frame and wait for the routed receipt. Write
    /// and wait failures fail this command only and never hold a client lock.
    fn send_command(
        self: &Arc<Self>,
        op: &str,
        params: Value,
        observation: Option<&wire::Observation>,
        timeout: Duration,
        urgent: bool,
    ) -> Result<(u64, Value), SidecarError> {
        if self.is_closed() {
            return Err(SidecarError::local(
                "cloudweb_control_closed",
                "the control link is closed",
            ));
        }
        self.ensure_reader()?;
        let seq = self.next_seq.fetch_add(1, AtomicOrdering::SeqCst) + 1;
        let rx = self.request(seq);
        if self.is_closed() {
            // An EOF settled every waiter between the pre-check and the
            // registration: undo the orphan slot instead of waiting out the
            // timeout for a receipt that can no longer be routed.
            self.remove_slot(seq);
            return Err(SidecarError::local(
                "cloudweb_control_closed",
                "the control link is closed",
            ));
        }
        let frame = wire::command_frame(
            op,
            &self.lease.session_id,
            &self.lease.backend_epoch,
            self.generation(),
            seq,
            params,
            timeout.as_millis() as u64,
            observation,
        );
        let started = Instant::now();
        let write_result = self.write_frame(&frame);
        if let Ok(elapsed) = write_result {
            if urgent {
                self.urgent_write_ms
                    .store(elapsed.as_millis() as u64, AtomicOrdering::SeqCst);
            }
        }
        if write_result.is_err() {
            // The frame never reached the pipe: its receipt will not either,
            // so unregister the waiter instead of letting it time out later.
            self.remove_slot(seq);
        }
        match write_result.and_then(|_| self.await_receipt(seq, rx, timeout)) {
            Ok(value) => {
                let elapsed = started.elapsed().as_millis() as u64;
                self.commands_total.fetch_add(1, AtomicOrdering::SeqCst);
                self.slowest_command_ms.fetch_max(elapsed, AtomicOrdering::SeqCst);
                if urgent {
                    self.release_all_rtt_ms
                        .store(started.elapsed().as_millis() as u64, AtomicOrdering::SeqCst);
                }
                // Every receipt carries the sidecar's current generation, so
                // the host resynchronises instead of replaying a rejection.
                if let Some(generation) = value["generation"].as_u64() {
                    self.set_generation(generation);
                }
                Ok((seq, value))
            }
            Err(error) => {
                if urgent {
                    log::warn!(
                        target: "cloudweb",
                        "CloudWeb urgent control write or receipt failed for {op}: {} {}",
                        error.code(),
                        error.message
                    );
                }
                Err(error)
            }
        }
    }

    /// A command round trip with the shared receipt interpretation. Used by
    /// the background paths (renewal thread, urgent ReleaseAll) that must not
    /// take the client lock.
    fn command(
        self: &Arc<Self>,
        op: &str,
        params: Value,
        observation: Option<&wire::Observation>,
        timeout: Duration,
        urgent: bool,
    ) -> Result<Option<Value>, SidecarError> {
        let (seq, value) = self.send_command(op, params, observation, timeout, urgent)?;
        interpret_receipt(&value, seq)
    }

    /// Lease renewal on the shared lease clock. Success stamps the renewal
    /// time; a lease loss or stale generation is counted, never replayed.
    fn renew_once(self: &Arc<Self>) -> Result<(), SidecarError> {
        let gap_ms = self.renewed_at_instant().map_or(0, |at| {
            at.elapsed().as_millis() as u64
        });
        self.lease
            .max_renew_gap_ms
            .fetch_max(gap_ms, AtomicOrdering::SeqCst);
        let result = self.command(
            "renew_lease",
            json!({ "owner_id": self.lease.owner_id }),
            None,
            CONTROL_TIMEOUT,
            false,
        );
        match result {
            Ok(_) => {
                self.lease
                    .renewed_at_epoch_ms
                    .store(epoch_ms_now(), AtomicOrdering::SeqCst);
                Ok(())
            }
            Err(error) => {
                self.lease
                    .renewed_at_epoch_ms
                    .store(-1, AtomicOrdering::SeqCst);
                if error.is_lease_loss() || error.is_generation_stale() {
                    self.lease.lease_losses.fetch_add(1, AtomicOrdering::SeqCst);
                }
                Err(error)
            }
        }
    }

    /// The urgent ReleaseAll attempt on the current mirrored generation. A
    /// `released: true` receipt fences the owner, so the lease clock is
    /// cleared here; a `generation_stale` rejection was never dispatched and
    /// the generation mirror is already resynchronised for the caller's one
    /// allowed retry.
    fn release_all_once(self: &Arc<Self>, reason: &str) -> Result<(), SidecarError> {
        let detail = self.command(
            "release_all",
            json!({ "reason": reason }),
            None,
            URGENT_RECEIPT_TIMEOUT,
            true,
        )?;
        if detail
            .as_ref()
            .and_then(|value| value["released"].as_bool())
            == Some(true)
        {
            self.lease
                .renewed_at_epoch_ms
                .store(-1, AtomicOrdering::SeqCst);
        }
        Ok(())
    }

    fn renewed_at_instant(&self) -> Option<Instant> {
        let epoch_ms = self
            .lease
            .renewed_at_epoch_ms
            .load(AtomicOrdering::SeqCst);
        if epoch_ms < 0 {
            None
        } else {
            Some(epoch_ms_to_instant(epoch_ms))
        }
    }
}

/// Shared receipt interpretation for every plane of the control link. The
/// caller applies its own error policy; the codes are stable for that mapping.
fn interpret_receipt(
    value: &Value,
    expected_seq: u64,
) -> Result<Option<Value>, SidecarError> {
    let kind = value["type"].as_str().unwrap_or_default();
    if kind == "error" {
        return Err(SidecarError::from_frame(value));
    }
    if kind != "receipt" {
        return Err(SidecarError::local(
            "cloudweb_protocol_shape",
            format!("expected a receipt, got {kind}"),
        ));
    }
    let receipt_seq = value["seq"].as_u64().unwrap_or_default();
    if receipt_seq != expected_seq {
        return Err(SidecarError::local(
            "cloudweb_seq_mismatch",
            format!(
                "receipt seq {receipt_seq} does not match command seq {expected_seq}"
            ),
        ));
    }
    match value["status"].as_str().unwrap_or_default() {
        "accepted" | "dispatched" => Ok(value.get("detail").cloned()),
        // Contract section 6: an unknown result must not be retried and the
        // remote input state stays possibly held on the sidecar side.
        "unknown" => Err(SidecarError::from_frame(value)),
        other => {
            let error = SidecarError::from_frame(value);
            let error = if other == "cancelled" {
                SidecarError::new("cancelled", error.message)
            } else {
                error
            };
            Err(error)
        }
    }
}

/// Launch parameters for one sidecar process.
#[derive(Clone)]
pub struct LaunchConfig {
    pub exec: PathBuf,
    pub extra_args: Vec<String>,
    /// Explicit inner actuator requested from the sidecar. The Host checks the
    /// handshake against this value instead of trusting its own adapter name.
    pub expected_backend: String,
    /// Development-only switch required before a `fake` handshake is accepted.
    pub allow_fake: bool,
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
            .field("expected_backend", &self.expected_backend)
            .field("allow_fake", &self.allow_fake)
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

fn explicit_backend(args: &[String], lookup: &dyn Fn(&str) -> Option<String>) -> Option<String> {
    let mut from_args = None;
    let mut iter = args.iter();
    while let Some(argument) = iter.next() {
        if argument == "--backend" {
            from_args = iter.next().cloned();
        } else if let Some(value) = argument.strip_prefix("--backend=") {
            from_args = Some(value.to_string());
        }
    }
    from_args
        .or_else(|| lookup(BACKEND_ENV))
        .map(|value| value.trim().to_ascii_lowercase())
        .filter(|value| !value.is_empty())
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
        // An outer `backend=sidecar` says nothing about the inner actuator.
        // Require the launch to name it, and reject the fake actuator unless a
        // development run explicitly opts in.
        let expected_backend = explicit_backend(&extra_args, lookup).ok_or_else(|| {
            SidecarError::local(
                "cloudweb_backend_required",
                "the sidecar launch must name --backend fake|cdp or set CLOUDWEB_BACKEND",
            )
        })?;
        match expected_backend.as_str() {
            "fake" | "cdp" => {}
            other => {
                return Err(SidecarError::local(
                    "cloudweb_backend_unsupported",
                    format!("unsupported sidecar backend {other}: expected fake or cdp"),
                ));
            }
        }
        let allow_fake = lookup(ALLOW_FAKE_ENV).is_some_and(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes"
            )
        });
        if expected_backend == "fake" && !allow_fake {
            return Err(SidecarError::local(
                "cloudweb_fake_backend_refused",
                "the fake sidecar actuator is a development backend; set CLOUDWEB_ALLOW_FAKE=1 to use it",
            ));
        }
        let token = random_hex(32)?;
        Ok(Self {
            exec,
            extra_args,
            expected_backend,
            allow_fake,
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
    /// Actuator actually built by the sidecar (`fake` or `cdp`).
    pub backend_kind: String,
    pub backend_version: String,
    /// Effective UI profile echoed by the sidecar.
    pub ui_profile: Option<String>,
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

/// Synchronous protocol client. The type keeps the host-side view of the
/// protocol state machine; every control-plane round trip goes through the
/// shared [`ControlLink`], which never holds this client's lock while waiting
/// for a receipt. The sidecar keeps the input ledger; this type only tracks
/// the host-side view of identity, generation and observation.
pub struct SidecarClient {
    link: Arc<ControlLink>,
    transport: Box<dyn FrameTransport>,
    token: String,
    frame_pipe: String,
    /// Host-side cache of the link's generation mirror. Only written through
    /// `set_generation` / `sync_generation` so it never disagrees with the
    /// value urgent paths read atomically.
    generation: u64,
    lease_ms: u64,
    renew_ms: u64,
    observation_max_age_ms: u64,
    capabilities: Vec<String>,
    backend_kind: String,
    backend_version: String,
    ui_profile: Option<String>,
    expected_backend: String,
    allow_fake: bool,
    state: String,
    observation: Option<CapturedFrame>,
}

impl SidecarClient {
    pub fn new(config: &LaunchConfig, parts: ControlParts) -> Self {
        let lease = Arc::new(LeaseState::new(config));
        let flags = Arc::new(SessionFlags::new());
        let link = ControlLink::new(parts.writer, parts.reader, lease, flags);
        Self {
            link,
            transport: parts.frame,
            token: config.token.clone(),
            frame_pipe: config.frame_pipe.clone(),
            generation: 0,
            lease_ms: FALLBACK_LEASE_MS,
            renew_ms: FALLBACK_RENEW_MS,
            observation_max_age_ms: FALLBACK_OBSERVATION_MS,
            capabilities: Vec::new(),
            backend_kind: String::new(),
            backend_version: String::new(),
            ui_profile: None,
            expected_backend: config.expected_backend.clone(),
            allow_fake: config.allow_fake,
            state: String::new(),
            observation: None,
        }
    }

    /// Successful control round trips and the slowest one, counters the link
    /// shares with the renewal and urgent paths.
    fn commands_total(&self) -> u64 {
        self.link
            .commands_total
            .load(AtomicOrdering::SeqCst)
    }

    fn slowest_command_ms(&self) -> u64 {
        self.link
            .slowest_command_ms
            .load(AtomicOrdering::SeqCst)
    }

    /// The shared control plane; the controller keeps this clone so urgent
    /// paths and the renewal thread never touch the client lock.
    pub fn link(&self) -> Arc<ControlLink> {
        Arc::clone(&self.link)
    }

    fn lease(&self) -> &LeaseState {
        &self.link.lease
    }

    fn set_generation(&mut self, generation: u64) {
        self.generation = generation;
        self.link.set_generation(generation);
    }

    fn sync_generation(&mut self) {
        self.generation = self.link.generation();
    }

    fn lease_held_at(&self) -> Option<Instant> {
        self.link.renewed_at_instant()
    }

    fn mark_renewed(&self) {
        self.lease()
            .renewed_at_epoch_ms
            .store(epoch_ms_now(), AtomicOrdering::SeqCst);
    }

    fn clear_renewed(&self) {
        self.lease()
            .renewed_at_epoch_ms
            .store(-1, AtomicOrdering::SeqCst);
    }

    /// Test/production seam for the renewal cadence; keeps the shared atomic
    /// mirror in sync for the background thread.
    fn set_renew_ms(&mut self, renew_ms: u64) {
        self.renew_ms = renew_ms;
        self.lease()
            .renew_ms
            .store(renew_ms, AtomicOrdering::SeqCst);
    }

    fn set_faulted(&self, value: bool) {
        self.link
            .flags
            .faulted
            .store(value, AtomicOrdering::SeqCst);
    }

    fn is_faulted_shared(&self) -> bool {
        self.link.flags.faulted.load(AtomicOrdering::SeqCst)
    }

    fn is_closed_shared(&self) -> bool {
        self.link.flags.closed.load(AtomicOrdering::SeqCst)
    }

    pub fn session_id(&self) -> &str {
        &self.link.lease.session_id
    }

    pub fn owner_id(&self) -> &str {
        &self.link.lease.owner_id
    }

    pub fn backend_epoch(&self) -> &str {
        &self.link.lease.backend_epoch
    }

    pub fn has_capability(&self, name: &str) -> bool {
        self.capabilities.iter().any(|value| value == name)
    }

    pub fn is_faulted(&self) -> bool {
        self.is_faulted_shared()
    }

    pub fn is_closed(&self) -> bool {
        self.is_closed_shared()
    }

    /// Whether the host currently believes it holds an owner lease. The
    /// periodic renewal thread reads the same shared clock.
    pub fn lease_held(&self) -> bool {
        self.lease_held_at().is_some()
    }

    pub fn max_renew_gap_ms(&self) -> u64 {
        self.lease()
            .max_renew_gap_ms
            .load(AtomicOrdering::SeqCst)
    }

    pub fn lease_losses(&self) -> u64 {
        self.lease().lease_losses.load(AtomicOrdering::SeqCst)
    }

    pub fn observation_age_ms(&self) -> Option<u64> {
        self.observation
            .as_ref()
            .map(|frame| frame.captured_at.elapsed().as_millis() as u64)
    }

    /// Sends `hello` and validates `hello_ok`. Every failure keeps an explicit
    /// error code; there is no fallback to another backend.
    pub fn handshake(&mut self) -> Result<HelloInfo, SidecarError> {
        if self.link.flags.handshaked.load(AtomicOrdering::SeqCst) {
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
        // The reader thread has not started, so the hello exchange stays a
        // raw synchronous write + read on the link halves.
        self.link.write_frame(&hello)?;
        let value = self.link.read_raw()?.ok_or_else(|| {
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
        if epoch != self.backend_epoch() {
            return Err(self.fail(SidecarError::local(
                "cloudweb_epoch_mismatch",
                "sidecar backend epoch does not match the launched process",
            )));
        }
        // The inner actuator is a fact reported by the sidecar, not something
        // the Host may infer from `implementation=sidecar` or its own args.
        let backend_kind = value["backend_kind"]
            .as_str()
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase();
        if backend_kind.is_empty() {
            return Err(self.fail(SidecarError::local(
                "cloudweb_backend_kind_missing",
                "hello_ok did not report the actual backend kind",
            )));
        }
        if backend_kind != self.expected_backend {
            return Err(self.fail(SidecarError::local(
                "cloudweb_backend_kind_mismatch",
                format!(
                    "sidecar reported backend {backend_kind}, launch requested {}",
                    self.expected_backend
                ),
            )));
        }
        if backend_kind == "fake" && !self.allow_fake {
            return Err(self.fail(SidecarError::local(
                "cloudweb_fake_backend_refused",
                "the fake sidecar actuator is a development backend; set CLOUDWEB_ALLOW_FAKE=1 to use it",
            )));
        }
        let ui_profile = value["ui_profile"].as_str().map(str::to_string);
        if ui_profile.as_deref() != Some(UI_PROFILE) {
            return Err(self.fail(SidecarError::local(
                "cloudweb_profile_mismatch",
                format!(
                    "sidecar reported profile {:?}, expected {UI_PROFILE}",
                    ui_profile
                ),
            )));
        }
        let state = value["state"].as_str().unwrap_or_default().to_string();
        if state == "closed" || state == "faulted" {
            return Err(self.fail(SidecarError::local(
                "cloudweb_sidecar_not_ready",
                format!("sidecar reported state {state}"),
            )));
        }

        self.link
            .flags
            .handshaked
            .store(true, AtomicOrdering::SeqCst);
        self.set_generation(value["generation"].as_u64().unwrap_or_default());
        self.backend_kind = backend_kind;
        self.ui_profile = ui_profile;
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
            self.set_renew_ms(limits["renew_ms"].as_u64().unwrap_or(self.renew_ms));
            self.observation_max_age_ms = limits["observation_max_age_ms"]
                .as_u64()
                .unwrap_or(self.observation_max_age_ms);
        }
        self.state = state.clone();
        Ok(HelloInfo {
            backend_kind: self.backend_kind.clone(),
            backend_version: self.backend_version.clone(),
            ui_profile: self.ui_profile.clone(),
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
        // On a slow cloud every command can take longer than the default
        // lease; ask for a bounded longer lease so the next command does not
        // arrive after expiry and churn the generation.
        let observed = self.slowest_command_ms().max(100);
        let requested = observed
            .saturating_mul(3)
            .clamp(FALLBACK_LEASE_MS, MAX_REQUESTED_LEASE_MS);
        let detail = self.command(
            "acquire_owner",
            json!({ "owner_id": self.owner_id(), "lease_ms": requested }),
        )?;
        let lease_ms = detail
            .as_ref()
            .and_then(|value| value["lease_ms"].as_u64())
            .unwrap_or(self.lease_ms);
        self.lease_ms = lease_ms;
        // Renew at half the granted lease; the renewal itself travels one
        // RTT, so the other half is the safety budget. Use the setter so the
        // hot-path lease checks and the shared renewal thread agree.
        self.set_renew_ms((lease_ms / 2).max(50));
        log::info!(
            target: "cloudweb",
            "CloudWeb owner acquired: session_id={} generation={} lease_ms={} requested_ms={}",
            self.session_id(),
            self.generation,
            lease_ms,
            requested
        );
        self.mark_renewed();
        Ok(())
    }

    /// Renews the lease through the shared link. When the sidecar reports the
    /// lease is gone the owner is dropped locally on the shared lease clock
    /// so the next acquisition starts a new generation; the rejected command
    /// is never replayed.
    pub fn renew_lease(&mut self) -> Result<(), SidecarError> {
        let result = self.link.renew_once();
        self.sync_generation();
        result.map_err(|error| self.classify(error))
    }

    /// Refreshes an already held lease. Frames and preview do not need
    /// ownership, so a session without an owner is left untouched here.
    fn keep_lease_warm(&mut self) {
        let due = self
            .lease_held_at()
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
    /// and recover a new generation when the lease was fenced while the pipe
    /// was busy.
    fn ensure_lease(&mut self) -> Result<(), SidecarError> {
        let renewed_at = self.lease_held_at();
        let due = match renewed_at {
            None => true,
            Some(at) => at.elapsed() >= Duration::from_millis(self.renew_ms),
        };
        if !due {
            return Ok(());
        }
        if renewed_at.is_some() {
            return match self.renew_lease() {
                Ok(()) => Ok(()),
                Err(error) if error.is_lease_loss() || error.is_generation_stale() => {
                    log::warn!(
                        target: "cloudweb",
                        "CloudWeb lease lost before renewal, acquiring a new generation: {} (max gap {} ms)",
                        error.code(),
                        self.max_renew_gap_ms()
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
    /// Captures a fresh frame, waiting out a short `frame_not_fresh` window.
    /// A generation bump or a slow poll can leave the slot stale for tens of
    /// milliseconds; a failed observation is not an input error yet.
    pub fn capture_fresh(&mut self) -> Result<CapturedFrame, SidecarError> {
        let deadline = Instant::now() + FRAME_STALE_RETRY;
        loop {
            match self.capture() {
                Ok(frame) => return Ok(frame),
                Err(error)
                    if error.code() == "cloudweb_frame_not_fresh"
                        && Instant::now() < deadline =>
                {
                    std::thread::sleep(Duration::from_millis(50));
                }
                Err(error) => return Err(error),
            }
        }
    }

    pub fn capture(&mut self) -> Result<CapturedFrame, SidecarError> {
        self.keep_lease_warm();
        let mut reconnects = 0;
        loop {
            match self.capture_once() {
                Ok(frame) => return Ok(frame),
                Err(error) => {
                    // The sidecar closes a frame connection that went
                    // undrained past its write budget. Reopen the plane and
                    // request a fresh id; the old one is no longer queued.
                    let reopenable = self.transport.can_reconnect_frame()
                        && reconnects < FRAME_RECONNECTS
                        && matches!(
                            error.code(),
                            "cloudweb_frame_closed"
                                | "cloudweb_frame_desynced"
                                | "cloudweb_frame_timeout"
                        );
                    if reopenable && self.transport.reconnect_frame(&self.frame_pipe).is_ok() {
                        reconnects += 1;
                        log::warn!(
                            target: "cloudweb",
                            "CloudWeb frame plane {}; reopened (attempt {})",
                            error.code(),
                            reconnects
                        );
                        continue;
                    }
                    return Err(error);
                }
            }
        }
    }

    fn capture_once(&mut self) -> Result<CapturedFrame, SidecarError> {
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
        if self.lease_held() {
            let _ = self.renew_after_slow_command();
        }
        Ok(frame)
    }

    fn renew_after_slow_command(&mut self) -> Result<(), SidecarError> {
        let overdue = self
            .lease_held_at()
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
                    self.max_renew_gap_ms(),
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

    /// MaaFramework PC contacts are semantic buttons, not ADB finger ids:
    /// 0 left, 1 right, 2 middle. The sidecar wire order is 0 left, 1 middle,
    /// 2 right, so the right/middle ids must be swapped explicitly.
    fn contact_button(contact: i32) -> Result<u8, SidecarError> {
        match contact {
            0 => Ok(0),
            1 => Ok(2),
            2 => Ok(1),
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
            match self.observation_for_input() {
                Ok(observation) => observation,
                Err(error) if error.code() == "cloudweb_observation_expired" => {
                    // A slow command (recognition, menu transition) can age the
                    // observation past its budget before the action runs.
                    // Take one fresh frame and re-check; the action still
                    // names that new frame.
                    log::debug!(
                        target: "cloudweb",
                        "CloudWeb observation expired before input; refreshing once"
                    );
                    self.capture_fresh()?;
                    self.observation_for_input()?
                }
                Err(error) => return Err(error),
            }
        };
        if observation.is_none() && !safety {
            return Err(self.record(SidecarError::local(
                "cloudweb_no_observation",
                format!("{op} needs a fresh observation; run screencap first"),
            )));
        }

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
                self.capture_fresh()?;
                let observation = self.observation_for_input()?;
                // The link records the whole retry window in its own
                // slowest-command counter.
                let retry = self.send(op, params, observation.as_ref());
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
        let button = Self::contact_button(contact)?;
        self.input(
            "mouse_down",
            json!({ "button": button, "x": x, "y": y }),
            false,
        )
    }

    pub fn release_button(&mut self, contact: i32) -> Result<(), SidecarError> {
        let button = Self::contact_button(contact)?;
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
    /// held key/button before returning ownership. The controller's Stop path
    /// now goes through the urgent ReleaseAll lane; this command is kept as
    /// the owner-authority protocol operation exercised by the unit tests.
    #[allow(dead_code)]
    pub fn release_owner(&mut self) -> Result<(), SidecarError> {
        let result = self.command(
            "release_owner",
            json!({ "owner_id": self.owner_id() }),
        );
        self.clear_renewed();
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
        if self.is_closed() {
            return Ok(());
        }
        let result = self.command(
            "close",
            json!({ "mode": "session", "reason": "host_close" }),
        );
        self.link
            .flags
            .closed
            .store(true, AtomicOrdering::SeqCst);
        self.clear_renewed();
        self.observation = None;
        match result {
            Ok(_) => {
                self.state = "closed".to_string();
                Ok(())
            }
            Err(error) => Err(error),
        }
    }

    /// Reads the sidecar's authoritative health, including the held-input
    /// ledger and lease remaining. This is separate from `diagnostics`, which
    /// is the host-side view.
    pub fn health(&mut self) -> Result<Value, SidecarError> {
        Ok(self
            .command("get_health", json!({}))?
            .unwrap_or(Value::Null))
    }

    pub fn diagnostics(&self) -> Value {
        json!({
            "backend": "cloud_web",
            "implementation": "sidecar",
            "protocol_version": wire::PROTOCOL_MAJOR,
            "backend_kind": self.backend_kind,
            "expected_backend": self.expected_backend,
            "backend_version": self.backend_version,
            "ui_profile": self.ui_profile,
            "session_id": self.session_id(),
            "backend_epoch": self.backend_epoch(),
            "generation": self.generation,
            "state": self.state,
            "capabilities": self.capabilities,
            "owner_lease": {
                "held": self.lease_held(),
                "lease_ms": self.lease_ms,
                "renew_ms": self.renew_ms,
                "max_renew_gap_ms": self.max_renew_gap_ms(),
                "lease_losses": self.lease_losses(),
            },
            "observation_max_age_ms": self.observation_max_age_ms,
            "observation_age_ms": self.observation_age_ms(),
            "commands": self.commands_total(),
            "slowest_command_ms": self.slowest_command_ms(),
            "last_error": self.link.flags.last_error(),
            // CW2 D1 evidence: the urgent lane never waits behind a normal
            // command, so these describe the Stop write and receipt cost.
            "control_link": {
                "release_all_rtt_ms": self
                    .link
                    .release_all_rtt_ms
                    .load(AtomicOrdering::SeqCst),
                "urgent_write_ms": self
                    .link
                    .urgent_write_ms
                    .load(AtomicOrdering::SeqCst),
                "pending": self.link.pending_count(),
            },
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
        if self.is_closed() {
            return Err(SidecarError::local(
                "cloudweb_closed",
                "the CloudWeb session is closed",
            ));
        }
        // Close is the one command that may still run on a faulted session.
        if self.is_faulted() && op != "close" {
            return Err(SidecarError::local(
                "cloudweb_faulted",
                "the CloudWeb session is faulted; close and relaunch are required",
            ));
        }
        if !self.link.flags.handshaked.load(AtomicOrdering::SeqCst) {
            return Err(SidecarError::local(
                "cloudweb_not_handshaked",
                "the CloudWeb handshake has not completed",
            ));
        }

        // The link allocates the seq, writes the whole frame under a short
        // writer lock and routes the receipt by seq. Waiting here holds no
        // lock the urgent Stop path needs (CW2 D1).
        let (seq, value) = match self
            .link
            .send_command(op, params, observation, CONTROL_TIMEOUT, false)
        {
            Ok(pair) => pair,
            Err(error) => return Err(self.classify(error)),
        };
        // send_command already adopted the receipt's generation into the
        // shared mirror; refresh the client-side cache to match.
        self.sync_generation();

        match interpret_receipt(&value, seq) {
            Ok(detail) => Ok(detail),
            Err(error) => {
                // A terminal `error` frame for a live seq faults the session;
                // rejected/unknown/shape/seq failures only record it.
                if value["type"].as_str() == Some("error") {
                    return Err(self.fail(error));
                }
                Err(self.record(error))
            }
        }
    }

    /// Marks the session unusable when the sidecar reports a terminal failure.
    fn record(&mut self, error: SidecarError) -> SidecarError {
        if error.is_faulted() || error.code == "cloudweb_sidecar_not_ready" {
            self.set_faulted(true);
        }
        self.link.flags.record_error(&error);
        error
    }

    fn fail(&mut self, error: SidecarError) -> SidecarError {
        self.set_faulted(true);
        self.link.flags.record_error(&error);
        error
    }

    fn fail_local(&mut self, code: &str, message: impl Into<String>) -> SidecarError {
        self.record(SidecarError::local(code, message))
    }

    /// Maps link-level failures onto the fatal/non-fatal policy. A closed
    /// pipe or a read/decode failure faults the session; a per-command wait
    /// timeout does not: the receipt for a slow command is later discarded by
    /// the reader's seq routing instead of shifting every following receipt
    /// (the CW2 D1 multiplexing removes the old desync that made timeouts
    /// fatal). Everything else only records the last error.
    fn classify(&mut self, error: SidecarError) -> SidecarError {
        let fatal = matches!(
            error.code(),
            "cloudweb_control_closed"
                | "cloudweb_control_read"
                | "cloudweb_control_reader_spawn_failed"
                | "control_decode"
                | "control_size"
        );
        if fatal {
            self.fail(error)
        } else {
            self.record(error)
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

/// Host-side immutable snapshot of the last screencap. Preview and rapid
/// recognition callbacks read this instead of issuing duplicate sidecar
/// captures; the sidecar's frame_id is the snapshot token.
#[cfg(windows)]
struct CachedFrame {
    bytes: Vec<u8>,
    frame_id: u64,
    geometry_revision: u64,
    captured_at: Instant,
}

#[cfg(windows)]
const FRAME_CACHE_TTL: Duration = Duration::from_millis(100);
/// A stale rVFC slot can recover after the page collector re-arms; give the
/// host capture a short grace instead of failing the whole recognition burst.
const FRAME_STALE_RETRY: Duration = Duration::from_millis(5000);

/// CloudWeb controller backed by a launched sidecar process.
#[cfg(windows)]
pub struct SidecarCloudWebController {
    config: LaunchConfig,
    /// Client protocol state. Long operations may hold this mutex while they
    /// wait for a receipt; the renewal thread and the urgent Stop path go
    /// through `link` instead and never wait here (CW2 D1).
    client: Arc<Mutex<Option<SidecarClient>>>,
    /// Clone of the current session's control plane, kept so Stop can write
    /// a ReleaseAll frame without touching the client mutex at all.
    link: Mutex<Option<Arc<ControlLink>>>,
    child: Mutex<Option<std::process::Child>>,
    /// Windows job object owning the sidecar child. KILL_ON_JOB_CLOSE makes
    /// the OS terminate the sidecar when this host process dies.
    #[cfg(windows)]
    job: Mutex<Option<isize>>,
    /// Machine-readable reason of the last failed connect, surfaced through
    /// `get_info` so a rejected connection is not opaque to the UI.
    connect_error: Mutex<Option<SidecarError>>,
    /// One reader owns capture; preview and recognition share the snapshot.
    frame_cache: Mutex<Option<CachedFrame>>,
    capture_gate: Mutex<()>,
    screencap_calls: AtomicU64,
    cache_hits: AtomicU64,
    connected: AtomicBool,
    renew_shutdown: Arc<AtomicBool>,
    renew_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
    uuid: String,
}

#[cfg(windows)]
impl SidecarCloudWebController {
    pub fn new(config: LaunchConfig) -> Self {
        let uuid = stable_uuid(&format!("sidecar:{}:{}", config.session_id, config.profile));
        Self {
            config,
            client: Arc::new(Mutex::new(None)),
            link: Mutex::new(None),
            child: Mutex::new(None),
            #[cfg(windows)]
            job: Mutex::new(None),
            connect_error: Mutex::new(None),
            frame_cache: Mutex::new(None),
            capture_gate: Mutex::new(()),
            screencap_calls: AtomicU64::new(0),
            cache_hits: AtomicU64::new(0),
            connected: AtomicBool::new(false),
            renew_shutdown: Arc::new(AtomicBool::new(false)),
            renew_thread: Mutex::new(None),
            uuid,
        }
    }

    /// Starts the host-side renewal timer. This is the Host half of CW2's
    /// independent renewal: MaaFramework may hold a key down for seconds
    /// without issuing another controller callback, so relying on input
    /// callbacks alone would let the 350 ms sidecar lease expire. Since CW2
    /// D1 the timer drives the shared [`ControlLink`] directly: renewal is
    /// routed by the link reader and never queues behind the client lock a
    /// slow command holds.
    fn start_renew_thread(&self) {
        self.stop_renew_thread();
        self.renew_shutdown.store(false, AtomicOrdering::SeqCst);
        let Some(link) = self.current_link() else {
            log::warn!(
                target: "cloudweb",
                "CloudWeb renewal thread not started: the session has no control link"
            );
            return;
        };
        let shutdown = Arc::clone(&self.renew_shutdown);
        let spawned = std::thread::Builder::new()
            .name("cloudweb-lease-renew".to_string())
            .spawn(move || {
                loop {
                    if shutdown.load(AtomicOrdering::SeqCst) {
                        return;
                    }
                    let interval = link.renew_interval();

                    // Sleep in bounded slices so shutdown/close is responsive
                    // even when the configured renewal interval is large.
                    let mut slept = Duration::ZERO;
                    while slept < interval {
                        if shutdown.load(AtomicOrdering::SeqCst) {
                            return;
                        }
                        let slice = (interval - slept).min(Duration::from_millis(25));
                        std::thread::sleep(slice);
                        slept += slice;
                    }
                    if shutdown.load(AtomicOrdering::SeqCst) {
                        return;
                    }

                    if link.is_closed() || link.is_faulted() {
                        return;
                    }
                    if !link.lease_held() {
                        continue;
                    }
                    match link.renew_once() {
                        Ok(()) => {}
                        Err(error) if error.is_lease_loss() || error.is_generation_stale() => {
                            log::warn!(
                                target: "cloudweb",
                                "CloudWeb lease renewal lost ownership: {} {}",
                                error.code(),
                                error.message
                            );
                        }
                        Err(error) => {
                            log::warn!(
                                target: "cloudweb",
                                "CloudWeb lease renewal failed: {} {}",
                                error.code(),
                                error.message
                            );
                        }
                    }
                }
            });
        match spawned {
            Ok(handle) => {
                *self
                    .renew_thread
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(handle);
            }
            Err(error) => log::warn!(
                target: "cloudweb",
                "failed to start the CloudWeb lease renewal thread: {error}"
            ),
        }
    }

    fn stop_renew_thread(&self) {
        self.renew_shutdown.store(true, AtomicOrdering::SeqCst);
        let handle = self
            .renew_thread
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
        if let Some(handle) = handle {
            if handle.join().is_err() {
                log::warn!(target: "cloudweb", "CloudWeb lease renewal thread panicked");
            }
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
    fn launch(&self) -> Result<SidecarClient, SidecarError> {
        use std::io::BufRead;

        let mut command = Command::new(&self.config.exec);
        command
            .args(self.config.arguments())
            .env(TOKEN_ENV, self.config.token())
            .env("CLOUDWEB_PARENT_PID", std::process::id().to_string())
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped());
        // The sidecar is a console-subsystem binary. Without this flag Windows
        // allocates a console (the user's default terminal is Windows
        // Terminal), which pops up and steals foreground on every launch.
        // Piped stdio above keeps stderr logging intact.
        #[cfg(windows)]
        {
            use std::os::windows::process::CommandExt;
            const CREATE_NO_WINDOW: u32 = 0x0800_0000;
            command.creation_flags(CREATE_NO_WINDOW);
        }
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
        #[cfg(windows)]
        {
            let job = job_object::assign(&child);
            *self
                .job
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()) = job;
        }
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

        match pipe_connect_parts(&self.config) {
            Ok(parts) => Ok(SidecarClient::new(&self.config, parts)),
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
        let mut reported_backend = None;
        for token in line.split_whitespace() {
            let Some((key, value)) = token.split_once('=') else {
                continue;
            };
            match key {
                "session_id" => reported_session = Some(value),
                "backend_epoch" => reported_epoch = Some(value),
                "backend" => reported_backend = Some(value),
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
        let reported_backend = reported_backend
            .map(str::to_ascii_lowercase)
            .unwrap_or_default();
        if reported_backend != self.config.expected_backend {
            return Err(SidecarError::local(
                "cloudweb_backend_identity_mismatch",
                format!(
                    "the sidecar reported backend {reported_backend}, launch requested {}",
                    self.config.expected_backend
                ),
            ));
        }
        Ok(())
    }

    fn lock_child(&self) -> std::sync::MutexGuard<'_, Option<std::process::Child>> {
        self.child
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Current session's control link, if installed. Locking this mutex is
    /// never done across a receipt wait, so the urgent paths grab it freely.
    fn lock_link(&self) -> std::sync::MutexGuard<'_, Option<Arc<ControlLink>>> {
        self.link
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn current_link(&self) -> Option<Arc<ControlLink>> {
        self.lock_link().clone()
    }

    /// Drops the session's link registration so no urgent path can write to
    /// a torn-down session. The reader thread ends via pipe EOF.
    fn drop_session_link(&self) {
        *self.lock_link() = None;
    }

    /// CW2 D1 Stop path: releases every held input through the urgent lane.
    /// It takes only the short link slot and the link's own atomics — never
    /// the client mutex a long normal command holds while awaiting its
    /// receipt. A `generation_stale` rejection was never dispatched, so one
    /// retry on the resynchronised generation is allowed; a timeout is
    /// reported and never replayed (the sidecar ledger stays authoritative).
    pub fn release_all_urgent(&self, reason: &str) -> Result<u64, SidecarError> {
        let link = self.current_link().ok_or_else(|| {
            SidecarError::local(
                "cloudweb_not_connected",
                "the CloudWeb session is not connected; cannot run release_all",
            )
        })?;
        let first = link.release_all_once(reason);
        let error = match first {
            Ok(()) => {
                let rtt = link
                    .release_all_rtt_ms
                    .load(AtomicOrdering::SeqCst);
                log::info!(
                    target: "cloudweb",
                    "CloudWeb urgent ReleaseAll acknowledged: rtt_ms={rtt} write_ms={}",
                    link.urgent_write_ms.load(AtomicOrdering::SeqCst)
                );
                return Ok(rtt);
            }
            Err(error) => error,
        };
        if error.is_generation_stale() {
            log::debug!(
                target: "cloudweb",
                "CloudWeb urgent ReleaseAll met a stale generation, retrying once on the resynchronised generation"
            );
            return match link.release_all_once(reason) {
                Ok(()) => {
                    let rtt = link
                        .release_all_rtt_ms
                        .load(AtomicOrdering::SeqCst);
                    log::info!(
                        target: "cloudweb",
                        "CloudWeb urgent ReleaseAll acknowledged after retry: rtt_ms={rtt}"
                    );
                    Ok(rtt)
                }
                Err(retry_error) => {
                    if retry_error.code() == "cloudweb_control_wait_timeout" {
                        log::warn!(
                            target: "cloudweb",
                            "cloudweb_release_all_timeout after retry: ReleaseAll was not acknowledged; not replaying"
                        );
                    }
                    Err(retry_error)
                }
            };
        }
        if error.code() == "cloudweb_control_wait_timeout" {
            log::warn!(
                target: "cloudweb",
                "cloudweb_release_all_timeout: ReleaseAll was not acknowledged; not replaying"
            );
        }
        Err(error)
    }

    /// Kills and reaps the registered child. This is the deterministic recycle
    /// path; a host that dies before it runs is covered by the job object the
    /// spawn assigned to the sidecar (`KILL_ON_JOB_CLOSE`), so no child
    /// survives a hard host death either.
    fn terminate_child(&self) {
        let mut slot = self.lock_child();
        let Some(child) = slot.as_mut() else {
            return;
        };
        reap(child);
        *slot = None;
        #[cfg(windows)]
        {
            let mut job = self
                .job
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if let Some(handle) = job.take() {
                job_object::close(handle);
            }
        }
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
        action: impl FnOnce(&mut SidecarClient) -> Result<R, SidecarError>,
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

/// Job object so the sidecar cannot outlive the host process.
#[cfg(windows)]
mod job_object {
    use std::os::windows::io::AsRawHandle;

    #[repr(C)]
    struct BasicLimit {
        per_process_user_time_limit: i64,
        per_job_user_time_limit: i64,
        limit_flags: u32,
        minimum_working_set_size: usize,
        maximum_working_set_size: usize,
        active_process_limit: u32,
        affinity: usize,
        priority_class: u32,
        scheduling_class: u32,
    }

    #[repr(C)]
    struct IoCounters {
        read_operation_count: u64,
        write_operation_count: u64,
        other_operation_count: u64,
        read_transfer_count: u64,
        write_transfer_count: u64,
        other_transfer_count: u64,
    }

    #[repr(C)]
    struct ExtendedLimit {
        basic: BasicLimit,
        io_info: IoCounters,
        process_memory_limit: usize,
        job_memory_limit: usize,
        peak_process_memory_used: usize,
        peak_job_memory_used: usize,
    }

    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn CreateJobObjectW(attributes: *mut core::ffi::c_void, name: *const u16) -> isize;
        fn SetInformationJobObject(
            job: isize,
            class: i32,
            info: *const core::ffi::c_void,
            length: u32,
        ) -> i32;
        fn AssignProcessToJobObject(job: isize, process: isize) -> i32;
        fn CloseHandle(handle: isize) -> i32;
    }

    const JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE: u32 = 0x0000_2000;
    const JOB_OBJECT_EXTENDED_LIMIT_INFORMATION: i32 = 9;

    pub fn assign(child: &std::process::Child) -> Option<isize> {
        unsafe {
            let job = CreateJobObjectW(std::ptr::null_mut(), std::ptr::null());
            if job == 0 {
                log::warn!(target: "cloudweb", "job object create failed");
                return None;
            }
            let mut info: ExtendedLimit = std::mem::zeroed();
            info.basic.limit_flags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
            let size = std::mem::size_of::<ExtendedLimit>() as u32;
            if SetInformationJobObject(
                job,
                JOB_OBJECT_EXTENDED_LIMIT_INFORMATION,
                &info as *const _ as *const core::ffi::c_void,
                size,
            ) == 0
            {
                log::warn!(target: "cloudweb", "job object set information failed");
                CloseHandle(job);
                return None;
            }
            if AssignProcessToJobObject(job, child.as_raw_handle() as isize) == 0 {
                log::warn!(target: "cloudweb", "job object assign failed");
                CloseHandle(job);
                return None;
            }
            Some(job)
        }
    }

    pub fn close(handle: isize) {
        unsafe {
            CloseHandle(handle);
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
        self.stop_renew_thread();
        // Any previous session's link is unregistered first so urgent paths
        // cannot write into a dying pipe.
        self.drop_session_link();
        self.frame_cache
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
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
                "CloudWeb sidecar handshaked: session_id={} generation={} state={} backend_kind={} backend_version={} profile={:?} lease_ms={}",
                client.session_id(),
                info.generation,
                info.state,
                info.backend_kind,
                info.backend_version,
                info.ui_profile,
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
            Err(error) => {
                log::info!(
                    target: "cloudweb",
                    "CloudWeb session connected without a ready frame: {} {}",
                    error.code(),
                    error.message
                );
                // A control-wait timeout faults the client (send() -> fail()).
                // Never install a faulted session as connected: every later
                // command would be rejected. Discard it and let the caller
                // report a failed connect instead.
                if client.is_faulted() || error.code() == "cloudweb_control_wait_timeout" {
                    self.set_connect_error(&error);
                    drop(client);
                    self.terminate_child();
                    return false;
                }
            }
        }
        *self
            .connect_error
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = None;
        // Register the control link before the renewal thread starts: the
        // thread and release_all_urgent use this clone, not the client mutex.
        let link = client.link();
        *self.lock_link() = Some(link);
        *self
            .client
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(client);
        self.connected.store(true, AtomicOrdering::SeqCst);
        self.start_renew_thread();
        true
    }

    fn connected(&self) -> bool {
        if !self.connected.load(AtomicOrdering::SeqCst) {
            return false;
        }
        // A faulted or closed session is not connected: report it so the
        // framework reconnects instead of pushing input into a dead session.
        // Never wait for the client lock here: this callback runs from the
        // MaaFramework controller and the state HTTP handler, and a long
        // screencap can hold the lock for seconds. A busy client is still
        // connected; the next command reports a fault if there is one.
        match self.client.try_lock() {
            Ok(guard) => match guard.as_ref() {
                Some(client) => !(client.is_faulted() || client.is_closed()),
                None => false,
            },
            Err(_) => true,
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
        // One reader owns capture. Preview and recognition callbacks that
        // arrive within the TTL share the immutable snapshot instead of
        // issuing duplicate sidecar captures.
        let _gate = self
            .capture_gate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        self.screencap_calls.fetch_add(1, AtomicOrdering::SeqCst);
        {
            let cache = self
                .frame_cache
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if let Some(cached) = cache.as_ref() {
                if cached.captured_at.elapsed() <= FRAME_CACHE_TTL {
                    self.cache_hits.fetch_add(1, AtomicOrdering::SeqCst);
                    return Some(cached.bytes.clone());
                }
            }
        }
        // with_client already logs the code and message; the framework only
        // sees the Option, so the log is the only place a regression is visible.
        let frame = match self.with_client("screencap", |client| client.capture_fresh()) {
            Ok(frame) => frame,
            Err(_) => return None,
        };
        let bytes = frame.bytes.clone();
        *self
            .frame_cache
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(CachedFrame {
            bytes: frame.bytes,
            frame_id: frame.frame_id,
            geometry_revision: frame.geometry_revision,
            captured_at: frame.captured_at,
        });
        Some(bytes)
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
        // CW2 D1: Stop releases held inputs through the urgent lane so a
        // long normal command cannot delay the frame write. ReleaseAll uses
        // the sidecar's cleanup authority: the same fence-and-release effect
        // as ReleaseOwner, and it also works after the lease has expired.
        // An unconfirmed release is reported as a failure: the remote held
        // state is then unknown and only the ledger is authoritative.
        let released = match self.release_all_urgent("host_stop") {
            Ok(_rtt_ms) => true,
            Err(error) => {
                log::warn!(
                    target: "cloudweb",
                    "CloudWeb urgent ReleaseAll failed: {} {}",
                    error.code(),
                    error.message
                );
                false
            }
        };
        // The snapshot belongs to the released observation; do not serve it
        // to a later owner/generation.
        self.frame_cache
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
        released
    }

    fn get_info(&self) -> String {
        // Never queue behind a long command: MaaFramework asks for info on
        // its controller path, and waiting here used to hold up the state
        // API and the task itself. When the client is busy, report the host
        // counters and let `health` be observed on the next idle call.
        let mut guard = match self.client.try_lock() {
            Ok(guard) => guard,
            Err(_) => {
                return json!({
                    "backend": "cloud_web",
                    "implementation": "sidecar",
                    "busy": true,
                    "host_frame_cache": {
                        "calls": self.screencap_calls.load(AtomicOrdering::SeqCst),
                        "hits": self.cache_hits.load(AtomicOrdering::SeqCst),
                    },
                })
                .to_string();
            }
        };
        match guard.as_mut() {
            Some(client) => {
                let mut diagnostics = client.diagnostics();
                if let Ok(health) = client.health() {
                    diagnostics["health"] = health;
                }
                let (cached_id, cached_geometry, cached_age) = {
                    let cache = self
                        .frame_cache
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                    match cache.as_ref() {
                        Some(cached) => (
                            json!(cached.frame_id),
                            json!(cached.geometry_revision),
                            json!(cached.captured_at.elapsed().as_millis() as u64),
                        ),
                        None => (Value::Null, Value::Null, Value::Null),
                    }
                };
                diagnostics["host_frame_cache"] = json!({
                    "calls": self.screencap_calls.load(AtomicOrdering::SeqCst),
                    "hits": self.cache_hits.load(AtomicOrdering::SeqCst),
                    "frame_id": cached_id,
                    "geometry_revision": cached_geometry,
                    "age_ms": cached_age,
                });
                diagnostics
            }
            None => {
                let error = self.take_connect_error();
                json!({
                    "backend": "cloud_web",
                    "implementation": "sidecar",
                    "protocol_version": wire::PROTOCOL_MAJOR,
                    "backend_kind": Value::Null,
                    "expected_backend": self.config.expected_backend,
                    "ui_profile": Value::Null,
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
        // child is reaped. Stop the renewal timer before taking the client so
        // the join cannot race a renewal that would keep the pipe alive.
        self.renew_shutdown.store(true, AtomicOrdering::SeqCst);
        self.frame_cache
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
        // Unregister the link from urgent paths, but keep the clone so the
        // close below still uses the live pipes; mark it closed only after
        // the attempt so teardown can never hang on a receipt that will not
        // arrive.
        let link = self.lock_link().take();
        let client = self
            .client
            .lock()
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
        if let Some(link) = link {
            link.mark_closed();
        }
        self.stop_renew_thread();
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

/// Windows named-pipe transport, split into its three plane halves so the
/// control reader thread, the frame-lock writer and the client-side frame
/// reader never share a lock. Each call drives the shared current-thread
/// runtime because MaaFramework invokes controller callbacks on its own
/// threads; `block_on` from multiple threads on the same runtime is the
/// supported way to fan out over one pipe.
#[cfg(windows)]
pub struct PipeControlWriter {
    runtime: Arc<tokio::runtime::Runtime>,
    sink: tokio::io::WriteHalf<tokio::net::windows::named_pipe::NamedPipeClient>,
}

#[cfg(windows)]
pub struct PipeControlReader {
    runtime: Arc<tokio::runtime::Runtime>,
    stream: tokio::io::ReadHalf<tokio::net::windows::named_pipe::NamedPipeClient>,
}

#[cfg(windows)]
pub struct PipeFrameTransport {
    runtime: Arc<tokio::runtime::Runtime>,
    frame: tokio::net::windows::named_pipe::NamedPipeClient,
}

/// `Runtime::block_on` panics inside another runtime, so the callback
/// threads are checked instead.
#[cfg(windows)]
fn guard_pipe_context() -> Result<(), SidecarError> {
    if tokio::runtime::Handle::try_current().is_ok() {
        return Err(SidecarError::local(
            "cloudweb_blocking_context",
            "the sidecar transport cannot block inside an async runtime",
        ));
    }
    Ok(())
}

/// Opens both pipes and hands back the three transport halves.
#[cfg(windows)]
pub fn pipe_connect_parts(config: &LaunchConfig) -> Result<ControlParts, SidecarError> {
    let runtime = Arc::new(
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|error| SidecarError::local("cloudweb_runtime_failed", error.to_string()))?,
    );
    let control = runtime
        .block_on(open_pipe(&config.control_pipe))
        .map_err(|error| SidecarError::new("cloudweb_control_pipe_failed", error))?;
    let frame = runtime
        .block_on(open_pipe(&config.frame_pipe))
        .map_err(|error| SidecarError::new("cloudweb_frame_pipe_failed", error))?;
    let (reader, writer) = tokio::io::split(control);
    Ok(ControlParts {
        writer: Box::new(PipeControlWriter {
            runtime: Arc::clone(&runtime),
            sink: writer,
        }),
        reader: Box::new(PipeControlReader {
            runtime: Arc::clone(&runtime),
            stream: reader,
        }),
        frame: Box::new(PipeFrameTransport { runtime, frame }),
    })
}

#[cfg(windows)]
impl ControlWriter for PipeControlWriter {
    fn send_control(&mut self, frame: &[u8]) -> Result<(), SidecarError> {
        guard_pipe_context()?;
        use tokio::io::AsyncWriteExt;
        let payload = frame.to_vec();
        let PipeControlWriter { runtime, sink } = self;
        runtime
            .block_on(async {
                // The timeout future must be created inside the runtime context.
                tokio::time::timeout(CONTROL_TIMEOUT, async {
                    sink.write_all(&payload).await?;
                    sink.flush().await
                })
                .await
            })
            .map_err(|_| {
                SidecarError::local("cloudweb_control_write_timeout", "control write timed out")
            })?
            .map_err(|error| SidecarError::new("cloudweb_control_write", error.to_string()))
    }
}

#[cfg(windows)]
impl ControlReader for PipeControlReader {
    fn recv_control(&mut self) -> Result<Option<Vec<u8>>, SidecarError> {
        guard_pipe_context()?;
        use tokio::io::AsyncReadExt;
        let PipeControlReader { runtime, stream } = self;
        let read = async {
            let mut length = [0u8; 4];
            match stream.read_exact(&mut length).await {
                Ok(_) => {}
                Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
                Err(error) => return Err(error.to_string()),
            }
            let length = u32::from_le_bytes(length) as usize;
            if length == 0 || length > wire::MAX_CONTROL_BYTES {
                return Err(format!("control frame of {length} bytes is out of range"));
            }
            let mut payload = vec![0u8; length];
            stream
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
}

#[cfg(windows)]
impl FrameTransport for PipeFrameTransport {
    fn can_reconnect_frame(&self) -> bool {
        true
    }

    fn reconnect_frame(&mut self, frame_pipe: &str) -> Result<(), SidecarError> {
        guard_pipe_context()?;
        let frame = self
            .runtime
            .block_on(open_pipe(frame_pipe))
            .map_err(|error| SidecarError::new("cloudweb_frame_pipe_failed", error))?;
        self.frame = frame;
        Ok(())
    }

    fn recv_frame(&mut self, timeout: Duration) -> Result<Option<RawFrame>, SidecarError> {
        guard_pipe_context()?;
        use tokio::io::AsyncReadExt;
        let PipeFrameTransport { runtime, frame } = self;
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
    use std::sync::Condvar;

    const TEST_EPOCH: &str = "epoch-test";
    const ALL_CAPS: [&str; 5] = HOST_CAPABILITIES;

    /// Frame-plane part of the scripted transport state.
    #[derive(Default)]
    struct FramePlane {
        frames: VecDeque<RawFrame>,
        /// Whether this fake transport accepts a frame-plane reopen.
        reconnectable: bool,
        /// Frame-plane reopens performed by the client.
        reconnects: u32,
        /// Number of frame reads that report a closed plane before the queue.
        frame_failures: u32,
    }

    /// Scripted control/frame plane shared by the three transport halves.
    /// `recv_control` blocks (with a bounded wake) until a receipt is pushed
    /// or the pipe reports EOF, matching how a real reader thread waits on
    /// the wire.
    struct Scripted {
        /// Queued receipts plus the EOF flag.
        responses: Mutex<(VecDeque<Value>, bool)>,
        cv: Condvar,
        sent: Mutex<Vec<Value>>,
        /// Command count after which the control pipe reports EOF.
        close_after: Mutex<Option<usize>>,
        /// Command count at which the next control write fails outright.
        fail_writes_after: Mutex<Option<usize>>,
        frames: Mutex<FramePlane>,
    }

    impl Scripted {
        fn new(responses: Vec<Value>) -> Arc<Self> {
            Arc::new(Self {
                responses: Mutex::new((responses.into_iter().collect(), false)),
                cv: Condvar::new(),
                sent: Mutex::new(Vec::new()),
                close_after: Mutex::new(None),
                fail_writes_after: Mutex::new(None),
                frames: Mutex::new(FramePlane::default()),
            })
        }

        fn into_parts(self: &Arc<Self>) -> ControlParts {
            ControlParts {
                writer: Box::new(ScriptedWriter {
                    shared: Arc::clone(&self),
                }),
                reader: Box::new(ScriptedReader {
                    shared: Arc::clone(&self),
                }),
                frame: Box::new(ScriptedFrames {
                    shared: Arc::clone(&self),
                }),
            }
        }

        fn push(&self, values: Vec<Value>) {
            let mut guard = self
                .responses
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let was_empty = guard.0.is_empty();
            guard.0.extend(values);
            drop(guard);
            if was_empty {
                self.cv.notify_all();
            }
        }

        /// Makes the next control read report EOF, waking the reader thread.
        fn set_eof(&self) {
            let mut guard = self
                .responses
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            guard.1 = true;
            drop(guard);
            self.cv.notify_all();
        }

        fn set_close_after(&self, commands: usize) {
            *self
                .close_after
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(commands);
        }

        fn set_fail_writes_after(&self, commands: usize) {
            *self
                .fail_writes_after
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(commands);
        }

        fn push_frame(&self, frame: RawFrame) {
            self.frames
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .frames
                .push_back(frame);
        }

        fn set_frame_failures(&self, failures: u32) {
            self.frames
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .frame_failures = failures;
        }

        fn set_reconnectable(&self, reconnectable: bool) {
            self.frames
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .reconnectable = reconnectable;
        }

        fn reconnects(&self) -> u32 {
            self.frames
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .reconnects
        }

        fn sent_count(&self) -> usize {
            self.sent
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .len()
        }

        /// Commands only: the hello frame carries no `op`.
        fn command_values(&self) -> Vec<Value> {
            self.sent
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .iter()
                .filter(|frame| frame["op"].is_string())
                .cloned()
                .collect()
        }

        /// Command ops in write order, joined by spaces for comparisons.
        fn ops_line(&self) -> String {
            self.command_values()
                .iter()
                .map(|frame| frame["op"].as_str().expect("op").to_string())
                .collect::<Vec<_>>()
                .join(" ")
        }

        fn op_count(&self, op: &str) -> usize {
            self.command_values()
                .iter()
                .filter(|frame| frame["op"].as_str() == Some(op))
                .count()
        }

        fn cmd(&self, index: usize) -> Value {
            self.command_values()[index].clone()
        }

        fn hello(&self) -> Value {
            self.sent
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .first()
                .cloned()
                .unwrap_or(Value::Null)
        }
    }

    struct ScriptedWriter {
        shared: Arc<Scripted>,
    }

    struct ScriptedReader {
        shared: Arc<Scripted>,
    }

    struct ScriptedFrames {
        shared: Arc<Scripted>,
    }

    impl ControlWriter for ScriptedWriter {
        fn send_control(&mut self, frame: &[u8]) -> Result<(), SidecarError> {
            let payload = wire::decode_control(&frame[4..])?;
            let mut sent = self
                .shared
                .sent
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let commands_before = sent
                .iter()
                .filter(|frame| frame["op"].is_string())
                .count();
            if self
                .shared
                .fail_writes_after
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .as_ref()
                == Some(&commands_before)
            {
                // The frame never reached the pipe: it is not recorded.
                return Err(SidecarError::local(
                    "cloudweb_test_write_failed",
                    "scripted control write refused",
                ));
            }
            sent.push(payload);
            let commands_after = sent
                .iter()
                .filter(|frame| frame["op"].is_string())
                .count();
            drop(sent);
            if self
                .shared
                .close_after
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .as_ref()
                == Some(&commands_after)
            {
                let mut guard = self
                    .shared
                    .responses
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                guard.0.clear();
                guard.1 = true;
                drop(guard);
                self.shared.cv.notify_all();
            }
            Ok(())
        }
    }

    impl ControlReader for ScriptedReader {
        fn recv_control(&mut self) -> Result<Option<Vec<u8>>, SidecarError> {
            let mut guard = self
                .shared
                .responses
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            loop {
                if let Some(value) = guard.0.pop_front() {
                    return Ok(Some(serde_json::to_vec(&value).map_err(|error| {
                        SidecarError::local("scripted", error.to_string())
                    })?));
                }
                if guard.1 {
                    return Ok(None);
                }
                if Arc::strong_count(&self.shared) <= 1 {
                    // Every other half and the test handle are gone: the
                    // scripted pipe is effectively closed.
                    return Ok(None);
                }
                (guard, _) = self
                    .shared
                    .cv
                    .wait_timeout(guard, Duration::from_millis(25))
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
            }
        }
    }

    impl FrameTransport for ScriptedFrames {
        fn recv_frame(&mut self, _timeout: Duration) -> Result<Option<RawFrame>, SidecarError> {
            let mut plane = self
                .shared
                .frames
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if plane.frame_failures > 0 {
                plane.frame_failures -= 1;
                return Ok(None);
            }
            Ok(plane.frames.pop_front())
        }

        fn can_reconnect_frame(&self) -> bool {
            self
                .shared
                .frames
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .reconnectable
        }

        fn reconnect_frame(&mut self, _frame_pipe: &str) -> Result<(), SidecarError> {
            let mut plane = self
                .shared
                .frames
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if !plane.reconnectable {
                return Err(SidecarError::local(
                    "cloudweb_frame_reconnect_unsupported",
                    "scripted transport is not reconnectable",
                ));
            }
            plane.reconnects += 1;
            Ok(())
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
        hello_ok_with_identity(generation, capabilities, "fake", Some("cloud_web.default"))
    }

    fn hello_ok_with_identity(
        generation: u64,
        capabilities: &[&str],
        backend_kind: &str,
        ui_profile: Option<&str>,
    ) -> Value {
        json!({
            "type": "hello_ok",
            "protocol_major": 1,
            "protocol_minor": 1,
            "backend_kind": backend_kind,
            "backend_version": "0.1.0",
            "ui_profile": ui_profile,
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

    fn receipt(seq: u64, generation: u64, detail: Option<Value>) -> Value {
        json!({
            "type": "receipt", "seq": seq, "status": "accepted",
            "generation": generation, "detail": detail
        })
    }

    fn dispatched(seq: u64, generation: u64) -> Value {
        json!({
            "type": "receipt", "seq": seq, "status": "dispatched",
            "generation": generation, "detail": null
        })
    }

    fn rejected(seq: u64, generation: u64, code: &str, message: &str) -> Value {
        json!({
            "type": "receipt", "seq": seq, "status": "rejected",
            "generation": generation, "error": code, "message": message
        })
    }

    fn unknown(seq: u64, generation: u64, code: &str, message: &str) -> Value {
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
            expected_backend: "fake".to_string(),
            allow_fake: true,
            session_id: "session-test".to_string(),
            profile: "default".to_string(),
            owner_id: "mxu:session-test".to_string(),
            backend_epoch: TEST_EPOCH.to_string(),
            control_pipe: r"\\.\pipe\ctl-test".to_string(),
            frame_pipe: r"\\.\pipe\frame-test".to_string(),
            token: "token-fixture".to_string(),
        }
    }

    fn scripted_client(responses: Vec<Value>) -> (SidecarClient, Arc<Scripted>) {
        let scripted = Scripted::new(responses);
        let client = SidecarClient::new(&config(), scripted.into_parts());
        (client, scripted)
    }

    /// A client that only completed the handshake (generation 1, no owner).
    fn handshaked(capabilities: &[&str]) -> (SidecarClient, Arc<Scripted>) {
        let (mut client, scripted) = scripted_client(vec![hello_ok(1, capabilities)]);
        client.handshake().expect("handshake completes");
        (client, scripted)
    }

    /// A client with handshake and ownership done (generation 2).
    fn owned(capabilities: &[&str]) -> (SidecarClient, Arc<Scripted>) {
        let (mut client, scripted) = handshaked(capabilities);
        scripted.push(vec![receipt(1, 2, Some(json!({ "lease_ms": 350 })))]);
        client.acquire_owner().expect("owner acquired");
        (client, scripted)
    }

    /// A client that also holds a fresh observation of frame 7.
    fn observing(capabilities: &[&str]) -> (SidecarClient, Arc<Scripted>) {
        let (mut client, scripted) = owned(capabilities);
        scripted.push(vec![receipt(2, 2, Some(metadata(7, 2, "fresh")))]);
        scripted.push_frame(frame_packet(7, 2, 0));
        client.capture().expect("capture");
        (client, scripted)
    }

    #[test]
    fn handshake_records_identity_limits_and_capabilities() {
        let (client, scripted) = handshaked(&ALL_CAPS);
        assert_eq!(client.generation, 1);
        assert_eq!(client.backend_kind, "fake");
        assert_eq!(client.ui_profile.as_deref(), Some("cloud_web.default"));
        assert_eq!(client.lease_ms, 350);
        assert_eq!(client.observation_max_age_ms, 1000);
        assert!(client.has_capability("geometry.v1"));
        assert_eq!(client.state, "ready");
        let hello = scripted.hello();
        assert_eq!(hello["type"], "hello");
        assert_eq!(hello["protocol_major"], 1);
        assert_eq!(hello["host"]["name"], "MXU");
        assert_eq!(hello["ui_profile"], "cloud_web.default");
        assert_eq!(hello["token"], "token-fixture");
    }

    #[test]
    fn handshake_rejects_a_backend_kind_that_differs_from_the_launch() {
        let mut config = config();
        config.expected_backend = "cdp".to_string();
        config.allow_fake = false;
        let scripted = Scripted::new(vec![hello_ok_with_identity(
            1,
            &ALL_CAPS,
            "fake",
            Some("cloud_web.default"),
        )]);
        let mut client = SidecarClient::new(&config, scripted.into_parts());
        let error = client.handshake().expect_err("mismatched kind must fail");
        assert_eq!(error.code(), "cloudweb_backend_kind_mismatch");
    }

    #[test]
    fn handshake_rejects_a_missing_backend_kind() {
        let scripted = Scripted::new(vec![hello_ok_with_identity(
            1,
            &ALL_CAPS,
            "",
            Some("cloud_web.default"),
        )]);
        let mut client = SidecarClient::new(&config(), scripted.into_parts());
        let error = client.handshake().expect_err("missing kind must fail");
        assert_eq!(error.code(), "cloudweb_backend_kind_missing");
    }

    #[test]
    fn handshake_refuses_fake_without_development_opt_in() {
        let mut config = config();
        config.allow_fake = false;
        let scripted = Scripted::new(vec![hello_ok(1, &ALL_CAPS)]);
        let mut client = SidecarClient::new(&config, scripted.into_parts());
        let error = client
            .handshake()
            .expect_err("fake must not be accepted implicitly");
        assert_eq!(error.code(), "cloudweb_fake_backend_refused");
    }

    #[test]
    fn handshake_rejects_a_profile_mismatch() {
        let scripted = Scripted::new(vec![hello_ok_with_identity(
            1,
            &ALL_CAPS,
            "fake",
            Some("cloud_web.other"),
        )]);
        let mut client = SidecarClient::new(&config(), scripted.into_parts());
        let error = client.handshake().expect_err("profile mismatch must fail");
        assert_eq!(error.code(), "cloudweb_profile_mismatch");
    }

    #[test]
    fn handshake_rejects_a_missing_ui_profile() {
        let scripted = Scripted::new(vec![hello_ok_with_identity(1, &ALL_CAPS, "fake", None)]);
        let mut client = SidecarClient::new(&config(), scripted.into_parts());
        let error = client
            .handshake()
            .expect_err("a missing ui_profile must fail closed");
        assert_eq!(error.code(), "cloudweb_profile_mismatch");
    }

    #[test]
    fn handshake_accepts_an_explicit_cdp_backend() {
        let mut config = config();
        config.expected_backend = "cdp".to_string();
        config.allow_fake = false;
        let scripted = Scripted::new(vec![hello_ok_with_identity(
            1,
            &ALL_CAPS,
            "cdp",
            Some("cloud_web.default"),
        )]);
        let mut client = SidecarClient::new(&config, scripted.into_parts());
        let info = client
            .handshake()
            .expect("an explicit cdp backend is accepted");
        assert_eq!(info.backend_kind, "cdp");
        assert_eq!(info.ui_profile.as_deref(), Some("cloud_web.default"));
    }

    #[test]
    fn handshake_is_not_repeated_and_identity_is_namespaced_per_backend() {
        let (mut client, _scripted) = handshaked(&ALL_CAPS);
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
        let scripted = Scripted::new(vec![json!({
            "type": "error", "code": "auth_failed", "message": "launch token mismatch"
        })]);
        let mut client = SidecarClient::new(&config(), scripted.into_parts());
        let error = client.handshake().unwrap_err();
        assert_eq!(error.code(), "auth_failed");
        assert!(client.is_faulted());

        let mut wrong_major = hello_ok(1, &ALL_CAPS);
        wrong_major["protocol_major"] = json!(2);
        let scripted = Scripted::new(vec![wrong_major]);
        let mut client = SidecarClient::new(&config(), scripted.into_parts());
        assert_eq!(
            client.handshake().unwrap_err().code(),
            "cloudweb_protocol_mismatch"
        );

        let mut wrong_epoch = hello_ok(1, &ALL_CAPS);
        wrong_epoch["backend_epoch"] = json!("someone-elses-epoch");
        let scripted = Scripted::new(vec![wrong_epoch]);
        let mut client = SidecarClient::new(&config(), scripted.into_parts());
        assert_eq!(
            client.handshake().unwrap_err().code(),
            "cloudweb_epoch_mismatch"
        );

        let mut not_ready = hello_ok(1, &ALL_CAPS);
        not_ready["state"] = json!("faulted");
        let scripted = Scripted::new(vec![not_ready]);
        let mut client = SidecarClient::new(&config(), scripted.into_parts());
        assert_eq!(
            client.handshake().unwrap_err().code(),
            "cloudweb_sidecar_not_ready"
        );
        assert!(client.is_faulted());
    }

    #[test]
    fn owner_conflict_is_surfaced_and_the_lease_is_dropped() {
        let (mut client, scripted) = owned(&ALL_CAPS);
        client.set_renew_ms(0);
        scripted.push(vec![rejected(
            2,
            2,
            "owner_conflict",
            "owner x still holds the lease",
        )]);
        let error = client.renew_lease().unwrap_err();
        assert_eq!(error.code(), "owner_conflict");
        assert!(!client.lease_held());
        // A conflicting owner counts as a lease loss: the host no longer holds
        // the generation it acquired.
        assert_eq!(client.lease_losses(), 1);

        // The next acquisition starts a new generation.
        scripted.push(vec![receipt(3, 3, Some(json!({ "lease_ms": 350 })))]);
        client
            .acquire_owner()
            .expect("a new owner was acquired");
        assert_eq!(client.generation, 3);
        assert_eq!(
            scripted.ops_line(),
            "acquire_owner renew_lease acquire_owner"
        );
    }

    #[test]
    fn stale_generation_is_resynchronised_before_the_next_command() {
        let (mut client, scripted) = owned(&ALL_CAPS);
        // Force the host view behind the sidecar and let it answer with the
        // current generation; the client adopts it instead of replaying.
        client.set_generation(7);
        client.set_renew_ms(0);
        scripted.push(vec![
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
            scripted.ops_line(),
            "acquire_owner renew_lease acquire_owner"
        );
        assert_eq!(client.commands_total(), 3);
    }

    #[test]
    fn input_mapping_sends_the_expected_ops_and_observation() {
        let (mut client, scripted) = observing(&ALL_CAPS);
        scripted.push(vec![
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
            scripted.ops_line(),
            "acquire_owner get_latest_frame mouse_down mouse_up key_down key_up wheel insert_text"
        );
        let down = scripted.cmd(2);
        assert_eq!(down["params"]["x"], 640);
        assert_eq!(down["params"]["y"], 360);
        assert_eq!(down["params"]["button"], 0);
        assert_eq!(down["based_on_frame_id"], 7);
        assert_eq!(down["geometry_revision"], 3);
        assert!(down["based_on_age_ms"].as_u64().expect("age") < 1000);
        assert_eq!(down["session_id"], "session-test");
        assert_eq!(down["backend_epoch"], TEST_EPOCH);
        assert_eq!(down["generation"], 2);
        assert_eq!(scripted.cmd(3)["params"]["button"], 0);
        assert_eq!(scripted.cmd(4)["params"]["key"], 65);
        assert_eq!(scripted.cmd(6)["params"]["dy"], -300);
        assert_eq!(scripted.cmd(7)["params"]["text"], "endfield");
        assert_eq!(client.commands_total(), 8);
    }

    #[test]
    fn touch_and_click_key_paths_map_to_sidecar_ops() {
        let (mut client, scripted) = observing(&ALL_CAPS);
        scripted.push(vec![
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
            scripted.ops_line(),
            "acquire_owner get_latest_frame mouse_down move_absolute mouse_up key_down key_up"
        );
        assert_eq!(scripted.cmd(3)["params"]["x"], 56);
        assert_eq!(scripted.cmd(3)["params"]["y"], 78);
    }

    #[test]
    fn swipe_uses_a_full_down_move_up_path() {
        let (mut client, scripted) = observing(&ALL_CAPS);
        scripted.push(vec![dispatched(3, 2), dispatched(4, 2), dispatched(5, 2)]);
        client.swipe(100, 100, 200, 200, 300).expect("swipe");
        assert_eq!(
            scripted.ops_line(),
            "acquire_owner get_latest_frame mouse_down move_absolute mouse_up"
        );
        assert_eq!(scripted.cmd(3)["params"]["x"], 200);
        assert_eq!(scripted.cmd(3)["based_on_frame_id"], 7);
        assert_eq!(scripted.cmd(4)["params"]["button"], 0);
    }

    #[test]
    fn absolute_input_without_an_observation_is_refused_not_faked() {
        let (mut client, scripted) = owned(&ALL_CAPS);
        let error = client.tap(10, 20).unwrap_err();
        assert_eq!(error.code(), "cloudweb_no_observation");
        // Nothing was sent for the refused action.
        assert_eq!(scripted.ops_line(), "acquire_owner");
    }

    #[test]
    fn expired_observation_refresh_failure_does_not_send_input() {
        let (mut client, scripted) = observing(&ALL_CAPS);
        // Age the stored observation past the limit instead of relying on a
        // zero-millisecond window.
        let frame = client.observation.as_mut().expect("observation");
        frame.captured_at = frame
            .captured_at
            .checked_sub(Duration::from_secs(5))
            .expect("a past instant");
        // The refresh itself fails; the stale input must not be sent.
        scripted.push(vec![rejected(3, 2, "site_not_ready", "not ready")]);
        let error = client.key_down(65).unwrap_err();
        assert_eq!(error.code(), "site_not_ready");
        assert_eq!(
            scripted.ops_line(),
            "acquire_owner get_latest_frame get_latest_frame"
        );
    }

    #[test]
    fn frames_run_without_ownership_and_the_first_input_acquires_it() {
        // Cold-start order used by the real sidecar: hello reports generation 0
        // in BrowserReady, a frame makes it Ready, and only input needs an owner.
        // Mirror the real process: hello_ok reports generation 0 in BrowserReady.
        let scripted = Scripted::new(vec![hello_ok(0, &ALL_CAPS)]);
        let mut client = SidecarClient::new(&config(), scripted.into_parts());
        client.handshake().expect("handshake at generation 0");
        assert_eq!(client.generation, 0);
        scripted.push(vec![receipt(1, 0, Some(metadata(7, 0, "fresh")))]);
        scripted.push_frame(frame_packet(7, 0, 0));
        let frame = client.capture().expect("a frame before ownership");
        assert_eq!(frame.frame_id, 7);
        // The capture did not acquire or renew a lease.
        assert_eq!(scripted.ops_line(), "get_latest_frame");
        assert!(!client.diagnostics()["owner_lease"]["held"]
            .as_bool()
            .unwrap());

        scripted.push(vec![
            receipt(2, 1, Some(json!({ "lease_ms": 350 }))),
            dispatched(3, 1),
        ]);
        client
            .key_down(65)
            .expect("the first input acquires ownership");
        assert_eq!(
            scripted.ops_line(),
            "get_latest_frame acquire_owner key_down"
        );
        assert_eq!(client.generation, 1);
    }

    #[test]
    fn a_release_still_works_without_an_observation() {
        let (mut client, scripted) = owned(&ALL_CAPS);
        scripted.push(vec![receipt(2, 2, None)]);
        // No capture happened, so the safety path must not be blocked by it.
        client
            .release_button(0)
            .expect("release without observation");
        assert_eq!(scripted.ops_line(), "acquire_owner mouse_up");
        assert!(scripted
            .command_values()
            .iter()
            .all(|frame| frame.get("based_on_frame_id").is_none()));
    }

    #[test]
    fn frame_stale_rejection_gets_one_refresh_then_retry() {
        let (mut client, scripted) = observing(&ALL_CAPS);
        scripted.push(vec![
            rejected(3, 2, "frame_stale", "latest frame is not fresh"),
            receipt(4, 2, Some(metadata(8, 2, "fresh"))),
            dispatched(5, 2),
        ]);
        scripted.push_frame(frame_packet(8, 2, 0));

        client.key_down(65).expect("key down after one refresh");
        assert_eq!(
            scripted.ops_line(),
            "acquire_owner get_latest_frame key_down get_latest_frame key_down"
        );
        assert_eq!(scripted.cmd(4)["based_on_frame_id"], 8);
        assert_eq!(scripted.cmd(4)["params"]["key"], 65);
    }

    #[test]
    fn an_expired_observation_is_refreshed_once_before_input() {
        let (mut client, scripted) = observing(&ALL_CAPS);
        // The recognition/menu transition took longer than the 1 s budget.
        client
            .observation
            .as_mut()
            .expect("observation")
            .captured_at = Instant::now() - Duration::from_secs(2);
        // Refresh capture (seq 3) + the retried input (seq 4).
        scripted.push(vec![receipt(3, 2, Some(metadata(8, 2, "fresh")))]);
        scripted
            .frames
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .frames
            .push_back(frame_packet(8, 2, 0));
        scripted.push(vec![dispatched(4, 2)]);
        client.key_down(65).expect("input after a fresh observation");
        let cmds = scripted.command_values();
        assert_eq!(cmds[cmds.len() - 2]["op"], "get_latest_frame");
        assert_eq!(cmds[cmds.len() - 1]["op"], "key_down");
        assert_eq!(cmds[cmds.len() - 1]["based_on_frame_id"], 8);
    }

    #[test]
    fn unknown_receipt_is_never_retried() {
        let (mut client, scripted) = observing(&ALL_CAPS);
        scripted.push(vec![unknown(3, 2, "input_unknown", "cdp dispatch failed")]);
        let error = client.key_down(65).unwrap_err();
        assert_eq!(error.code(), "input_unknown");
        assert_eq!(
            scripted.op_count("key_down"),
            1,
            "an unknown result must not be replayed"
        );
    }

    #[test]
    fn capture_rejects_non_canonical_stale_and_unpaired_frames() {
        // The client is `owned`, so captures start at seq 2.
        let (mut client, scripted) = owned(&ALL_CAPS);

        // Wrong size.
        let mut small = metadata(7, 2, "fresh");
        small["width"] = json!(640);
        scripted.push(vec![receipt(2, 2, Some(small))]);
        assert_eq!(
            client.capture().unwrap_err().code(),
            "cloudweb_frame_not_canonical"
        );

        // Not fresh, and freshness unknown is refused as well.
        scripted.push(vec![
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
        scripted.push(vec![receipt(5, 2, Some(metadata(9, 2, "fresh")))]);
        assert_eq!(
            client.capture().unwrap_err().code(),
            "cloudweb_frame_closed"
        );

        // A packet carrying a different frame id must not be passed off as new.
        scripted.push(vec![receipt(6, 2, Some(metadata(10, 2, "fresh")))]);
        scripted.push_frame(frame_packet(99, 2, 0));
        assert_eq!(
            client.capture().unwrap_err().code(),
            "cloudweb_frame_desynced"
        );

        // A packet from an older generation.
        scripted.push(vec![receipt(7, 2, Some(metadata(11, 2, "fresh")))]);
        scripted.push_frame(frame_packet(11, 1, 0));
        assert_eq!(
            client.capture().unwrap_err().code(),
            "cloudweb_frame_generation"
        );

        // A stale header tag on the matching packet.
        scripted.push(vec![receipt(8, 2, Some(metadata(12, 2, "fresh")))]);
        scripted.push_frame(frame_packet(12, 2, 1 /* stale */));
        assert_eq!(
            client.capture().unwrap_err().code(),
            "cloudweb_frame_not_fresh"
        );

        // An older packet is skipped for the matching one.
        scripted.push(vec![receipt(9, 2, Some(metadata(13, 2, "fresh")))]);
        scripted.push_frame(frame_packet(12, 2, 0));
        scripted.push_frame(frame_packet(13, 2, 0));
        let frame = client.capture().expect("matching frame after a skip");
        assert_eq!(frame.frame_id, 13);
        assert_eq!(frame.bytes, b"fake-png".to_vec());
    }

    #[test]
    fn capture_reopens_a_closed_frame_plane_and_uses_a_fresh_frame() {
        let (mut client, scripted) = owned(&ALL_CAPS);
        scripted.set_reconnectable(true);

        // The sidecar closed the plane while no capture was reading it; the
        // first metadata is fresh but its packet is gone. The client must
        // reopen and re-request instead of failing the observation.
        scripted.push(vec![receipt(2, 2, Some(metadata(20, 2, "fresh")))]);
        scripted.push(vec![receipt(3, 2, Some(metadata(21, 2, "fresh")))]);
        scripted.push_frame(frame_packet(21, 2, 0));
        scripted.set_frame_failures(1);
        let frame = client
            .capture()
            .expect("capture after a frame-plane reopen");
        assert_eq!(frame.frame_id, 21);
        assert_eq!(scripted.reconnects(), 1);
        assert_eq!(
            scripted.op_count("get_latest_frame"),
            2,
            "the retry must request a new frame id"
        );

        // A plane that stays closed is retried a bounded number of times and
        // then surfaces the error instead of spinning.
        let (mut client, scripted) = owned(&ALL_CAPS);
        scripted.set_reconnectable(true);
        scripted.set_frame_failures(FRAME_RECONNECTS + 5);
        scripted.push(vec![
            receipt(2, 2, Some(metadata(30, 2, "fresh"))),
            receipt(3, 2, Some(metadata(31, 2, "fresh"))),
            receipt(4, 2, Some(metadata(32, 2, "fresh"))),
        ]);
        assert_eq!(
            client.capture().unwrap_err().code(),
            "cloudweb_frame_closed"
        );
        assert_eq!(scripted.reconnects(), FRAME_RECONNECTS);
    }

    #[test]
    fn unsupported_capabilities_return_an_explicit_error() {
        let (mut client, scripted) = handshaked(&["frame.latest", "input.keyboard", "geometry.v1"]);
        scripted.push(vec![receipt(1, 2, Some(json!({ "lease_ms": 350 })))]);
        client.acquire_owner().expect("owner");
        scripted.push(vec![receipt(2, 2, Some(metadata(7, 2, "fresh")))]);
        scripted.push_frame(frame_packet(7, 2, 0));
        client.capture().expect("capture");
        let before = scripted.sent_count();

        // The declared set has neither input.relative nor input.text.
        assert_eq!(
            client.look(10, 10).unwrap_err().code(),
            "cloudweb_capability_missing"
        );
        assert_eq!(
            client.input_text("hi").unwrap_err().code(),
            "cloudweb_capability_missing"
        );
        assert_eq!(scripted.sent_count(), before);
    }

    #[test]
    fn pc_contacts_map_to_sidecar_wire_buttons() {
        let (mut client, scripted) = observing(&ALL_CAPS);
        scripted.push(vec![dispatched(3, 2), dispatched(4, 2)]);
        client.press(1, 10, 10).expect("right contact press");
        client.release_button(2).expect("middle contact release");
        assert_eq!(scripted.cmd(2)["params"]["button"], 2, "contact1 -> wire2");
        assert_eq!(scripted.cmd(3)["params"]["button"], 1, "contact2 -> wire1");
        assert_eq!(scripted.ops_line().rsplit_once(" ").is_some(), true);
        assert!(scripted.ops_line().ends_with("mouse_down mouse_up"));

        // Unknown contacts still fail before sending anything.
        let before = scripted.sent_count();
        assert_eq!(
            client.press(3, 10, 10).unwrap_err().code(),
            "cloudweb_unsupported_multitouch"
        );
        assert_eq!(scripted.sent_count(), before);
    }

    #[test]
    fn lease_loss_during_a_slow_command_is_recorded_and_reacquired() {
        let (mut client, scripted) = owned(&ALL_CAPS);
        client.set_renew_ms(0);
        scripted.push(vec![
            rejected(2, 3, "lease_expired", "lease expired"),
            receipt(3, 4, Some(json!({ "lease_ms": 350 }))),
        ]);
        client
            .ensure_lease()
            .expect("a new generation is acquired after the lease loss");
        assert_eq!(client.generation, 4);
        assert_eq!(client.lease_losses(), 1);
        // The host records the gap it observed rather than claiming the lease
        // held; a real p95 still needs a live session (N1 stays open).
        assert!(client.max_renew_gap_ms() < 1000);
    }

    #[test]
    fn a_slow_session_asks_for_a_bounded_longer_lease() {
        let (mut client, scripted) = handshaked(&ALL_CAPS);
        // The connect capture and early commands observed ~700 ms round trips.
        client
            .link()
            .slowest_command_ms
            .store(700, AtomicOrdering::SeqCst);
        scripted.push(vec![receipt(1, 2, Some(json!({ "lease_ms": 2100 })))]);
        client.acquire_owner().expect("owner");
        assert_eq!(scripted.cmd(0)["params"]["lease_ms"], 2100);
        assert_eq!(client.lease_ms, 2100);
        assert_eq!(client.renew_ms, 1050);
        assert_eq!(
            client.link().renew_interval(),
            Duration::from_millis(1050)
        );

        // A fast session keeps the default request and the default renewal.
        let (mut client, scripted) = handshaked(&ALL_CAPS);
        client
            .link()
            .slowest_command_ms
            .store(10, AtomicOrdering::SeqCst);
        scripted.push(vec![receipt(1, 2, Some(json!({ "lease_ms": 350 })))]);
        client.acquire_owner().expect("owner");
        assert_eq!(scripted.cmd(0)["params"]["lease_ms"], 350);
        assert_eq!(client.link().renew_interval(), Duration::from_millis(175));
    }

    #[test]
    fn a_failed_lease_recovery_is_not_masked_as_success() {
        let (mut client, scripted) = owned(&ALL_CAPS);
        client.set_renew_ms(0);
        scripted.push(vec![
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
        let (mut client, scripted) = owned(&ALL_CAPS);
        scripted.push(vec![receipt(
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
        assert_eq!(scripted.ops_line(), "acquire_owner close");
    }

    #[test]
    fn a_faulted_session_still_allows_close_and_blocks_input() {
        let (mut client, scripted) = owned(&ALL_CAPS);
        scripted.push(vec![unknown(
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
        scripted.push(vec![receipt(3, 4, Some(json!({ "released": true })))]);
        client.close().expect("close on a faulted session");
        assert!(client.is_closed());
    }

    #[test]
    fn a_closed_control_pipe_is_reported_instead_of_a_fake_success() {
        let (mut client, scripted) = handshaked(&ALL_CAPS);
        scripted.set_close_after(1);
        let error = client.acquire_owner().unwrap_err();
        assert_eq!(error.code(), "cloudweb_control_closed");
        assert!(client.is_faulted());

        // EOF in the middle of a command is equally explicit.
        let (mut client, scripted) = owned(&ALL_CAPS);
        scripted.set_close_after(2);
        assert_eq!(
            client.renew_lease().unwrap_err().code(),
            "cloudweb_control_closed"
        );
    }

    #[test]
    fn receipt_shape_is_validated_and_a_foreign_seq_is_not_misrouted() {
        let (mut client, scripted) = owned(&ALL_CAPS);
        scripted.push(vec![json!({
            "type": "greeting", "seq": 2, "status": "accepted", "generation": 2
        })]);
        assert_eq!(
            client.renew_lease().unwrap_err().code(),
            "cloudweb_protocol_shape"
        );

        // The reader routes by seq: a receipt for another seq is parked, and
        // the next command still gets its own receipt.
        scripted.push(vec![receipt(99, 2, None)]);
        scripted.push(vec![receipt(3, 2, None)]);
        client
            .renew_lease()
            .expect("the receipt for the live seq is routed, the other is not");
        assert_eq!(scripted.ops_line(), "acquire_owner renew_lease renew_lease");

        // The shared interpreter still rejects a mismatched seq outright.
        let error = interpret_receipt(&receipt(9, 1, None), 3).unwrap_err();
        assert_eq!(error.code(), "cloudweb_seq_mismatch");
    }

    // -- CW2 D1: control-link properties the debt is about ----------------

    /// With a normal command blocked waiting for its receipt, the urgent
    /// ReleaseAll frame must still reach the pipe within 100 ms.
    #[test]
    fn a_normal_command_await_does_not_delay_the_urgent_frame() {
        let (client, scripted) = owned(&ALL_CAPS);
        let link = client.link();

        // A normal renewal whose receipt is deliberately never scripted:
        // this waiter holds no lock the writer needs.
        let blocked_link = Arc::clone(&link);
        let blocking = std::thread::spawn(move || blocked_link.renew_once());

        let urgent_link = Arc::clone(&link);
        let started_at = Arc::new(Mutex::new(None::<Instant>));
        let urgent_started = Arc::clone(&started_at);
        let urgent = std::thread::spawn(move || {
            *urgent_started
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(Instant::now());
            urgent_link.release_all_once("urgent-lane-test")
        });

        let deadline = Instant::now() + Duration::from_millis(100);
        let written_at = loop {
            if scripted.ops_line().contains("release_all") {
                break Instant::now();
            }
            if Instant::now() >= deadline {
                panic!(
                    "the urgent frame was not written within 100 ms while a normal command awaited its receipt; ops: {}",
                    scripted.ops_line()
                );
            }
            std::thread::sleep(Duration::from_millis(1));
        };
        let started = started_at
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .expect("the urgent thread recorded its start");
        let latency = written_at.saturating_duration_since(started);
        assert!(
            latency <= Duration::from_millis(100),
            "urgent write latency was {} ms",
            latency.as_millis()
        );

        // Now acknowledge both in-flight commands (arrival order of the two
        // receipts is intentionally reversed).
        scripted.push(vec![receipt(3, 2, None), receipt(2, 2, None)]);
        blocking.join().expect("renewal finished").expect("renewal acked");
        urgent.join().expect("release finished").expect("release acked");
        let ops = scripted.ops_line();
        assert!(ops.contains("renew_lease") && ops.contains("release_all"), "{ops}");
        assert_eq!(link.pending_count(), 0, "no pending may survive the round trips");
    }

    /// Receipts are routed by seq, independently of arrival order.
    #[test]
    fn receipts_route_by_seq_independent_of_arrival_order() {
        let (client, scripted) = handshaked(&ALL_CAPS);
        let link = client.link();
        link.ensure_reader().expect("the reader starts");

        let seq_a = link.next_seq.fetch_add(1, AtomicOrdering::SeqCst) + 1;
        let rx_a = link.request(seq_a);
        let seq_b = link.next_seq.fetch_add(1, AtomicOrdering::SeqCst) + 1;
        let rx_b = link.request(seq_b);

        // B's receipt arrives first.
        scripted.push(vec![receipt(seq_b, 5, None)]);
        let value_b = rx_b
            .recv_timeout(Duration::from_secs(2))
            .expect("b routed")
            .expect("no transport error");
        assert_eq!(value_b["seq"].as_u64(), Some(seq_b));

        scripted.push(vec![receipt(seq_a, 7, None)]);
        let value_a = rx_a
            .recv_timeout(Duration::from_secs(2))
            .expect("a routed")
            .expect("no transport error");
        assert_eq!(value_a["seq"].as_u64(), Some(seq_a));
        assert_eq!(link.pending_count(), 0);
    }

    /// EOF settles every pending waiter with `cloudweb_control_closed` and
    /// leaves nothing behind in the map.
    #[test]
    fn eof_fails_every_pending_without_leak() {
        let (client, scripted) = handshaked(&ALL_CAPS);
        let link = client.link();
        link.ensure_reader().expect("the reader starts");

        let seq_1 = link.next_seq.fetch_add(1, AtomicOrdering::SeqCst) + 1;
        let rx_1 = link.request(seq_1);
        let seq_2 = link.next_seq.fetch_add(1, AtomicOrdering::SeqCst) + 1;
        let rx_2 = link.request(seq_2);
        // A receipt nobody asked for: parked early, must be drained on EOF.
        scripted.push(vec![receipt(99, 1, None)]);
        let deadline = Instant::now() + Duration::from_secs(2);
        while link.pending_count() < 3 && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(2));
        }
        assert_eq!(link.pending_count(), 3, "two waiters plus one early receipt");

        scripted.set_eof();
        let error_1 = rx_1
            .recv_timeout(Duration::from_secs(2))
            .expect("settled")
            .expect_err("EOF must fail the waiter");
        assert_eq!(error_1.code(), "cloudweb_control_closed");
        let error_2 = rx_2
            .recv_timeout(Duration::from_secs(2))
            .expect("settled")
            .expect_err("EOF must fail the waiter");
        assert_eq!(error_2.code(), "cloudweb_control_closed");
        assert_eq!(link.pending_count(), 0, "EOF must leak no pending entry");

        // Later commands surface the closed control plane immediately.
        let error = link
            .command(
                "renew_lease",
                json!({ "owner_id": "mxu:session-test" }),
                None,
                CONTROL_TIMEOUT,
                false,
            )
            .unwrap_err();
        assert_eq!(error.code(), "cloudweb_control_closed");
    }

    /// An urgent write failure must be reported explicitly (the Stop did not
    /// reach the sidecar) and must not leave a pending registration.
    #[test]
    fn an_urgent_write_failure_is_reported_without_pending_leak() {
        let (client, scripted) = handshaked(&ALL_CAPS);
        let link = client.link();
        scripted.set_fail_writes_after(0);

        let error = link.release_all_once("host_stop").unwrap_err();
        assert_eq!(error.code(), "cloudweb_test_write_failed");
        assert!(
            scripted.ops_line().is_empty(),
            "a refused write must not reach the pipe"
        );
        assert_eq!(link.pending_count(), 0, "the failed command must unregister");
    }

    /// A `generation_stale` rejection of the urgent ReleaseAll was never
    /// dispatched, and the receipt's generation resynchronises the mirror so
    /// the controller's single retry can succeed.
    #[test]
    fn a_stale_release_all_rejection_resyncs_the_generation() {
        let (client, scripted) = handshaked(&ALL_CAPS);
        let link = client.link();
        link.set_generation(7);
        scripted.push(vec![rejected(
            1,
            11,
            "generation_stale",
            "command generation 7 != current 11",
        )]);

        let error = link.release_all_once("host_stop").unwrap_err();
        assert_eq!(error.code(), "generation_stale");
        assert_eq!(link.generation(), 11, "the mirror must adopt the receipt's generation");

        // The one allowed retry on the resynchronised generation succeeds.
        scripted.push(vec![receipt(
            2,
            12,
            Some(json!({ "released": true, "old_generation": 11 })),
        )]);
        link.release_all_once("host_stop").expect("retry acknowledged");
        assert_eq!(link.generation(), 12);
        assert_eq!(link.pending_count(), 0);
    }

    /// An acknowledged ReleaseAll fences the owner on the shared lease clock
    /// so the renewal thread stops renewing a generation the sidecar dropped.
    #[test]
    fn an_acknowledged_release_all_clears_the_shared_lease_clock() {
        let (client, scripted) = owned(&ALL_CAPS);
        assert!(client.lease_held());
        let link = client.link();
        scripted.push(vec![receipt(
            2,
            3,
            Some(json!({ "released": true, "old_generation": 2 })),
        )]);
        link.release_all_once("host_stop").expect("acknowledged");
        assert!(!client.lease_held(), "the fenced owner must clear the lease clock");
        assert_eq!(link.generation(), 3);
    }

    /// A receipt wait timeout fails only that command: the session can keep
    /// issuing commands and a late receipt never crosses into the next one.
    #[test]
    fn a_wait_timeout_fails_only_its_command() {
        let (client, scripted) = handshaked(&ALL_CAPS);
        let link = client.link();
        let error = link
            .command(
                "renew_lease",
                json!({ "owner_id": "mxu:session-test" }),
                None,
                Duration::from_millis(60),
                false,
            )
            .unwrap_err();
        assert_eq!(error.code(), "cloudweb_control_wait_timeout");
        assert_eq!(link.pending_count(), 0, "a timed-out wait must remove its slot");

        // The abandoned command's late receipt arrives now; it must be parked
        // and must not answer the next command.
        scripted.push(vec![receipt(1, 1, None)]);
        scripted.push(vec![receipt(2, 1, Some(json!({ "lease_ms": 350 })))]);
        let detail = link
            .command(
                "renew_lease",
                json!({ "owner_id": "mxu:session-test" }),
                None,
                Duration::from_secs(2),
                false,
            )
            .expect("the next command gets its own receipt");
        assert_eq!(detail, Some(json!({ "lease_ms": 350 })));
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

        let missing_backend = LaunchConfig::resolve("session-a", "default", &|key| match key {
            SIDECAR_PATH_ENV => Some("cloudweb-sidecar.exe".to_string()),
            _ => None,
        })
        .expect_err("an unconfigured actuator must fail");
        assert_eq!(missing_backend.code(), "cloudweb_backend_required");

        let fake_without_opt_in = LaunchConfig::resolve("session-a", "default", &|key| match key {
            SIDECAR_PATH_ENV => Some("cloudweb-sidecar.exe".to_string()),
            SIDECAR_ARGS_ENV => Some("--backend fake".to_string()),
            _ => None,
        })
        .expect_err("fake requires an explicit development opt-in");
        assert_eq!(fake_without_opt_in.code(), "cloudweb_fake_backend_refused");

        let config = LaunchConfig::resolve("session-a", "default", &|key| match key {
            SIDECAR_PATH_ENV => Some("cloudweb-sidecar.exe".to_string()),
            SIDECAR_ARGS_ENV => Some("--backend fake".to_string()),
            ALLOW_FAKE_ENV => Some("1".to_string()),
            _ => None,
        })
        .expect("resolved config");
        assert_eq!(config.extra_args, vec!["--backend", "fake"]);
        assert_eq!(config.expected_backend, "fake");
        assert!(config.allow_fake);
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

    #[cfg(windows)]
    #[test]
    fn readiness_identity_is_verified_against_the_launch_request() {
        let controller = SidecarCloudWebController::new(config());
        let ready = format!(
            "cloudweb-sidecar ready control_pipe=x frame_pipe=y \
             session_id=session-test backend_epoch={TEST_EPOCH} backend=Fake protocol=1.0"
        );
        assert!(controller.verify_ready_line(&ready).is_ok());

        let wrong_backend = format!(
            "cloudweb-sidecar ready session_id=session-test backend_epoch={TEST_EPOCH} backend=Cdp"
        );
        let error = controller
            .verify_ready_line(&wrong_backend)
            .expect_err("a different inner backend must be rejected");
        assert_eq!(error.code(), "cloudweb_backend_identity_mismatch");

        let wrong_session = format!(
            "cloudweb-sidecar ready session_id=other backend_epoch={TEST_EPOCH} backend=Fake"
        );
        assert_eq!(
            controller
                .verify_ready_line(&wrong_session)
                .expect_err("a different session must be rejected")
                .code(),
            "cloudweb_sidecar_identity_mismatch"
        );

        let wrong_epoch =
            "cloudweb-sidecar ready session_id=session-test backend_epoch=other backend=Fake";
        assert_eq!(
            controller
                .verify_ready_line(wrong_epoch)
                .expect_err("a different epoch must be rejected")
                .code(),
            "cloudweb_sidecar_identity_mismatch"
        );

        let missing =
            format!("cloudweb-sidecar ready session_id=session-test backend_epoch={TEST_EPOCH}");
        assert_eq!(
            controller
                .verify_ready_line(&missing)
                .expect_err("a missing backend identity must be rejected")
                .code(),
            "cloudweb_backend_identity_mismatch"
        );
    }

    #[test]
    fn diagnostics_describe_the_session_without_secrets() {
        let (mut client, scripted) = owned(&ALL_CAPS);
        scripted.push(vec![receipt(2, 2, Some(metadata(7, 2, "fresh")))]);
        scripted.push_frame(frame_packet(7, 2, 0));
        client.capture().expect("capture");
        let diagnostics = client.diagnostics();
        assert_eq!(diagnostics["implementation"], "sidecar");
        assert_eq!(diagnostics["backend"], "cloud_web");
        assert_eq!(diagnostics["session_id"], "session-test");
        assert_eq!(diagnostics["generation"], 2);
        assert_eq!(diagnostics["owner_lease"]["lease_ms"], 350);
        assert_eq!(diagnostics["commands"], 2);
        assert!(diagnostics["observation_age_ms"].is_number());
        // CW2 D1: the urgent lane's evidence keys are host-side counters.
        assert!(diagnostics["control_link"]["release_all_rtt_ms"].is_number());
        assert!(diagnostics["control_link"]["urgent_write_ms"].is_number());
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
            ALLOW_FAKE_ENV => Some("1".to_string()),
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
        assert_eq!(info["backend_kind"], "fake");
        assert_eq!(info["expected_backend"], "fake");
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
        // The second callback within the cache TTL is a preview/recognition
        // read, not a new sidecar capture.
        let cached = CustomControllerCallback::screencap(&controller).expect("cached frame");
        assert_eq!(frame, cached);

        // Inputs after a fresh observation must reach the running session. Hold
        // the key for longer than the 350 ms sidecar lease: the host renewal
        // thread must keep the owner alive without another input callback.
        assert!(CustomControllerCallback::key_down(&controller, 65));
        std::thread::sleep(Duration::from_secs(2));
        let held: Value =
            serde_json::from_str(&CustomControllerCallback::get_info(&controller)).expect("info");
        assert_eq!(
            held["health"]["held_keys"],
            json!([65]),
            "the 2 s long press must survive the 350 ms lease: {held}"
        );
        assert!(CustomControllerCallback::key_up(&controller, 65));
        assert!(CustomControllerCallback::touch_down(
            &controller,
            0,
            100,
            200,
            1
        ));
        assert!(CustomControllerCallback::touch_up(&controller, 0));
        // PC contacts: 1 = right, 2 = middle. Both must reach the sidecar's
        // wire order (right=2, middle=1), not be refused as multitouch.
        assert!(CustomControllerCallback::touch_down(
            &controller,
            1,
            100,
            200,
            1
        ));
        assert!(CustomControllerCallback::touch_up(&controller, 1));
        assert!(CustomControllerCallback::touch_down(
            &controller,
            2,
            100,
            200,
            1
        ));
        assert!(CustomControllerCallback::touch_up(&controller, 2));
        assert!(CustomControllerCallback::input_text(
            &controller,
            "cloudweb"
        ));
        // A contact without a single-pointer mapping must not report success.
        assert!(!CustomControllerCallback::touch_down(
            &controller,
            3,
            1,
            1,
            1
        ));

        let after: Value =
            serde_json::from_str(&CustomControllerCallback::get_info(&controller)).expect("info");
        assert!(after["commands"].as_u64().expect("commands") >= 7);
        assert!(after["observation_age_ms"].is_number());
        assert!(
            after["host_frame_cache"]["hits"]
                .as_u64()
                .expect("cache hits")
                >= 1,
            "the second screencap must be served from the host snapshot: {after}"
        );
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

    /// Real-cloud CW2 T5: a long relative move is in flight when the host
    /// sends the urgent ReleaseAll. The frame must be written without waiting
    /// for the move, and the sidecar must preempt it at a step boundary.
    #[test]
    #[ignore = "requires the live cloud page on CDP 9223 and the deployed sidecar binary"]
    fn a_real_cloud_slow_move_is_preempted_by_the_urgent_release_all() {
        let exec = std::env::var(SIDECAR_PATH_ENV)
            .unwrap_or_else(|_| panic!("{SIDECAR_PATH_ENV} must point at the sidecar binary"));
        let config = LaunchConfig::resolve("session-t5", "default", &|key| match key {
            SIDECAR_PATH_ENV => Some(exec.clone()),
            SIDECAR_ARGS_ENV => Some(
                "--backend cdp --cdp-port 9223 --cdp-url https://endfield.hypergryph.com/cloud"
                    .to_string(),
            ),
            _ => None,
        })
        .expect("launch config resolves");
        let controller = Arc::new(SidecarCloudWebController::new(config));
        let _pid = controller
            .launch_for_test()
            .expect("the real cloud session launches");
        let frame =
            CustomControllerCallback::screencap(controller.as_ref()).expect("a real cloud frame");
        // The continuous CDP pipeline serves canonical JPEG; the fake backend
        // serves PNG. Both are accepted here, an empty frame is not.
        assert!(
            frame.starts_with(&[0xFF, 0xD8, 0xFF]) || frame.starts_with(b"\x89PNG\r\n\x1a\n"),
            "unexpected frame signature: {:02X?}",
            &frame[..frame.len().min(8)]
        );

        // The first input also acquires ownership; the frame above opened the
        // continuous input window. The move runs ~4.7 s uninterrupted.
        let worker = controller.clone();
        let slow = std::thread::spawn(move || {
            CustomControllerCallback::relative_move(worker.as_ref(), 60_000, 0)
        });
        std::thread::sleep(Duration::from_millis(300));
        let rtt = controller
            .release_all_urgent("t5-probe")
            .expect("the urgent ReleaseAll is acknowledged");
        assert!(rtt <= 1_000, "urgent ReleaseAll RTT {rtt} ms exceeds 1 s");
        let posted = slow.join().expect("relative-move thread");
        assert!(
            !posted,
            "the preempted move must report failure, not success"
        );

        let info: Value =
            serde_json::from_str(&CustomControllerCallback::get_info(controller.as_ref()))
                .expect("info");
        assert_eq!(info["health"]["held_keys"], json!([]));
        assert_eq!(info["health"]["held_buttons"], json!([]));
        assert_eq!(
            info["health"]["possibly_held"],
            json!([]),
            "no input may remain uncertain: {info}"
        );
        assert!(
            info["control_link"]["release_all_rtt_ms"]
                .as_u64()
                .unwrap_or(u64::MAX)
                <= 1_000,
            "diagnostics RTT exceeds 1 s: {info}"
        );
        eprintln!(
            "T5 real-cloud urgent ReleaseAll rtt_ms={rtt} control_link={}",
            info["control_link"]
        );
        drop(controller);
    }
}
