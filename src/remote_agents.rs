use std::collections::{BTreeMap, HashMap, HashSet};
use std::io::{self, BufRead, BufReader, Read, Write};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc as std_mpsc, Arc};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use serde::Deserialize;
use tokio::sync::mpsc;

use crate::api::schema::AgentStatus;
use crate::config::RemoteAgentSourceConfig;
use crate::detect::{Agent, AgentState};
use crate::events::AppEvent;

const SNAPSHOT_INTERVAL_SECONDS: u64 = 2;
const SNAPSHOT_STALL_TIMEOUT: Duration = Duration::from_secs(12);
const MAX_SNAPSHOT_LINE_BYTES: usize = 1024 * 1024;
const MAX_STDERR_BYTES: usize = 32 * 1024;
const MAX_REMOTE_TEXT_CHARS: usize = 256;
const MAX_REMOTE_TOKEN_CHARS: usize = 512;
const MAX_REMOTE_ID_CHARS: usize = 256;
const MAX_REMOTE_WORKSPACES: usize = 512;
const MAX_REMOTE_TABS: usize = 2048;
const MAX_REMOTE_PANES: usize = 4096;
const MAX_REMOTE_AGENTS: usize = 1024;
const MAX_REMOTE_METADATA_ENTRIES_PER_AGENT: usize = 64;
const RECONNECT_MAX_DELAY: Duration = Duration::from_secs(30);
const WATCHER_POLL_INTERVAL: Duration = Duration::from_millis(100);
const MIN_SNAPSHOT_PROCESS_INTERVAL: Duration = Duration::from_millis(500);
const READER_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(1);

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) struct RemoteHostKey(String);

impl RemoteHostKey {
    pub(crate) fn for_source(source: &RemoteAgentSourceConfig) -> Self {
        Self(format!("{}\u{1f}{}", source.target, source.session))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RemoteHostRegistration {
    pub key: RemoteHostKey,
    pub label: String,
    pub generation: u64,
    pub order: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RemoteAgentPresentation {
    pub workspace_id: String,
    pub tab_id: String,
    pub pane_id: String,
    pub workspace_label: String,
    pub tab_label: Option<String>,
    pub pane_label: Option<String>,
    pub terminal_title: Option<String>,
    pub terminal_title_stripped: Option<String>,
    pub agent_label: String,
    pub agent_kind_label: Option<String>,
    pub agent: Option<Agent>,
    pub state: AgentState,
    pub seen: bool,
    pub state_labels: HashMap<String, String>,
    pub tokens: HashMap<String, String>,
    /// Display order from the remote snapshot vectors.
    pub display_order: (usize, usize, usize),
    /// Public order fields exposed to agent-view custom sorting.
    pub order: (usize, usize, usize),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RemoteAgentSnapshot {
    pub version: String,
    pub protocol: u32,
    pub agents: Vec<RemoteAgentPresentation>,
}

#[derive(Deserialize)]
struct FleetSuccessResponse {
    result: FleetResponseResult,
}

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum FleetResponseResult {
    SessionSnapshot {
        snapshot: FleetSessionSnapshot,
    },
    #[serde(other)]
    Other,
}

#[derive(Deserialize)]
struct FleetSessionSnapshot {
    version: String,
    protocol: u32,
    workspaces: Vec<FleetWorkspaceInfo>,
    tabs: Vec<FleetTabInfo>,
    panes: Vec<FleetPaneInfo>,
    agents: Vec<FleetAgentInfo>,
}

#[derive(Deserialize)]
struct FleetWorkspaceInfo {
    workspace_id: String,
    label: String,
}

#[derive(Deserialize)]
struct FleetTabInfo {
    tab_id: String,
    workspace_id: String,
    number: usize,
    label: String,
    #[serde(default)]
    custom_label: Option<bool>,
}

#[derive(Deserialize)]
struct FleetPaneInfo {
    pane_id: String,
    #[serde(default)]
    number: Option<usize>,
    terminal_id: String,
    workspace_id: String,
    tab_id: String,
}

#[derive(Deserialize)]
struct FleetAgentInfo {
    terminal_id: String,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    agent: Option<String>,
    #[serde(default)]
    title: Option<String>,
    #[serde(default)]
    terminal_title: Option<String>,
    #[serde(default)]
    terminal_title_stripped: Option<String>,
    #[serde(default)]
    display_agent: Option<String>,
    #[serde(deserialize_with = "agent_status_or_unknown")]
    agent_status: AgentStatus,
    #[serde(default)]
    state_labels: HashMap<String, String>,
    #[serde(default)]
    tokens: HashMap<String, String>,
    workspace_id: String,
    tab_id: String,
    pane_id: String,
}

/// A newer remote may report status names this build does not know without an
/// incompatible wire change; degrade them to `Unknown` instead of failing the
/// whole snapshot.
fn agent_status_or_unknown<'de, D>(deserializer: D) -> Result<AgentStatus, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    Ok(serde_json::from_value(serde_json::Value::String(value)).unwrap_or(AgentStatus::Unknown))
}

#[derive(Debug)]
pub(crate) enum RemoteAgentUpdateKind {
    Snapshot(RemoteAgentSnapshot),
    Offline { error: String },
}

#[derive(Debug)]
pub(crate) struct RemoteAgentUpdate {
    pub host: RemoteHostKey,
    pub generation: u64,
    pub kind: RemoteAgentUpdateKind,
    completion: Option<Arc<AtomicBool>>,
}

impl RemoteAgentUpdate {
    fn tracked(
        host: RemoteHostKey,
        generation: u64,
        kind: RemoteAgentUpdateKind,
        pending: &Arc<AtomicBool>,
    ) -> Option<Self> {
        pending
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .ok()?;
        Some(Self {
            host,
            generation,
            kind,
            completion: Some(pending.clone()),
        })
    }

    #[cfg(test)]
    pub(crate) fn immediate(
        host: RemoteHostKey,
        generation: u64,
        kind: RemoteAgentUpdateKind,
    ) -> Self {
        Self {
            host,
            generation,
            kind,
            completion: None,
        }
    }
}

impl Drop for RemoteAgentUpdate {
    fn drop(&mut self) {
        if let Some(completion) = &self.completion {
            completion.store(false, Ordering::Release);
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct StoredRemoteAgent {
    presentation: RemoteAgentPresentation,
    state_change_seq: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RemoteHostState {
    label: String,
    generation: u64,
    order: usize,
    online: bool,
    version: Option<String>,
    protocol: Option<u32>,
    last_error: Option<String>,
    agents: BTreeMap<String, StoredRemoteAgent>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RemotePanelAgent {
    pub host_key: String,
    pub host_label: String,
    pub host_order: usize,
    pub online: bool,
    pub presentation: RemoteAgentPresentation,
    pub state_change_seq: Option<u64>,
}

#[derive(Debug, Default)]
pub(crate) struct RemoteAgentRegistry {
    hosts: BTreeMap<RemoteHostKey, RemoteHostState>,
}

impl RemoteAgentRegistry {
    pub(crate) fn reconcile(&mut self, registrations: &[RemoteHostRegistration]) -> bool {
        let wanted = registrations
            .iter()
            .map(|registration| registration.key.clone())
            .collect::<HashSet<_>>();
        let original_len = self.hosts.len();
        self.hosts.retain(|key, _| wanted.contains(key));
        let mut changed = self.hosts.len() != original_len;

        for registration in registrations {
            match self.hosts.get_mut(&registration.key) {
                Some(host) if host.generation == registration.generation => {
                    if host.label != registration.label || host.order != registration.order {
                        host.label = registration.label.clone();
                        host.order = registration.order;
                        changed = true;
                    }
                }
                Some(host) => {
                    *host = empty_host_state(registration);
                    changed = true;
                }
                None => {
                    self.hosts
                        .insert(registration.key.clone(), empty_host_state(registration));
                    changed = true;
                }
            }
        }
        changed
    }

    pub(crate) fn apply_update(
        &mut self,
        mut update: RemoteAgentUpdate,
        next_state_change_seq: &mut u64,
    ) -> bool {
        let Some(host) = self.hosts.get_mut(&update.host) else {
            return false;
        };
        if host.generation != update.generation {
            return false;
        }

        let kind = std::mem::replace(
            &mut update.kind,
            RemoteAgentUpdateKind::Offline {
                error: String::new(),
            },
        );
        match kind {
            RemoteAgentUpdateKind::Offline { error } => {
                let last_error = Some(sanitize_remote_text(&error, MAX_REMOTE_TEXT_CHARS));
                let changed = host.online || host.last_error != last_error;
                host.online = false;
                host.last_error = last_error;
                changed
            }
            RemoteAgentUpdateKind::Snapshot(snapshot) => {
                let mut agents = BTreeMap::new();
                for presentation in snapshot.agents {
                    let key = presentation.pane_id.clone();
                    let state_change_seq = match host.agents.get(&key) {
                        Some(stored)
                            if stored.presentation.state == presentation.state
                                && stored.presentation.seen == presentation.seen =>
                        {
                            stored.state_change_seq
                        }
                        Some(_) => Some(next_sequence(next_state_change_seq)),
                        None => None,
                    };
                    agents.insert(
                        key,
                        StoredRemoteAgent {
                            presentation,
                            state_change_seq,
                        },
                    );
                }

                let new_version = Some(snapshot.version);
                let new_protocol = Some(snapshot.protocol);
                let changed = !host.online
                    || host.version != new_version
                    || host.protocol != new_protocol
                    || host.agents != agents;
                host.online = true;
                host.version = new_version;
                host.protocol = new_protocol;
                host.last_error = None;
                host.agents = agents;
                changed
            }
        }
    }

    pub(crate) fn panel_agents(&self) -> Vec<RemotePanelAgent> {
        let mut hosts = self.hosts.iter().collect::<Vec<_>>();
        hosts.sort_by_key(|(_, host)| host.order);
        hosts
            .into_iter()
            .flat_map(|(key, host)| {
                let mut agents = host.agents.values().collect::<Vec<_>>();
                agents.sort_by(|left, right| {
                    left.presentation
                        .display_order
                        .cmp(&right.presentation.display_order)
                });
                agents.into_iter().map(move |agent| RemotePanelAgent {
                    host_key: key.0.clone(),
                    host_label: host.label.clone(),
                    host_order: host.order,
                    online: host.online,
                    presentation: agent.presentation.clone(),
                    state_change_seq: agent.state_change_seq,
                })
            })
            .collect()
    }

    pub(crate) fn unavailable_source_count(&self) -> usize {
        self.hosts.values().filter(|host| !host.online).count()
    }
}

fn empty_host_state(registration: &RemoteHostRegistration) -> RemoteHostState {
    RemoteHostState {
        label: registration.label.clone(),
        generation: registration.generation,
        order: registration.order,
        online: false,
        version: None,
        protocol: None,
        last_error: None,
        agents: BTreeMap::new(),
    }
}

fn next_sequence(next: &mut u64) -> u64 {
    *next = next.saturating_add(1);
    *next
}

struct RemoteAgentWatcher {
    generation: u64,
    cancel: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

#[derive(Default)]
pub(crate) struct RemoteAgentSupervisor {
    watchers: HashMap<RemoteHostKey, RemoteAgentWatcher>,
    /// Cancelled watchers whose threads have not exited yet. Reconcile runs on
    /// the event thread, so they are reaped once finished instead of joined
    /// synchronously; Drop joins whatever is left.
    retired: Vec<RemoteAgentWatcher>,
    next_generation: u64,
}

impl RemoteAgentSupervisor {
    pub(crate) fn reconcile(
        &mut self,
        sources: Vec<RemoteAgentSourceConfig>,
        event_tx: mpsc::Sender<AppEvent>,
    ) -> Vec<RemoteHostRegistration> {
        self.reconcile_with(sources, event_tx, spawn_remote_agent_watcher)
    }

    fn reconcile_with<F>(
        &mut self,
        sources: Vec<RemoteAgentSourceConfig>,
        event_tx: mpsc::Sender<AppEvent>,
        mut spawn_watcher: F,
    ) -> Vec<RemoteHostRegistration>
    where
        F: FnMut(
            RemoteAgentSourceConfig,
            RemoteHostKey,
            u64,
            Arc<AtomicBool>,
            Arc<AtomicBool>,
            mpsc::Sender<AppEvent>,
        ) -> io::Result<JoinHandle<()>>,
    {
        let wanted = sources
            .iter()
            .map(RemoteHostKey::for_source)
            .collect::<HashSet<_>>();
        let removed = self
            .watchers
            .keys()
            .filter(|key| !wanted.contains(*key))
            .cloned()
            .collect::<Vec<_>>();
        let finished = self
            .watchers
            .iter()
            .filter(|(_, watcher)| watcher.handle.as_ref().is_some_and(JoinHandle::is_finished))
            .map(|(key, _)| key.clone())
            .collect::<Vec<_>>();
        for key in finished {
            if let Some(watcher) = self.watchers.remove(&key) {
                join_watcher(watcher);
            }
        }
        for key in removed {
            if let Some(watcher) = self.watchers.remove(&key) {
                watcher.cancel.store(true, Ordering::Release);
                self.retired.push(watcher);
            }
        }
        let mut still_running = Vec::new();
        for watcher in self.retired.drain(..) {
            if watcher.handle.as_ref().is_some_and(JoinHandle::is_finished) {
                join_watcher(watcher);
            } else {
                still_running.push(watcher);
            }
        }
        self.retired = still_running;

        let mut registrations = Vec::with_capacity(sources.len());
        for (order, source) in sources.into_iter().enumerate() {
            let key = RemoteHostKey::for_source(&source);
            if !self.watchers.contains_key(&key) {
                let Some(generation) = self.next_generation.checked_add(1) else {
                    tracing::error!(
                        target = %source.target,
                        "remote agent watcher generation space exhausted"
                    );
                    continue;
                };
                self.next_generation = generation;
                let cancel = Arc::new(AtomicBool::new(false));
                let pending = Arc::new(AtomicBool::new(false));
                let handle = match spawn_watcher(
                    source.clone(),
                    key.clone(),
                    generation,
                    cancel.clone(),
                    pending,
                    event_tx.clone(),
                ) {
                    Ok(handle) => Some(handle),
                    Err(err) => {
                        tracing::error!(
                            target = %source.target,
                            %err,
                            "failed to spawn remote agent watcher"
                        );
                        None
                    }
                };
                let Some(handle) = handle else {
                    continue;
                };
                self.watchers.insert(
                    key.clone(),
                    RemoteAgentWatcher {
                        generation,
                        cancel,
                        handle: Some(handle),
                    },
                );
            }

            if let Some(watcher) = self.watchers.get(&key) {
                registrations.push(RemoteHostRegistration {
                    key,
                    label: source.fleet_label(),
                    generation: watcher.generation,
                    order,
                });
            }
        }
        registrations
    }
}

fn spawn_remote_agent_watcher(
    source: RemoteAgentSourceConfig,
    host: RemoteHostKey,
    generation: u64,
    cancel: Arc<AtomicBool>,
    pending: Arc<AtomicBool>,
    event_tx: mpsc::Sender<AppEvent>,
) -> io::Result<JoinHandle<()>> {
    std::thread::Builder::new()
        .name("herdr-remote-agents".to_string())
        .spawn(move || run_watcher(source, host, generation, cancel, pending, event_tx))
}

impl Drop for RemoteAgentSupervisor {
    fn drop(&mut self) {
        let mut watchers = self
            .watchers
            .drain()
            .map(|(_, watcher)| watcher)
            .collect::<Vec<_>>();
        watchers.append(&mut self.retired);
        for watcher in &watchers {
            watcher.cancel.store(true, Ordering::Release);
        }
        for watcher in watchers.drain(..) {
            join_watcher(watcher);
        }
    }
}

fn join_watcher(mut watcher: RemoteAgentWatcher) {
    if let Some(handle) = watcher.handle.take() {
        if handle.join().is_err() {
            tracing::warn!("remote agent watcher panicked during shutdown");
        }
    }
}

fn run_watcher(
    source: RemoteAgentSourceConfig,
    host: RemoteHostKey,
    generation: u64,
    cancel: Arc<AtomicBool>,
    pending: Arc<AtomicBool>,
    event_tx: mpsc::Sender<AppEvent>,
) {
    let mut reconnect_delay = Duration::from_secs(1);
    let mut last_logged_error: Option<String> = None;
    let mut last_error_log_at = None;
    while !cancel.load(Ordering::Acquire) {
        let outcome =
            stream_remote_snapshots(&source, &host, generation, &cancel, &pending, &event_tx);
        if cancel.load(Ordering::Acquire) || outcome.cancelled {
            return;
        }

        let should_log = last_logged_error.as_deref() != Some(outcome.error.as_str())
            || last_error_log_at
                .is_none_or(|last: Instant| last.elapsed() >= Duration::from_secs(300));
        if should_log {
            tracing::warn!(
                target = %source.target,
                session = %source.session,
                error = %outcome.error,
                "remote agent source unavailable"
            );
            last_logged_error = Some(outcome.error.clone());
            last_error_log_at = Some(Instant::now());
        }

        if outcome.received_snapshot {
            reconnect_delay = Duration::from_secs(1);
        }

        let update = loop {
            if let Some(update) = RemoteAgentUpdate::tracked(
                host.clone(),
                generation,
                RemoteAgentUpdateKind::Offline {
                    error: outcome.error.clone(),
                },
                &pending,
            ) {
                break update;
            }
            // The last snapshot event is still queued in the app channel; wait
            // for it to drain so the offline notice is never silently dropped.
            if cancelable_sleep(&cancel, WATCHER_POLL_INTERVAL) {
                return;
            }
        };
        let update = AppEvent::RemoteAgentsUpdated(Box::new(update));
        if matches!(
            event_tx.try_send(update),
            Err(mpsc::error::TrySendError::Closed(_))
        ) {
            return;
        }
        if cancelable_sleep(&cancel, reconnect_delay) {
            return;
        }
        reconnect_delay = reconnect_delay.saturating_mul(2).min(RECONNECT_MAX_DELAY);
    }
}

struct StreamOutcome {
    cancelled: bool,
    received_snapshot: bool,
    error: String,
}

fn stream_remote_snapshots(
    source: &RemoteAgentSourceConfig,
    host: &RemoteHostKey,
    generation: u64,
    cancel: &AtomicBool,
    pending: &Arc<AtomicBool>,
    event_tx: &mpsc::Sender<AppEvent>,
) -> StreamOutcome {
    let mut child = match spawn_snapshot_stream(source) {
        Ok(child) => child,
        Err(err) => {
            return StreamOutcome {
                cancelled: false,
                received_snapshot: false,
                error: format!("failed to start ssh: {err}"),
            };
        }
    };

    let Some(stdout) = child.stdout.take() else {
        terminate_child_tree(&mut child);
        return StreamOutcome {
            cancelled: false,
            received_snapshot: false,
            error: "ssh stdout was unavailable".to_string(),
        };
    };
    let Some(stderr) = child.stderr.take() else {
        terminate_child_tree(&mut child);
        return StreamOutcome {
            cancelled: false,
            received_snapshot: false,
            error: "ssh stderr was unavailable".to_string(),
        };
    };

    let (line_tx, line_rx) = std_mpsc::sync_channel(1);
    let stdout_handle = match std::thread::Builder::new()
        .name("herdr-remote-stdout".to_string())
        .spawn(move || read_snapshot_lines(stdout, line_tx))
    {
        Ok(handle) => handle,
        Err(err) => {
            terminate_child_tree(&mut child);
            return StreamOutcome {
                cancelled: false,
                received_snapshot: false,
                error: format!("failed to start ssh stdout reader: {err}"),
            };
        }
    };
    let stderr_handle = match std::thread::Builder::new()
        .name("herdr-remote-stderr".to_string())
        .spawn(move || read_capped_stderr(stderr))
    {
        Ok(handle) => handle,
        Err(err) => {
            terminate_child_tree(&mut child);
            drop(line_rx);
            join_reader(stdout_handle, "stdout");
            return StreamOutcome {
                cancelled: false,
                received_snapshot: false,
                error: format!("failed to start ssh stderr reader: {err}"),
            };
        }
    };
    let mut last_output = Instant::now();
    let mut last_processed = None;
    let mut received_snapshot = false;
    let mut error = None;
    let mut cancelled = false;

    loop {
        if cancel.load(Ordering::Acquire) {
            cancelled = true;
            break;
        }
        match line_rx.recv_timeout(WATCHER_POLL_INTERVAL) {
            Ok(Ok(line)) => {
                if last_processed
                    .is_some_and(|last: Instant| last.elapsed() < MIN_SNAPSHOT_PROCESS_INTERVAL)
                {
                    // A transient network stall can flush queued frames in one
                    // burst; drop the extras instead of tearing the stream down.
                    // A remote that never slows down still hits the stall
                    // timeout because skipped frames leave last_output alone.
                    continue;
                }
                last_processed = Some(Instant::now());
                match parse_snapshot_line(&line) {
                    Ok(snapshot) => {
                        last_output = Instant::now();
                        received_snapshot = true;
                        if let Some(update) = RemoteAgentUpdate::tracked(
                            host.clone(),
                            generation,
                            RemoteAgentUpdateKind::Snapshot(snapshot),
                            pending,
                        ) {
                            let update = AppEvent::RemoteAgentsUpdated(Box::new(update));
                            match event_tx.try_send(update) {
                                Ok(()) => {}
                                Err(mpsc::error::TrySendError::Full(_)) => {}
                                Err(mpsc::error::TrySendError::Closed(_)) => {
                                    cancelled = true;
                                    break;
                                }
                            }
                        }
                    }
                    Err(err) => {
                        error = Some(err);
                        break;
                    }
                }
            }
            Ok(Err(err)) => {
                error = Some(err);
                break;
            }
            Err(std_mpsc::RecvTimeoutError::Disconnected) => break,
            Err(std_mpsc::RecvTimeoutError::Timeout) => {
                if last_output.elapsed() >= SNAPSHOT_STALL_TIMEOUT {
                    error = Some("remote snapshot stream timed out".to_string());
                    break;
                }
            }
        }
    }

    terminate_child_tree(&mut child);
    drop(line_rx);
    join_reader(stdout_handle, "stdout");
    let stderr = join_reader(stderr_handle, "stderr").unwrap_or_default();
    if cancelled {
        return StreamOutcome {
            cancelled: true,
            received_snapshot,
            error: String::new(),
        };
    }

    let error = error.unwrap_or_else(|| {
        if stderr.is_empty() {
            "ssh snapshot stream disconnected".to_string()
        } else {
            format!("ssh snapshot stream disconnected: {stderr}")
        }
    });
    StreamOutcome {
        cancelled: false,
        received_snapshot,
        error: sanitize_remote_text(&error, MAX_REMOTE_TEXT_CHARS),
    }
}

fn spawn_snapshot_stream(source: &RemoteAgentSourceConfig) -> io::Result<Child> {
    let mut child = snapshot_stream_command(source).spawn()?;
    let Some(mut stdin) = child.stdin.take() else {
        terminate_child_tree(&mut child);
        return Err(io::Error::new(
            io::ErrorKind::BrokenPipe,
            "ssh stdin was unavailable",
        ));
    };
    if let Err(err) = stdin.write_all(remote_snapshot_script(&source.session).as_bytes()) {
        terminate_child_tree(&mut child);
        return Err(err);
    }
    drop(stdin);
    Ok(child)
}

fn snapshot_stream_command(source: &RemoteAgentSourceConfig) -> Command {
    let mut command = Command::new("ssh");
    command
        .arg("-T")
        .arg("-o")
        .arg("BatchMode=yes")
        .arg("-o")
        .arg("NumberOfPasswordPrompts=0")
        .arg("-o")
        .arg("StrictHostKeyChecking=yes")
        .arg("-o")
        .arg("UpdateHostKeys=no")
        .arg("-o")
        .arg("ClearAllForwardings=yes")
        .arg("-o")
        .arg("ForwardAgent=no")
        .arg("-o")
        .arg("ForwardX11=no")
        .arg("-o")
        .arg("GSSAPIDelegateCredentials=no")
        .arg("-o")
        .arg("PermitLocalCommand=no")
        .arg("-o")
        .arg("ControlMaster=no")
        .arg("-o")
        .arg("ControlPath=none")
        .arg("-o")
        .arg("ControlPersist=no")
        .arg("-o")
        .arg("RemoteCommand=none")
        .arg("-o")
        .arg("ConnectTimeout=5")
        .arg("-o")
        .arg("ConnectionAttempts=1")
        .arg("-o")
        .arg("ServerAliveInterval=5")
        .arg("-o")
        .arg("ServerAliveCountMax=2");
    command
        .arg("--")
        .arg(&source.target)
        .arg("/bin/sh -s")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    crate::platform::configure_background_command(&mut command);
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    crate::platform::detach_server_daemon_command(&mut command);
    command
}

fn remote_snapshot_script(session: &str) -> String {
    let version = env!("CARGO_PKG_VERSION");
    let protocol = crate::protocol::PROTOCOL_VERSION;
    format!(
        r#"fleet_session={}
fleet_bin=
try_herdr() {{
    candidate=$1
    if [ -z "$candidate" ] || [ ! -x "$candidate" ]; then
        return 1
    fi
    fleet_schema=$("$candidate" api schema --json 2>/dev/null) || return 1
    case "$fleet_schema" in
        *'"protocol": {protocol},'*) ;;
        *) return 1 ;;
    esac
    fleet_help=$("$candidate" api help 2>&1) || return 1
    case "$fleet_help" in
        *'herdr api snapshot'*) fleet_bin=$candidate; return 0 ;;
        *) return 1 ;;
    esac
}}
path_candidate=$(command -v herdr 2>/dev/null || :)
try_herdr "$path_candidate" ||
try_herdr "$HOME/.local/bin/herdr" ||
try_herdr "/opt/homebrew/bin/herdr" ||
try_herdr "/usr/local/bin/herdr" ||
try_herdr "/home/linuxbrew/.linuxbrew/bin/herdr" ||
try_herdr "$HOME/.local/share/mise/installs/herdr/{version}/bin/herdr" ||
try_herdr "$HOME/.local/share/mise/installs/herdr/{version}/herdr" ||
try_herdr "$HOME/.local/share/mise/installs/github-ogulcancelik-herdr/{version}/herdr" ||
try_herdr "$HOME/.nix-profile/bin/herdr" ||
try_herdr "/etc/profiles/per-user/$USER/bin/herdr" ||
try_herdr "/nix/var/nix/profiles/default/bin/herdr" ||
try_herdr "/run/current-system/sw/bin/herdr" || {{
    printf '%s\n' 'no compatible remote herdr binary found' >&2
    exit 127
}}
env -u HERDR_SOCKET_PATH -u HERDR_CLIENT_SOCKET_PATH "$fleet_bin" --session "$fleet_session" api snapshot || exit $?
while sleep {SNAPSHOT_INTERVAL_SECONDS}; do
    env -u HERDR_SOCKET_PATH -u HERDR_CLIENT_SOCKET_PATH "$fleet_bin" --session "$fleet_session" api snapshot || exit $?
done
"#,
        shell_quote(session)
    )
}

fn shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\"'\"'"))
}

fn terminate_child_tree(child: &mut Child) {
    let mut pids = crate::platform::session_processes(child.id());
    if pids.is_empty() {
        pids.push(child.id());
    }
    pids.sort_unstable_by(|left, right| right.cmp(left));
    pids.dedup();
    crate::platform::signal_processes(&pids, crate::platform::Signal::Kill);
    let _ = child.kill();
    let _ = child.wait();
}

fn join_reader<T>(handle: JoinHandle<T>, stream: &str) -> Option<T> {
    let deadline = Instant::now() + READER_SHUTDOWN_TIMEOUT;
    while !handle.is_finished() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(10));
    }
    if !handle.is_finished() {
        tracing::warn!(
            stream,
            "detaching stuck remote agent reader after tree kill"
        );
        return None;
    }
    match handle.join() {
        Ok(value) => Some(value),
        Err(_) => {
            tracing::warn!(stream, "remote agent reader panicked");
            None
        }
    }
}

fn cancelable_sleep(cancel: &AtomicBool, duration: Duration) -> bool {
    let deadline = Instant::now() + duration;
    while Instant::now() < deadline {
        if cancel.load(Ordering::Acquire) {
            return true;
        }
        std::thread::sleep(
            WATCHER_POLL_INTERVAL.min(deadline.saturating_duration_since(Instant::now())),
        );
    }
    cancel.load(Ordering::Acquire)
}

fn read_snapshot_lines(stdout: impl Read, sender: std_mpsc::SyncSender<Result<Vec<u8>, String>>) {
    let mut reader = BufReader::new(stdout);
    loop {
        match read_bounded_line(&mut reader, MAX_SNAPSHOT_LINE_BYTES) {
            Ok(Some(line)) => {
                if sender.send(Ok(line)).is_err() {
                    return;
                }
            }
            Ok(None) => return,
            Err(err) => {
                let _ = sender.send(Err(err.to_string()));
                return;
            }
        }
    }
}

fn read_bounded_line(reader: &mut impl BufRead, max_bytes: usize) -> io::Result<Option<Vec<u8>>> {
    let mut line = Vec::new();
    loop {
        let available = reader.fill_buf()?;
        if available.is_empty() {
            return if line.is_empty() {
                Ok(None)
            } else {
                Ok(Some(line))
            };
        }
        let consumed = available
            .iter()
            .position(|byte| *byte == b'\n')
            .map_or(available.len(), |index| index + 1);
        if line.len().saturating_add(consumed) > max_bytes {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("remote snapshot exceeds {max_bytes} bytes"),
            ));
        }
        line.extend_from_slice(&available[..consumed]);
        let found_newline = available.get(consumed.saturating_sub(1)) == Some(&b'\n');
        reader.consume(consumed);
        if found_newline {
            while matches!(line.last(), Some(b'\n' | b'\r')) {
                line.pop();
            }
            return Ok(Some(line));
        }
    }
}

fn read_capped_stderr(mut stderr: impl Read) -> String {
    let mut stored = Vec::new();
    let mut chunk = [0_u8; 4096];
    loop {
        match stderr.read(&mut chunk) {
            Ok(0) | Err(_) => break,
            Ok(read) => {
                let remaining = MAX_STDERR_BYTES.saturating_sub(stored.len());
                stored.extend_from_slice(&chunk[..read.min(remaining)]);
                if stored.len() >= MAX_STDERR_BYTES {
                    break;
                }
            }
        }
    }
    sanitize_remote_text(&String::from_utf8_lossy(&stored), MAX_REMOTE_TEXT_CHARS)
}

pub(crate) fn parse_snapshot_line(line: &[u8]) -> Result<RemoteAgentSnapshot, String> {
    let response = serde_json::from_slice::<FleetSuccessResponse>(line)
        .map_err(|err| format!("invalid remote snapshot response: {err}"))?;
    let FleetResponseResult::SessionSnapshot { snapshot } = response.result else {
        return Err("remote response was not a session snapshot".to_string());
    };
    snapshot_for_fleet(snapshot)
}

fn snapshot_for_fleet(snapshot: FleetSessionSnapshot) -> Result<RemoteAgentSnapshot, String> {
    if snapshot.protocol != crate::protocol::PROTOCOL_VERSION {
        return Err(format!(
            "remote protocol {} is incompatible with local protocol {}",
            snapshot.protocol,
            crate::protocol::PROTOCOL_VERSION
        ));
    }
    if snapshot.workspaces.len() > MAX_REMOTE_WORKSPACES {
        return Err(format!(
            "remote snapshot contains more than {MAX_REMOTE_WORKSPACES} workspaces"
        ));
    }
    if snapshot.tabs.len() > MAX_REMOTE_TABS {
        return Err(format!(
            "remote snapshot contains more than {MAX_REMOTE_TABS} tabs"
        ));
    }
    if snapshot.panes.len() > MAX_REMOTE_PANES {
        return Err(format!(
            "remote snapshot contains more than {MAX_REMOTE_PANES} panes"
        ));
    }
    if snapshot.agents.len() > MAX_REMOTE_AGENTS {
        return Err(format!(
            "remote snapshot contains more than {MAX_REMOTE_AGENTS} agents"
        ));
    }

    let mut workspaces = HashMap::new();
    for (workspace_display_order, workspace) in snapshot.workspaces.into_iter().enumerate() {
        validate_remote_id("workspace", &workspace.workspace_id)?;
        if workspaces
            .insert(
                workspace.workspace_id,
                (
                    workspace_display_order,
                    sanitize_remote_text(&workspace.label, MAX_REMOTE_TEXT_CHARS),
                ),
            )
            .is_some()
        {
            return Err("remote snapshot contains duplicate workspace ids".to_string());
        }
    }

    let mut tabs = HashMap::new();
    let mut tab_counts = HashMap::<String, usize>::new();
    let mut next_tab_display_order = HashMap::<String, usize>::new();
    for tab in snapshot.tabs {
        validate_remote_id("tab", &tab.tab_id)?;
        validate_remote_id("workspace", &tab.workspace_id)?;
        if !workspaces.contains_key(&tab.workspace_id) {
            return Err("remote tab references an unknown workspace".to_string());
        }
        *tab_counts.entry(tab.workspace_id.clone()).or_default() += 1;
        let display_order = next_tab_display_order
            .entry(tab.workspace_id.clone())
            .or_default();
        let tab_display_order = *display_order;
        *display_order = display_order.saturating_add(1);
        if tabs
            .insert(
                tab.tab_id,
                (
                    tab.workspace_id,
                    tab.number,
                    sanitize_remote_text(&tab.label, MAX_REMOTE_TEXT_CHARS),
                    tab.custom_label,
                    tab_display_order,
                ),
            )
            .is_some()
        {
            return Err("remote snapshot contains duplicate tab ids".to_string());
        }
    }

    let mut pane_orders = HashMap::new();
    let mut next_pane_order = HashMap::<String, usize>::new();
    for pane in snapshot.panes {
        validate_remote_id("terminal", &pane.terminal_id)?;
        validate_remote_id("workspace", &pane.workspace_id)?;
        validate_remote_id("tab", &pane.tab_id)?;
        validate_remote_id("pane", &pane.pane_id)?;
        if !workspaces.contains_key(&pane.workspace_id) {
            return Err("remote pane references an unknown workspace".to_string());
        }
        let Some((tab_workspace_id, _, _, _, _)) = tabs.get(&pane.tab_id) else {
            return Err("remote pane references an unknown tab".to_string());
        };
        if tab_workspace_id != &pane.workspace_id {
            return Err("remote pane tab belongs to a different workspace".to_string());
        }
        let order = next_pane_order.entry(pane.tab_id.clone()).or_default();
        let pane_order = *order;
        *order = order.saturating_add(1);
        let public_pane_order = pane.number.unwrap_or_else(|| pane_order.saturating_add(1));
        if pane_orders
            .insert(
                pane.pane_id,
                (
                    pane.workspace_id,
                    pane.tab_id,
                    pane.terminal_id,
                    pane_order,
                    public_pane_order,
                ),
            )
            .is_some()
        {
            return Err("remote snapshot contains duplicate pane ids".to_string());
        }
    }

    let mut pane_ids = HashSet::new();
    let mut agents = Vec::with_capacity(snapshot.agents.len());
    for info in snapshot.agents {
        if info.state_labels.len().saturating_add(info.tokens.len())
            > MAX_REMOTE_METADATA_ENTRIES_PER_AGENT
        {
            return Err(format!(
                "remote agent metadata contains more than {MAX_REMOTE_METADATA_ENTRIES_PER_AGENT} entries"
            ));
        }
        validate_remote_id("terminal", &info.terminal_id)?;
        validate_remote_id("workspace", &info.workspace_id)?;
        validate_remote_id("tab", &info.tab_id)?;
        validate_remote_id("pane", &info.pane_id)?;
        if !pane_ids.insert(info.pane_id.clone()) {
            return Err("remote snapshot contains duplicate agent pane ids".to_string());
        }
        let Some((
            pane_workspace_id,
            pane_tab_id,
            pane_terminal_id,
            pane_display_order,
            public_pane_order,
        )) = pane_orders.get(&info.pane_id)
        else {
            return Err("remote agent references an unknown pane".to_string());
        };
        if pane_workspace_id != &info.workspace_id
            || pane_tab_id != &info.tab_id
            || pane_terminal_id != &info.terminal_id
        {
            return Err("remote agent identity disagrees with its pane".to_string());
        }

        let Some((workspace_order, workspace_label)) = workspaces.get(&info.workspace_id).cloned()
        else {
            return Err("remote agent references an unknown workspace".to_string());
        };
        let Some((
            tab_workspace_id,
            public_tab_order,
            tab_label,
            custom_tab_label,
            tab_display_order,
        )) = tabs.get(&info.tab_id)
        else {
            return Err("remote agent references an unknown tab".to_string());
        };
        if tab_workspace_id != &info.workspace_id {
            return Err("remote agent tab belongs to a different workspace".to_string());
        }
        let tab_label = (tab_counts
            .get(tab_workspace_id)
            .copied()
            .unwrap_or_default()
            > 1
            || custom_tab_label.unwrap_or(true))
        .then(|| tab_label.clone());
        let fallback_agent = info
            .name
            .as_deref()
            .or(info.agent.as_deref())
            .unwrap_or("agent");
        let agent_label = sanitize_remote_text(
            info.display_agent.as_deref().unwrap_or(fallback_agent),
            MAX_REMOTE_TEXT_CHARS,
        );
        let agent_kind_label = info
            .agent
            .as_deref()
            .map(|label| sanitize_remote_text(label, MAX_REMOTE_TEXT_CHARS));
        let known_agent = info
            .agent
            .as_deref()
            .and_then(crate::detect::parse_agent_label);
        let (state, seen) = presentation_state(info.agent_status);

        agents.push(RemoteAgentPresentation {
            workspace_id: info.workspace_id,
            tab_id: info.tab_id,
            pane_id: info.pane_id.clone(),
            workspace_label,
            tab_label,
            pane_label: info
                .title
                .as_deref()
                .map(|value| sanitize_remote_text(value, MAX_REMOTE_TEXT_CHARS)),
            terminal_title: info
                .terminal_title
                .as_deref()
                .map(|value| sanitize_remote_text(value, MAX_REMOTE_TEXT_CHARS)),
            terminal_title_stripped: info
                .terminal_title_stripped
                .as_deref()
                .map(|value| sanitize_remote_text(value, MAX_REMOTE_TEXT_CHARS)),
            agent_label,
            agent_kind_label,
            agent: known_agent,
            state,
            seen,
            state_labels: sanitize_remote_map(info.state_labels, MAX_REMOTE_TEXT_CHARS),
            tokens: sanitize_remote_map(info.tokens, MAX_REMOTE_TOKEN_CHARS),
            display_order: (workspace_order, *tab_display_order, *pane_display_order),
            order: (workspace_order, *public_tab_order, *public_pane_order),
        });
    }

    Ok(RemoteAgentSnapshot {
        version: sanitize_remote_text(&snapshot.version, MAX_REMOTE_TEXT_CHARS),
        protocol: snapshot.protocol,
        agents,
    })
}

fn validate_remote_id(kind: &str, value: &str) -> Result<(), String> {
    if value.is_empty()
        || value.chars().count() > MAX_REMOTE_ID_CHARS
        || value.chars().any(char::is_control)
    {
        return Err(format!("remote {kind} id is invalid"));
    }
    Ok(())
}

fn sanitize_remote_map(
    values: HashMap<String, String>,
    max_value_chars: usize,
) -> HashMap<String, String> {
    values
        .into_iter()
        .filter(|(key, _)| {
            !key.is_empty()
                && key.len() <= 32
                && key
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
        })
        .map(|(key, value)| (key, sanitize_remote_text(&value, max_value_chars)))
        .collect()
}

fn sanitize_remote_text(value: &str, max_chars: usize) -> String {
    value
        .chars()
        .filter(|ch| !ch.is_control() && !is_bidi_control(*ch))
        .take(max_chars)
        .collect::<String>()
}

fn is_bidi_control(ch: char) -> bool {
    matches!(
        ch,
        '\u{061c}'
            | '\u{200e}'
            | '\u{200f}'
            | '\u{202a}'..='\u{202e}'
            | '\u{2066}'..='\u{2069}'
    )
}

fn presentation_state(status: AgentStatus) -> (AgentState, bool) {
    match status {
        AgentStatus::Idle => (AgentState::Idle, true),
        AgentStatus::Working => (AgentState::Working, true),
        AgentStatus::Blocked => (AgentState::Blocked, true),
        AgentStatus::Done => (AgentState::Idle, false),
        AgentStatus::Unknown => (AgentState::Unknown, true),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::schema::{
        AgentInfo, PaneInfo, ResponseResult, SessionSnapshot, SuccessResponse, TabInfo,
        WorkspaceInfo,
    };

    fn source(target: &str) -> RemoteAgentSourceConfig {
        RemoteAgentSourceConfig {
            target: target.to_string(),
            label: None,
            session: "default".to_string(),
        }
    }

    fn snapshot(status: AgentStatus) -> RemoteAgentSnapshot {
        RemoteAgentSnapshot {
            version: "0.8.0".into(),
            protocol: crate::protocol::PROTOCOL_VERSION,
            agents: vec![RemoteAgentPresentation {
                workspace_id: "workspace:1".into(),
                tab_id: "tab:1".into(),
                pane_id: "pane:1".into(),
                workspace_label: "repo".into(),
                tab_label: None,
                pane_label: None,
                terminal_title: None,
                terminal_title_stripped: None,
                agent_label: "claude".into(),
                agent_kind_label: Some("claude".into()),
                agent: Some(Agent::Claude),
                state: presentation_state(status).0,
                seen: presentation_state(status).1,
                state_labels: HashMap::new(),
                tokens: HashMap::new(),
                display_order: (1, 1, 1),
                order: (1, 1, 1),
            }],
        }
    }

    fn fleet_agent(tab_id: &str, pane_id: &str, terminal_id: &str) -> FleetAgentInfo {
        FleetAgentInfo {
            terminal_id: terminal_id.into(),
            name: None,
            agent: Some("claude".into()),
            title: None,
            terminal_title: None,
            terminal_title_stripped: None,
            display_agent: None,
            agent_status: AgentStatus::Working,
            state_labels: HashMap::new(),
            tokens: HashMap::new(),
            workspace_id: "w1".into(),
            tab_id: tab_id.into(),
            pane_id: pane_id.into(),
        }
    }

    fn spawn_idle_test_watcher(
        _source: RemoteAgentSourceConfig,
        _host: RemoteHostKey,
        _generation: u64,
        cancel: Arc<AtomicBool>,
        _pending: Arc<AtomicBool>,
        _event_tx: mpsc::Sender<AppEvent>,
    ) -> io::Result<JoinHandle<()>> {
        std::thread::Builder::new()
            .name("herdr-remote-agents-test".into())
            .spawn(move || {
                while !cancel.load(Ordering::Acquire) {
                    std::thread::sleep(Duration::from_millis(1));
                }
            })
    }

    #[test]
    fn supervisor_preserves_generation_for_labels_and_renews_identity() {
        let (event_tx, _event_rx) = mpsc::channel(1);
        let mut supervisor = RemoteAgentSupervisor::default();
        let mut original = source("box");
        original.label = Some("first".into());

        let first = supervisor.reconcile_with(
            vec![original.clone()],
            event_tx.clone(),
            spawn_idle_test_watcher,
        );
        assert_eq!(first.len(), 1);
        let first_generation = first[0].generation;

        original.label = Some("renamed".into());
        let renamed = supervisor.reconcile_with(
            vec![original.clone()],
            event_tx.clone(),
            spawn_idle_test_watcher,
        );
        assert_eq!(renamed[0].generation, first_generation);
        assert_eq!(renamed[0].label, "renamed");

        original.session = "work".into();
        let changed_identity =
            supervisor.reconcile_with(vec![original], event_tx.clone(), spawn_idle_test_watcher);
        assert!(changed_identity[0].generation > first_generation);

        assert!(supervisor
            .reconcile_with(Vec::new(), event_tx.clone(), spawn_idle_test_watcher)
            .is_empty());
        let readded =
            supervisor.reconcile_with(vec![source("box")], event_tx, spawn_idle_test_watcher);
        assert!(readded[0].generation > changed_identity[0].generation);
    }

    #[test]
    fn registry_rejects_stale_generation_after_reconfigure() {
        let key = RemoteHostKey::for_source(&source("box"));
        let mut registry = RemoteAgentRegistry::default();
        registry.reconcile(&[RemoteHostRegistration {
            key: key.clone(),
            label: "box".into(),
            generation: 2,
            order: 0,
        }]);
        let mut seq = 0;

        assert!(!registry.apply_update(
            RemoteAgentUpdate::immediate(
                key,
                1,
                RemoteAgentUpdateKind::Snapshot(snapshot(AgentStatus::Working)),
            ),
            &mut seq,
        ));
        assert!(registry.panel_agents().is_empty());
    }

    #[test]
    fn offline_keeps_last_snapshot_and_relabels_its_state_in_ui_layer() {
        let key = RemoteHostKey::for_source(&source("box"));
        let mut registry = RemoteAgentRegistry::default();
        registry.reconcile(&[RemoteHostRegistration {
            key: key.clone(),
            label: "box".into(),
            generation: 1,
            order: 0,
        }]);
        let mut seq = 0;
        assert!(registry.apply_update(
            RemoteAgentUpdate::immediate(
                key.clone(),
                1,
                RemoteAgentUpdateKind::Snapshot(snapshot(AgentStatus::Working)),
            ),
            &mut seq,
        ));
        assert!(registry.apply_update(
            RemoteAgentUpdate::immediate(
                key,
                1,
                RemoteAgentUpdateKind::Offline {
                    error: "network down".into(),
                },
            ),
            &mut seq,
        ));

        let agents = registry.panel_agents();
        assert_eq!(agents.len(), 1);
        assert!(!agents[0].online);
        assert_eq!(agents[0].presentation.state, AgentState::Working);
    }

    #[test]
    fn identical_snapshot_does_not_request_another_render() {
        let key = RemoteHostKey::for_source(&source("box"));
        let mut registry = RemoteAgentRegistry::default();
        registry.reconcile(&[RemoteHostRegistration {
            key: key.clone(),
            label: "box".into(),
            generation: 1,
            order: 0,
        }]);
        let mut seq = 0;
        let update = || {
            RemoteAgentUpdate::immediate(
                key.clone(),
                1,
                RemoteAgentUpdateKind::Snapshot(snapshot(AgentStatus::Working)),
            )
        };

        assert!(registry.apply_update(update(), &mut seq));
        assert!(!registry.apply_update(update(), &mut seq));
        assert_eq!(seq, 0);
    }

    #[test]
    fn identical_remote_pane_ids_remain_distinct_across_hosts() {
        let first = RemoteHostKey::for_source(&source("first"));
        let second = RemoteHostKey::for_source(&source("second"));
        let mut registry = RemoteAgentRegistry::default();
        registry.reconcile(&[
            RemoteHostRegistration {
                key: first.clone(),
                label: "first".into(),
                generation: 1,
                order: 0,
            },
            RemoteHostRegistration {
                key: second.clone(),
                label: "second".into(),
                generation: 2,
                order: 1,
            },
        ]);
        let mut seq = 0;
        for (host, generation) in [(first, 1), (second, 2)] {
            assert!(registry.apply_update(
                RemoteAgentUpdate::immediate(
                    host,
                    generation,
                    RemoteAgentUpdateKind::Snapshot(snapshot(AgentStatus::Working)),
                ),
                &mut seq,
            ));
        }

        let agents = registry.panel_agents();
        assert_eq!(agents.len(), 2);
        assert_eq!(agents[0].presentation.pane_id, "pane:1");
        assert_eq!(agents[1].presentation.pane_id, "pane:1");
        assert_ne!(agents[0].host_key, agents[1].host_key);
    }

    #[test]
    fn status_changes_receive_local_observation_sequence() {
        let key = RemoteHostKey::for_source(&source("box"));
        let mut registry = RemoteAgentRegistry::default();
        registry.reconcile(&[RemoteHostRegistration {
            key: key.clone(),
            label: "box".into(),
            generation: 1,
            order: 0,
        }]);
        let mut seq = 41;
        assert!(registry.apply_update(
            RemoteAgentUpdate::immediate(
                key.clone(),
                1,
                RemoteAgentUpdateKind::Snapshot(snapshot(AgentStatus::Working)),
            ),
            &mut seq,
        ));
        assert_eq!(registry.panel_agents()[0].state_change_seq, None);

        assert!(registry.apply_update(
            RemoteAgentUpdate::immediate(
                key,
                1,
                RemoteAgentUpdateKind::Snapshot(snapshot(AgentStatus::Blocked)),
            ),
            &mut seq,
        ));
        assert_eq!(registry.panel_agents()[0].state_change_seq, Some(42));
    }

    #[test]
    fn tracked_update_allows_only_one_pending_event_per_host() {
        let pending = Arc::new(AtomicBool::new(false));
        let key = RemoteHostKey::for_source(&source("box"));
        let first = RemoteAgentUpdate::tracked(
            key.clone(),
            1,
            RemoteAgentUpdateKind::Snapshot(snapshot(AgentStatus::Working)),
            &pending,
        )
        .unwrap();
        assert!(RemoteAgentUpdate::tracked(
            key.clone(),
            1,
            RemoteAgentUpdateKind::Snapshot(snapshot(AgentStatus::Blocked)),
            &pending,
        )
        .is_none());

        drop(first);

        assert!(RemoteAgentUpdate::tracked(
            key,
            1,
            RemoteAgentUpdateKind::Snapshot(snapshot(AgentStatus::Blocked)),
            &pending,
        )
        .is_some());
    }

    #[test]
    fn bounded_reader_rejects_oversized_line_without_allocating_it() {
        let input = b"123456\n";
        let mut reader = BufReader::new(&input[..]);
        let err = read_bounded_line(&mut reader, 4).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn parser_sanitizes_remote_control_characters() {
        let response = SuccessResponse {
            id: "snapshot".into(),
            result: ResponseResult::SessionSnapshot {
                snapshot: Box::new(SessionSnapshot {
                    version: "0.8.0".into(),
                    protocol: crate::protocol::PROTOCOL_VERSION,
                    focused_workspace_id: None,
                    focused_tab_id: None,
                    focused_pane_id: None,
                    workspaces: vec![WorkspaceInfo {
                        workspace_id: "workspace:1".into(),
                        number: 1,
                        label: "repo\u{202e}\u{1b}[31m".into(),
                        focused: false,
                        pane_count: 1,
                        tab_count: 1,
                        active_tab_id: "tab:1".into(),
                        agent_status: AgentStatus::Working,
                        tokens: HashMap::new(),
                        worktree: None,
                    }],
                    tabs: vec![TabInfo {
                        tab_id: "tab:1".into(),
                        workspace_id: "workspace:1".into(),
                        number: 1,
                        label: "review".into(),
                        custom_label: true,
                        focused: false,
                        pane_count: 1,
                        agent_status: AgentStatus::Working,
                    }],
                    panes: vec![PaneInfo {
                        pane_id: "pane:1".into(),
                        number: 1,
                        terminal_id: "terminal:1".into(),
                        workspace_id: "workspace:1".into(),
                        tab_id: "tab:1".into(),
                        focused: false,
                        cwd: None,
                        foreground_cwd: None,
                        label: None,
                        agent: Some("claude".into()),
                        title: Some("unsafe\nname".into()),
                        terminal_title: None,
                        terminal_title_stripped: None,
                        display_agent: None,
                        agent_status: AgentStatus::Working,
                        state_labels: HashMap::new(),
                        tokens: HashMap::new(),
                        agent_session: None,
                        scroll: None,
                        revision: 1,
                    }],
                    layouts: Vec::new(),
                    agents: vec![AgentInfo {
                        terminal_id: "terminal:1".into(),
                        name: None,
                        agent: Some("claude".into()),
                        title: Some("unsafe\nname".into()),
                        terminal_title: None,
                        terminal_title_stripped: None,
                        display_agent: None,
                        agent_status: AgentStatus::Working,
                        screen_detection_skipped: false,
                        state_labels: HashMap::new(),
                        tokens: HashMap::new(),
                        agent_session: None,
                        workspace_id: "workspace:1".into(),
                        tab_id: "tab:1".into(),
                        pane_id: "pane:1".into(),
                        focused: false,
                        launch_pending: false,
                        interactive_ready: true,
                        state_change_seq: 1,
                        cwd: None,
                        foreground_cwd: None,
                        revision: 1,
                    }],
                }),
            },
        };
        let line = serde_json::to_vec(&response).unwrap();

        let parsed = parse_snapshot_line(&line).unwrap();

        assert_eq!(parsed.agents[0].workspace_label, "repo[31m");
        assert_eq!(parsed.agents[0].tab_label.as_deref(), Some("review"));
        assert_eq!(parsed.agents[0].pane_label.as_deref(), Some("unsafename"));
    }

    #[test]
    fn parser_rejects_tabs_from_unknown_workspaces() {
        let err = snapshot_for_fleet(FleetSessionSnapshot {
            version: "0.8.0".into(),
            protocol: crate::protocol::PROTOCOL_VERSION,
            workspaces: Vec::new(),
            tabs: vec![FleetTabInfo {
                tab_id: "tab:1".into(),
                workspace_id: "workspace:missing".into(),
                number: 1,
                label: "1".into(),
                custom_label: Some(false),
            }],
            panes: Vec::new(),
            agents: Vec::new(),
        })
        .unwrap_err();

        assert!(err.contains("unknown workspace"));
    }

    #[test]
    fn parser_degrades_unrecognized_agent_status_to_unknown() {
        let line = format!(
            r#"{{"result":{{"type":"session_snapshot","snapshot":{{"version":"0.8.0","protocol":{},"workspaces":[{{"workspace_id":"w1","label":"repo"}}],"tabs":[{{"tab_id":"t1","workspace_id":"w1","number":1,"label":"1"}}],"panes":[{{"pane_id":"p1","number":1,"terminal_id":"term1","workspace_id":"w1","tab_id":"t1"}}],"agents":[{{"terminal_id":"term1","agent":"claude","agent_status":"hibernating","workspace_id":"w1","tab_id":"t1","pane_id":"p1"}}]}}}}}}"#,
            crate::protocol::PROTOCOL_VERSION
        );

        let parsed = parse_snapshot_line(line.as_bytes()).unwrap();

        assert_eq!(parsed.agents[0].state, AgentState::Unknown);
        assert!(parsed.agents[0].seen);
    }

    #[test]
    fn parser_rejects_incompatible_remote_protocol() {
        let err = snapshot_for_fleet(FleetSessionSnapshot {
            version: "future".into(),
            protocol: crate::protocol::PROTOCOL_VERSION.saturating_add(1),
            workspaces: Vec::new(),
            tabs: Vec::new(),
            panes: Vec::new(),
            agents: Vec::new(),
        })
        .unwrap_err();

        assert!(err.contains("incompatible"));
    }

    #[test]
    fn moved_remote_tabs_keep_display_order_and_public_sort_numbers() {
        let parsed = snapshot_for_fleet(FleetSessionSnapshot {
            version: "0.8.0".into(),
            protocol: crate::protocol::PROTOCOL_VERSION,
            workspaces: vec![FleetWorkspaceInfo {
                workspace_id: "w1".into(),
                label: "repo".into(),
            }],
            tabs: vec![
                FleetTabInfo {
                    tab_id: "w1:t2".into(),
                    workspace_id: "w1".into(),
                    number: 2,
                    label: "1".into(),
                    custom_label: Some(false),
                },
                FleetTabInfo {
                    tab_id: "w1:t1".into(),
                    workspace_id: "w1".into(),
                    number: 1,
                    label: "2".into(),
                    custom_label: Some(false),
                },
            ],
            panes: vec![
                FleetPaneInfo {
                    pane_id: "w1:p2".into(),
                    number: Some(2),
                    terminal_id: "terminal:2".into(),
                    workspace_id: "w1".into(),
                    tab_id: "w1:t2".into(),
                },
                FleetPaneInfo {
                    pane_id: "w1:p1".into(),
                    number: Some(1),
                    terminal_id: "terminal:1".into(),
                    workspace_id: "w1".into(),
                    tab_id: "w1:t1".into(),
                },
            ],
            agents: vec![
                fleet_agent("w1:t1", "w1:p1", "terminal:1"),
                fleet_agent("w1:t2", "w1:p2", "terminal:2"),
            ],
        })
        .unwrap();

        let by_pane = parsed
            .agents
            .iter()
            .map(|agent| (agent.pane_id.as_str(), (agent.display_order, agent.order)))
            .collect::<HashMap<_, _>>();
        assert_eq!(by_pane["w1:p2"], ((0, 0, 0), (0, 2, 2)));
        assert_eq!(by_pane["w1:p1"], ((0, 1, 0), (0, 1, 1)));

        let key = RemoteHostKey::for_source(&source("box"));
        let mut registry = RemoteAgentRegistry::default();
        registry.reconcile(&[RemoteHostRegistration {
            key: key.clone(),
            label: "box".into(),
            generation: 1,
            order: 0,
        }]);
        let mut seq = 0;
        registry.apply_update(
            RemoteAgentUpdate::immediate(key, 1, RemoteAgentUpdateKind::Snapshot(parsed)),
            &mut seq,
        );
        let displayed = registry.panel_agents();
        assert_eq!(displayed[0].presentation.pane_id, "w1:p2");
        assert_eq!(displayed[1].presentation.pane_id, "w1:p1");
    }

    #[test]
    fn legacy_single_tab_without_custom_marker_keeps_its_label_visible() {
        let parsed = snapshot_for_fleet(FleetSessionSnapshot {
            version: "0.8.0".into(),
            protocol: crate::protocol::PROTOCOL_VERSION,
            workspaces: vec![FleetWorkspaceInfo {
                workspace_id: "w1".into(),
                label: "repo".into(),
            }],
            tabs: vec![FleetTabInfo {
                tab_id: "w1:t1".into(),
                workspace_id: "w1".into(),
                number: 1,
                label: "review".into(),
                custom_label: None,
            }],
            panes: vec![FleetPaneInfo {
                pane_id: "w1:p1".into(),
                number: None,
                terminal_id: "terminal:1".into(),
                workspace_id: "w1".into(),
                tab_id: "w1:t1".into(),
            }],
            agents: vec![fleet_agent("w1:t1", "w1:p1", "terminal:1")],
        })
        .unwrap();

        assert_eq!(parsed.agents[0].tab_label.as_deref(), Some("review"));
        assert_eq!(parsed.agents[0].order.2, 1);
    }

    #[test]
    fn remote_command_is_non_interactive_and_quotes_session() {
        let mut configured = source("user@box");
        configured.session = "work".into();
        let ssh = snapshot_stream_command(&configured);
        let args = ssh
            .get_args()
            .map(|arg| arg.to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        assert!(args.windows(2).any(|pair| pair == ["-o", "BatchMode=yes"]));
        assert!(args
            .windows(2)
            .any(|pair| pair == ["-o", "StrictHostKeyChecking=yes"]));
        for option in [
            "ForwardAgent=no",
            "ForwardX11=no",
            "GSSAPIDelegateCredentials=no",
            "PermitLocalCommand=no",
            "ControlMaster=no",
            "ControlPath=none",
            "ControlPersist=no",
        ] {
            assert!(args.windows(2).any(|pair| pair == ["-o", option]));
        }
        assert!(args.contains(&"user@box".to_string()));
        let command = args.last().unwrap();
        assert_eq!(command, "/bin/sh -s");
        let script = remote_snapshot_script(&configured.session);
        assert!(script.contains("fleet_session='work'"));
        assert!(script.contains("command -v herdr"));
        assert!(script.contains("api help"));
        assert!(script.contains(&format!(
            "\"protocol\": {},",
            crate::protocol::PROTOCOL_VERSION
        )));
    }
}
