use std::collections::HashMap;
use std::ffi::{OsStr, OsString};
use std::io::{self, Write, stdout};
#[cfg(unix)]
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::time::{Duration, Instant};

use agent_client_protocol_schema::v1::{
    AgentResponse, CloseSessionRequest, Error as AcpError, LoadSessionRequest, NewSessionRequest,
    RequestId, Response, SessionId,
};
use flume::{Receiver, Sender, WeakSender};
use maki_agent::ChildGuard;
use serde::de::DeserializeOwned;
use serde_json::Value;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{ChildStdin, ChildStdout, Command};
use tokio::runtime::Handle;
use tracing::{debug, warn};

use crate::methods;

/// The shim's `initialize` handshake with each child. Clients never use a
/// negative id, so a child answer carrying this one is dropped.
const HANDSHAKE_REQUEST_ID: i64 = -1;
const FIRST_ASK_REQUEST_ID: i64 = 1000;
const CHILD_EXIT_TIMEOUT: Duration = Duration::from_secs(5);
const MULTIPLEX_FLAG: &str = "--multiplex";

pub type SpawnFn = Arc<dyn Fn(&Path) -> io::Result<ChildLink> + Send + Sync>;
type ChildKill = Box<dyn FnOnce() + Send>;

/// One child `maki acp` process, as the router sees it. Dropping `stdin` makes
/// the child exit on its own. The router treats stdout EOF as the end of the
/// child, and `exited` only bounds the wait before a kill.
pub struct ChildLink {
    stdin: Sender<String>,
    lines: Receiver<Value>,
    exited: Receiver<()>,
    kill: ChildKill,
}

/// The executable and arguments every child starts from. The shim keeps its own
/// cwd; only [`ChildCommand::spawn`] applies the session cwd.
pub struct ChildCommand {
    program: PathBuf,
    args: Vec<OsString>,
}

impl ChildCommand {
    /// Drops the flag that started the shim, so every other argument, current
    /// and future, reaches the child unchanged.
    pub fn new(program: PathBuf, args: impl IntoIterator<Item = OsString>) -> Self {
        Self {
            program,
            args: args
                .into_iter()
                .filter(|arg| arg.as_os_str() != OsStr::new(MULTIPLEX_FLAG))
                .collect(),
        }
    }

    pub fn spawn(&self, cwd: &Path) -> io::Result<ChildLink> {
        let mut command = std::process::Command::new(&self.program);
        command.args(&self.args).current_dir(cwd);
        // `ChildGuard` kills the process group, which only works when the child
        // leads one. Every other spawner in maki calls `setsid` too.
        #[cfg(unix)]
        unsafe {
            command.pre_exec(|| {
                let _ = rustix::process::setsid();
                Ok(())
            });
        }
        // Converting drops stdio settings, so they are applied on the wrapper.
        let mut command: Command = command.into();
        command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit());
        let mut child = command.spawn()?;
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| io::Error::other("maki acp child has no stdin"))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| io::Error::other("maki acp child has no stdout"))?;
        let (stdin_tx, stdin_rx) = flume::unbounded();
        let (lines_tx, lines_rx) = flume::unbounded();
        let (exit_tx, exit_rx) = flume::bounded(1);
        let (kill_tx, kill_rx) = flume::bounded(1);

        tokio::spawn(write_child(stdin, stdin_rx));
        tokio::spawn(read_child(stdout, lines_tx));
        tokio::spawn(watch_child(ChildGuard::new(child), kill_rx, exit_tx));

        Ok(ChildLink {
            stdin: stdin_tx,
            lines: lines_rx,
            exited: exit_rx,
            kill: Box::new(move || {
                let _ = kill_tx.send(());
            }),
        })
    }
}

pub fn run(spawn: SpawnFn) -> color_eyre::Result<()> {
    Handle::current().block_on(serve(spawn))
}

async fn serve(spawn: SpawnFn) -> color_eyre::Result<()> {
    let (out_tx, out_rx) = flume::unbounded::<Value>();
    let writer_task = tokio::spawn(write_stdout(out_rx));

    let (in_tx, in_rx) = flume::unbounded::<Incoming>();
    let router_lines = in_tx.downgrade();
    let reader_task = tokio::spawn(read_stdin(in_tx));

    let mut router = Router::new(spawn, router_lines);
    router.run(in_rx, &out_tx).await;
    drop(out_tx);
    let _ = writer_task.await;
    reader_task.await??;

    Ok(())
}

async fn write_stdout(out_rx: Receiver<Value>) {
    let stdout = stdout();
    while let Ok(message) = out_rx.recv_async().await {
        let mut handle = stdout.lock();
        if serde_json::to_writer(&mut handle, &message).is_ok() {
            let _ = handle.write_all(b"\n");
            let _ = handle.flush();
        }
    }
}

/// Only this task holds a strong sender, so EOF closes the router loop even
/// while children are still alive.
async fn read_stdin(tx: Sender<Incoming>) -> io::Result<()> {
    let mut reader = BufReader::new(tokio::io::stdin());
    loop {
        let mut line = String::new();
        if reader.read_line(&mut line).await? == 0 {
            return Ok(());
        }
        if tx.send_async(Incoming::Client(line)).await.is_err() {
            return Ok(());
        }
    }
}

enum Incoming {
    Client(String),
    Child(ChildId, Value),
    /// The child's stdout reached EOF, so every line it wrote has arrived.
    Closed(ChildId),
}

type ChildId = u64;

struct RunningChild {
    stdin: Sender<String>,
    exited: Receiver<()>,
    kill: ChildKill,
    /// Where [`ChildCommand::spawn`] started it, for the `session/load` warning.
    cwd: PathBuf,
}

enum Pending {
    /// `session/new` names its session only in the answer.
    New { child: ChildId },
    /// `session/load` for an id no child owns yet.
    Load { child: ChildId, session: SessionId },
    /// Any other request forwarded to a child, so a dead child can answer it.
    Forwarded { child: ChildId, session: SessionId },
    /// `session/close`, whose response retires the child once it reaches the
    /// client.
    Close { child: ChildId, session: SessionId },
}

impl Pending {
    fn child(&self) -> ChildId {
        match self {
            Self::New { child }
            | Self::Load { child, .. }
            | Self::Forwarded { child, .. }
            | Self::Close { child, .. } => *child,
        }
    }

    fn session(&self) -> Option<&SessionId> {
        match self {
            Self::New { .. } => None,
            Self::Load { session, .. }
            | Self::Forwarded { session, .. }
            | Self::Close { session, .. } => Some(session),
        }
    }
}

struct Ask {
    child: ChildId,
    original: RequestId,
}

struct Router {
    spawn: SpawnFn,
    line_tx: WeakSender<Incoming>,
    children: HashMap<ChildId, RunningChild>,
    sessions: HashMap<SessionId, ChildId>,
    pending: HashMap<RequestId, Pending>,
    asks: HashMap<RequestId, Ask>,
    next_child: ChildId,
    next_ask: i64,
    client_init: Value,
}

impl Router {
    fn new(spawn: SpawnFn, line_tx: WeakSender<Incoming>) -> Self {
        Self {
            spawn,
            line_tx,
            children: HashMap::new(),
            sessions: HashMap::new(),
            pending: HashMap::new(),
            asks: HashMap::new(),
            next_child: 0,
            next_ask: FIRST_ASK_REQUEST_ID,
            client_init: Value::Null,
        }
    }

    async fn run(&mut self, incoming: Receiver<Incoming>, out: &Sender<Value>) {
        while let Ok(message) = incoming.recv_async().await {
            match message {
                Incoming::Client(line) => self.handle_client(&line, out).await,
                Incoming::Child(id, value) => self.handle_child(id, value, out).await,
                Incoming::Closed(child) => self.handle_exit(child, out),
            }
        }
        self.shutdown().await;
    }

    async fn handle_client(&mut self, line: &str, out: &Sender<Value>) {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            return;
        }
        let raw: Value = match serde_json::from_str(trimmed) {
            Ok(raw) => raw,
            Err(e) => {
                warn!(error = %e, "invalid JSON on stdin");
                respond(out, RequestId::Null, Err(AcpError::parse_error()));
                return;
            }
        };
        if raw.get("result").is_some() || raw.get("error").is_some() {
            self.handle_client_response(&raw);
            return;
        }
        let Some(method) = raw.get("method").and_then(Value::as_str) else {
            if let Some(id) = raw.get("id").map(request_id) {
                respond(out, id, Err(AcpError::invalid_request()));
            }
            return;
        };
        let Some(id) = raw.get("id").map(request_id) else {
            self.handle_notification(method, &raw);
            return;
        };
        match method {
            "initialize" => {
                self.client_init = raw.get("params").cloned().unwrap_or(Value::Null);
                respond(
                    out,
                    id,
                    Ok(AgentResponse::InitializeResponse(
                        methods::initialize_response(),
                    )),
                );
            }
            "session/new" => self.new_session(&raw, id, out).await,
            "session/load" => self.load_session(&raw, id, out).await,
            "session/close" => self.close_session(&raw, id, out).await,
            _ => self.forward_session_request(method, &raw, id, out).await,
        }
    }

    /// Everything a child owns arrives with a `sessionId`, whatever the method.
    async fn forward_session_request(
        &mut self,
        method: &str,
        raw: &Value,
        id: RequestId,
        out: &Sender<Value>,
    ) {
        let Some(session) = raw.pointer("/params/sessionId").and_then(Value::as_str) else {
            debug!(method, "ignoring a request the shim cannot route");
            return respond(out, id, Err(AcpError::method_not_found()));
        };
        let session = SessionId::new(session);
        let Some(&child) = self.sessions.get(&session) else {
            return respond(
                out,
                id,
                Err(AcpError::resource_not_found(Some(session.to_string()))),
            );
        };
        self.forward(child, raw, &session, id, out).await;
    }

    fn handle_notification(&self, method: &str, raw: &Value) {
        let Some(session) = raw.pointer("/params/sessionId").and_then(Value::as_str) else {
            debug!(method, "ignoring a notification the shim cannot route");
            return;
        };
        let session = SessionId::new(session);
        let Some(&child) = self.sessions.get(&session) else {
            warn!(
                method = %method,
                session = %session,
                "notification for a session the shim does not own"
            );
            return;
        };
        if let Err(error) = self.send(child, raw) {
            warn!(
                method = %method,
                session = %session,
                %error,
                "cannot forward a client notification"
            );
        }
    }

    async fn new_session(&mut self, raw: &Value, id: RequestId, out: &Sender<Value>) {
        let request: NewSessionRequest = match parse_params(raw) {
            Ok(request) => request,
            Err(error) => return respond(out, id, Err(error)),
        };
        let child = match self.start_child(&request.cwd).await {
            Ok(child) => child,
            Err(error) => return respond(out, id, Err(error)),
        };
        self.pending.insert(id.clone(), Pending::New { child });
        if let Err(error) = self.send(child, raw) {
            self.pending.remove(&id);
            self.retire(child);
            respond(out, id, Err(error));
        }
    }

    async fn load_session(&mut self, raw: &Value, id: RequestId, out: &Sender<Value>) {
        let request: LoadSessionRequest = match parse_params(raw) {
            Ok(request) => request,
            Err(error) => return respond(out, id, Err(error)),
        };
        if let Some(&child) = self.sessions.get(&request.session_id) {
            let session = request.session_id.clone();
            self.warn_on_cwd_change(child, &session, &request.cwd);
            self.forward(child, raw, &session, id, out).await;
            return;
        }
        let child = match self.start_child(&request.cwd).await {
            Ok(child) => child,
            Err(error) => return respond(out, id, Err(error)),
        };
        self.pending.insert(
            id.clone(),
            Pending::Load {
                child,
                session: request.session_id,
            },
        );
        if let Err(error) = self.send(child, raw) {
            self.pending.remove(&id);
            self.retire(child);
            respond(out, id, Err(error));
        }
    }

    /// The child answers only once its claim is released, so the shim retires
    /// the child after forwarding that answer, never before.
    async fn close_session(&mut self, raw: &Value, id: RequestId, out: &Sender<Value>) {
        let request: CloseSessionRequest = match parse_params(raw) {
            Ok(request) => request,
            Err(error) => return respond(out, id, Err(error)),
        };
        let Some(&child) = self.sessions.get(&request.session_id) else {
            return respond(
                out,
                id,
                Err(AcpError::resource_not_found(Some(
                    request.session_id.to_string(),
                ))),
            );
        };
        self.pending.insert(
            id.clone(),
            Pending::Close {
                child,
                session: request.session_id,
            },
        );
        if let Err(error) = self.send(child, raw) {
            self.pending.remove(&id);
            self.retire(child);
            respond(out, id, Err(error));
        }
    }

    async fn start_child(&mut self, cwd: &Path) -> Result<ChildId, AcpError> {
        let link = (self.spawn)(cwd).map_err(|e| spawn_error(cwd, &e))?;
        let child = self.next_child;
        self.next_child += 1;
        let ChildLink {
            stdin,
            lines,
            exited,
            kill,
        } = link;
        let line_tx = self.line_tx.clone();
        tokio::spawn(async move {
            while let Ok(value) = lines.recv_async().await {
                let Some(line_tx) = line_tx.upgrade() else {
                    break;
                };
                if line_tx
                    .send_async(Incoming::Child(child, value))
                    .await
                    .is_err()
                {
                    break;
                }
            }
            if let Some(tx) = line_tx.upgrade() {
                let _ = tx.send_async(Incoming::Closed(child)).await;
            }
        });
        self.children.insert(
            child,
            RunningChild {
                stdin,
                exited,
                kill,
                cwd: cwd.to_path_buf(),
            },
        );
        if let Err(error) = self.handshake(child) {
            self.retire(child);
            return Err(error);
        }
        Ok(child)
    }

    async fn handle_child(&mut self, child: ChildId, value: Value, out: &Sender<Value>) {
        if !self.children.contains_key(&child) {
            debug!(child, "dropping a line from a retired child");
            return;
        }
        let id = value.get("id").map(request_id);
        if value.get("method").is_some() {
            match id {
                Some(id) => self.forward_ask(child, id, &value, out),
                None => {
                    self.warn_on_unknown_session(&value);
                    send(out, &value);
                }
            }
            return;
        }
        let Some(id) = id else {
            warn!(child, "dropping a child message with neither id nor method");
            return;
        };
        if id == RequestId::Number(HANDSHAKE_REQUEST_ID) {
            return;
        }
        let mut failed = false;
        let mut closed = false;
        match self.pending.get(&id).map(Pending::child) {
            Some(owner) if owner != child => {
                warn!(%id, child, "child answered a request owned by another child");
            }
            _ => {
                if let Some(pending) = self.pending.remove(&id) {
                    match pending {
                        Pending::New { .. } => {
                            if value.get("error").is_some() {
                                failed = true;
                            } else if let Some(session) =
                                value.pointer("/result/sessionId").and_then(Value::as_str)
                            {
                                self.sessions.insert(SessionId::new(session), child);
                            } else {
                                warn!(child, "session/new answered without a sessionId");
                            }
                        }
                        Pending::Load { session, .. } => {
                            if value.get("error").is_some() {
                                failed = true;
                            } else {
                                self.sessions.insert(session, child);
                            }
                        }
                        Pending::Forwarded { .. } => {}
                        Pending::Close { .. } => closed = true,
                    }
                }
            }
        }
        send(out, &value);
        if closed || failed {
            self.retire(child);
        }
    }

    /// Children mint their own ids, so two asks can share one. The client sees a
    /// shim id and the answer goes back under the child's original one.
    fn forward_ask(
        &mut self,
        child: ChildId,
        original: RequestId,
        request: &Value,
        out: &Sender<Value>,
    ) {
        let shim_id = RequestId::Number(self.next_ask);
        self.next_ask += 1;
        let mut forwarded = request.clone();
        forwarded["id"] = serde_json::to_value(&shim_id).unwrap_or(Value::Null);
        self.asks.insert(shim_id, Ask { child, original });
        send(out, &forwarded);
    }

    fn handle_client_response(&mut self, raw: &Value) {
        let Some(id) = raw.get("id").map(request_id) else {
            warn!("client response without an id");
            return;
        };
        let Some(ask) = self.asks.remove(&id) else {
            warn!(%id, "client response to an unknown ask");
            return;
        };
        let mut forwarded = raw.clone();
        forwarded["id"] = serde_json::to_value(&ask.original).unwrap_or(Value::Null);
        if let Err(error) = self.send(ask.child, &forwarded) {
            warn!(%error, %id, "cannot forward a client response");
        }
    }

    fn handle_exit(&mut self, child: ChildId, out: &Sender<Value>) {
        self.retire(child);
        let gone: Vec<(RequestId, Pending)> = self
            .pending
            .extract_if(|_, pending| pending.child() == child)
            .collect();
        for (id, pending) in gone {
            let detail = match pending.session() {
                Some(session) => format!("session {session}: its maki acp process exited"),
                None => "maki acp process exited before answering session/new".to_owned(),
            };
            respond(out, id, Err(AcpError::new(-32603, detail)));
        }
    }

    async fn forward(
        &mut self,
        child: ChildId,
        raw: &Value,
        session: &SessionId,
        id: RequestId,
        out: &Sender<Value>,
    ) {
        self.pending.insert(
            id.clone(),
            Pending::Forwarded {
                child,
                session: session.clone(),
            },
        );
        if let Err(error) = self.send(child, raw) {
            self.pending.remove(&id);
            self.retire(child);
            respond(out, id, Err(error));
        }
    }

    fn warn_on_cwd_change(&self, child: ChildId, session: &SessionId, cwd: &Path) {
        let Some(running) = self.children.get(&child) else {
            return;
        };
        if !same_cwd(&running.cwd, cwd) {
            warn!(
                session = %session,
                child,
                spawn_cwd = %running.cwd.display(),
                load_cwd = %cwd.display(),
                "session/load names a cwd its child was not spawned in"
            );
        }
    }

    fn warn_on_unknown_session(&self, value: &Value) {
        let Some(session) = value.pointer("/params/sessionId").and_then(Value::as_str) else {
            return;
        };
        if !self.sessions.contains_key(&SessionId::new(session)) {
            let method = value
                .get("method")
                .and_then(Value::as_str)
                .unwrap_or_default();
            warn!(
                session = %session,
                %method,
                "forwarding a child notification for a session with no live child"
            );
        }
    }

    fn handshake(&self, child: ChildId) -> Result<(), AcpError> {
        self.send(
            child,
            &serde_json::json!({
                "jsonrpc": "2.0",
                "id": HANDSHAKE_REQUEST_ID,
                "method": "initialize",
                "params": self.client_init.clone(),
            }),
        )
    }

    fn send(&self, child: ChildId, message: &Value) -> Result<(), AcpError> {
        self.children
            .get(&child)
            .ok_or_else(|| AcpError::new(-32603, "maki acp child is gone"))?
            .stdin
            .send(message.to_string())
            .map_err(|_| AcpError::new(-32603, "maki acp child is gone"))
    }

    fn retire(&mut self, child: ChildId) {
        self.sessions.retain(|_, owner| *owner != child);
        self.asks.retain(|_, ask| ask.child != child);
        if let Some(running) = self.children.remove(&child) {
            reap(running);
        }
    }

    async fn shutdown(&mut self) {
        self.sessions.clear();
        let deadline = Instant::now() + CHILD_EXIT_TIMEOUT;
        let mut children = Vec::new();
        for (_, child) in self.children.drain() {
            let RunningChild {
                stdin,
                exited,
                kill,
                ..
            } = child;
            drop(stdin);
            children.push((exited, kill));
        }
        for (exited, kill) in children {
            if !wait_for_exit(exited, deadline).await {
                kill();
            }
        }
    }
}

/// Drops the child's stdin now, so its process sees EOF, then waits and kills
/// on its own task. The router never parks on a slow exit.
fn reap(child: RunningChild) {
    let RunningChild {
        stdin,
        exited,
        kill,
        ..
    } = child;
    drop(stdin);
    tokio::spawn(async move {
        if !wait_for_exit(exited, Instant::now() + CHILD_EXIT_TIMEOUT).await {
            kill();
        }
    });
}

async fn wait_for_exit(exited: Receiver<()>, deadline: Instant) -> bool {
    let left = deadline.saturating_duration_since(Instant::now());
    tokio::select! {
        _ = exited.recv_async() => true,
        _ = tokio::time::sleep(left) => false,
    }
}

/// Case and trailing separators are noise, so only a real difference earns a
/// warning. Canonicalization fails for a path that is gone, which falls back to
/// comparing the spellings the client sent.
fn same_cwd(left: &Path, right: &Path) -> bool {
    match (left.canonicalize(), right.canonicalize()) {
        (Ok(left), Ok(right)) => left == right,
        _ => left == right,
    }
}

async fn write_child(mut stdin: ChildStdin, lines: Receiver<String>) {
    while let Ok(line) = lines.recv_async().await {
        if stdin.write_all(line.as_bytes()).await.is_err() || stdin.write_all(b"\n").await.is_err()
        {
            return;
        }
    }
}

async fn read_child(stdout: ChildStdout, lines: Sender<Value>) {
    let mut reader = BufReader::new(stdout);
    loop {
        let mut line = String::new();
        match reader.read_line(&mut line).await {
            Ok(0) | Err(_) => return,
            Ok(_) => {
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    continue;
                }
                match serde_json::from_str(trimmed) {
                    Ok(value) => {
                        if lines.send(value).is_err() {
                            return;
                        }
                    }
                    Err(e) => debug!(error = %e, "non-JSON line from a child"),
                }
            }
        }
    }
}

async fn watch_child(mut child: ChildGuard, kill: Receiver<()>, exited: Sender<()>) {
    let killed = tokio::select! {
        _ = child.status() => false,
        _ = kill.recv_async() => true,
    };
    if killed {
        child.kill_and_reap().await;
    }
    let _ = exited.send(());
}

fn respond(out: &Sender<Value>, id: RequestId, result: Result<AgentResponse, AcpError>) {
    send(out, &Response::new(id, result));
}

fn send(out: &Sender<Value>, message: &impl serde::Serialize) {
    if let Ok(value) = serde_json::to_value(message) {
        let _ = out.send(value);
    }
}

fn parse_params<T: DeserializeOwned>(raw: &Value) -> Result<T, AcpError> {
    serde_json::from_value(raw.get("params").cloned().unwrap_or(Value::Null))
        .map_err(|e| AcpError::invalid_params().data(Value::String(e.to_string())))
}

fn request_id(value: &Value) -> RequestId {
    serde_json::from_value(value.clone()).unwrap_or(RequestId::Null)
}

fn spawn_error(cwd: &Path, error: &io::Error) -> AcpError {
    warn!(cwd = %cwd.display(), %error, "cannot start a child");
    AcpError::new(
        -32603,
        format!("cannot start maki acp in {}: {error}", cwd.display()),
    )
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicBool, Ordering};

    use serde_json::json;
    use tokio::runtime::{Builder, Runtime};
    use tokio::task::JoinHandle;

    use super::*;

    const TEST_TIMEOUT: Duration = Duration::from_secs(10);
    const SESSION_A: &str = "/a";
    const SESSION_B: &str = "/b";
    const INIT_PROTOCOL: &str = "1";
    const ASK_ID: i64 = 7;
    const ASK_METHOD: &str = "session/request_permission";
    const EXITED_CODE: i32 = -32603;

    struct FakeChild {
        stdin: Receiver<String>,
        lines: Option<Sender<Value>>,
        exited: Option<Sender<()>>,
        killed: Arc<AtomicBool>,
    }

    impl FakeChild {
        fn next_line(&self) -> Value {
            let line = self
                .stdin
                .recv_timeout(TEST_TIMEOUT)
                .expect("the shim writes to the child");
            serde_json::from_str(&line).expect("the shim writes JSON")
        }

        fn reply(&self, value: Value) {
            self.lines
                .as_ref()
                .expect("the child is alive")
                .send(value)
                .expect("the shim still reads");
        }

        /// Drops both signals, like a process leaving: stdout closes and exit
        /// fires.
        fn exit(&mut self) {
            self.exited = None;
            self.lines = None;
        }
    }

    struct Harness {
        in_tx: Sender<Incoming>,
        out_rx: Receiver<Value>,
        spawned: Receiver<(PathBuf, FakeChild)>,
        rt: Runtime,
        task: JoinHandle<()>,
    }

    impl Harness {
        fn new() -> Self {
            let rt = Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .expect("tokio runtime builds");
            let (in_tx, in_rx) = flume::unbounded();
            let (out_tx, out_rx) = flume::unbounded();
            let (spawn_tx, spawned) = flume::unbounded();
            let spawn = Arc::new(move |cwd: &Path| {
                let (stdin_tx, stdin_rx) = flume::unbounded();
                let (lines_tx, lines_rx) = flume::unbounded();
                let (exit_tx, exit_rx) = flume::bounded(1);
                let killed = Arc::new(AtomicBool::new(false));
                let kill_flag = Arc::clone(&killed);
                spawn_tx
                    .send((
                        cwd.to_path_buf(),
                        FakeChild {
                            stdin: stdin_rx,
                            lines: Some(lines_tx),
                            exited: Some(exit_tx),
                            killed,
                        },
                    ))
                    .expect("the test holds the harness");
                Ok(ChildLink {
                    stdin: stdin_tx,
                    lines: lines_rx,
                    exited: exit_rx,
                    kill: Box::new(move || kill_flag.store(true, Ordering::Relaxed)),
                })
            });
            let mut router = Router::new(spawn, in_tx.downgrade());
            let task = rt.spawn(async move { router.run(in_rx, &out_tx).await });
            Self {
                in_tx,
                out_rx,
                spawned,
                rt,
                task,
            }
        }

        fn send(&self, request: Value) {
            self.in_tx
                .send(Incoming::Client(request.to_string()))
                .expect("the router still runs");
        }

        fn spawned(&self) -> (PathBuf, FakeChild) {
            self.spawned
                .recv_timeout(TEST_TIMEOUT)
                .expect("a child is spawned")
        }

        fn answer(&self) -> Value {
            self.out_rx
                .recv_timeout(TEST_TIMEOUT)
                .expect("the client is answered")
        }

        fn finish(self) {
            drop(self.in_tx);
            self.rt.block_on(self.task).expect("router task finishes");
        }
    }

    fn new_request(id: i64, cwd: &str) -> Value {
        json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": "session/new",
            "params": { "cwd": cwd, "mcpServers": [] },
        })
    }

    fn load_request(id: i64, session: &str, cwd: &str) -> Value {
        json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": "session/load",
            "params": { "sessionId": session, "cwd": cwd, "mcpServers": [] },
        })
    }

    fn close_request(id: i64, session: &str) -> Value {
        json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": "session/close",
            "params": { "sessionId": session },
        })
    }

    fn child_ask(id: i64, marker: &str) -> Value {
        json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": ASK_METHOD,
            "params": { "marker": marker },
        })
    }

    fn session_response(id: i64, session: &str) -> Value {
        json!({ "jsonrpc": "2.0", "id": id, "result": { "sessionId": session } })
    }

    fn prompt_request(id: Value, session: &str) -> Value {
        json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": "session/prompt",
            "params": {
                "sessionId": session,
                "prompt": [{ "type": "text", "text": "hi" }],
            },
        })
    }

    fn cancel_notification(session: &str) -> Value {
        json!({
            "jsonrpc": "2.0",
            "method": "session/cancel",
            "params": { "sessionId": session },
        })
    }

    fn session_update(session: &str, text: &str) -> Value {
        json!({
            "jsonrpc": "2.0",
            "method": "session/update",
            "params": {
                "sessionId": session,
                "update": {
                    "sessionUpdate": "agent_message_chunk",
                    "content": { "type": "text", "text": text },
                },
            },
        })
    }

    fn open_session(harness: &Harness, id: i64, cwd: &str, session: &str) -> FakeChild {
        harness.send(new_request(id, cwd));
        let (_, child) = harness.spawned();
        child.next_line();
        child.next_line();
        child.reply(session_response(id, session));
        assert_eq!(harness.answer()["result"]["sessionId"], session);
        child
    }

    #[test]
    fn two_sessions_spawn_one_child_each_in_their_own_cwd() {
        let harness = Harness::new();

        harness.send(new_request(1, SESSION_A));
        let (cwd_a, mut child_a) = harness.spawned();
        assert_eq!(cwd_a, PathBuf::from(SESSION_A));
        assert_eq!(child_a.next_line()["method"], "initialize");
        assert_eq!(child_a.next_line()["method"], "session/new");
        child_a.reply(session_response(1, "a"));
        assert_eq!(harness.answer()["result"]["sessionId"], "a");

        harness.send(new_request(2, SESSION_B));
        let (cwd_b, mut child_b) = harness.spawned();
        assert_eq!(cwd_b, PathBuf::from(SESSION_B));
        assert_eq!(child_b.next_line()["method"], "initialize");
        let forwarded = child_b.next_line();
        assert_eq!(forwarded["method"], "session/new");
        assert_eq!(forwarded["id"], 2);
        child_b.reply(session_response(2, "b"));
        assert_eq!(harness.answer()["result"]["sessionId"], "b");

        child_a.exit();
        child_b.exit();
        harness.finish();
    }

    #[test]
    fn initialize_is_answered_by_the_shim_and_carried_to_each_child() {
        let harness = Harness::new();
        let params = json!({
            "protocolVersion": INIT_PROTOCOL,
            "clientCapabilities": { "fs": { "readTextFile": true } },
        });
        harness.send(json!({
            "jsonrpc": "2.0",
            "id": 0,
            "method": "initialize",
            "params": params.clone(),
        }));

        let answer = harness.answer();
        assert_eq!(answer["id"], 0);
        assert_eq!(answer["result"]["agentInfo"]["name"], "maki");
        assert!(
            answer["result"]["agentCapabilities"]["sessionCapabilities"]["close"].is_object(),
            "the shim advertises what its children do"
        );

        harness.send(new_request(1, SESSION_A));
        let (_, mut child) = harness.spawned();
        let handshake = child.next_line();
        assert_eq!(handshake["method"], "initialize");
        assert_eq!(handshake["id"], HANDSHAKE_REQUEST_ID);
        assert_eq!(handshake["params"], params);
        assert_eq!(child.next_line()["method"], "session/new");

        child.reply(session_response(1, "a"));
        assert_eq!(harness.answer()["result"]["sessionId"], "a");
        child.exit();
        harness.finish();
    }

    #[test]
    fn loading_a_mapped_session_does_not_spawn() {
        let harness = Harness::new();
        harness.send(new_request(1, SESSION_A));
        let (_, mut child) = harness.spawned();
        child.next_line();
        child.next_line();
        child.reply(session_response(1, "a"));
        harness.answer();

        harness.send(load_request(2, "a", SESSION_A));
        let forwarded = child.next_line();
        assert_eq!(forwarded["method"], "session/load");
        assert_eq!(forwarded["id"], 2);
        assert!(
            harness.spawned.is_empty(),
            "no second child for a mapped id"
        );

        child.reply(json!({ "jsonrpc": "2.0", "id": 2, "result": {} }));
        assert_eq!(harness.answer()["id"], 2);
        child.exit();
        harness.finish();
    }

    #[test]
    fn end_of_input_closes_every_child() {
        let harness = Harness::new();
        let mut children = Vec::new();
        for (id, cwd, session) in [(1, SESSION_A, "a"), (2, SESSION_B, "b")] {
            harness.send(new_request(id, cwd));
            let (_, child) = harness.spawned();
            child.next_line();
            child.next_line();
            child.reply(session_response(id, session));
            harness.answer();
            children.push(child);
        }
        for child in &mut children {
            child.exit();
        }

        harness.finish();

        for child in &children {
            assert!(
                child.stdin.recv_timeout(TEST_TIMEOUT).is_err(),
                "the child's stdin closes on EOF"
            );
            assert!(
                !child.killed.load(Ordering::Relaxed),
                "a child that exits on EOF must not be killed"
            );
        }
    }

    #[test]
    fn a_prompt_reaches_only_its_own_child() {
        let harness = Harness::new();
        let mut a = open_session(&harness, 1, SESSION_A, "a");
        let mut b = open_session(&harness, 2, SESSION_B, "b");

        harness.send(prompt_request(json!("p1"), "a"));
        let forwarded = a.next_line();
        assert_eq!(forwarded["method"], "session/prompt");
        assert_eq!(forwarded["id"], "p1");
        assert!(b.stdin.is_empty(), "only the owning child sees the prompt");

        a.reply(json!({
            "jsonrpc": "2.0",
            "id": "p1",
            "result": { "stopReason": "end_turn" },
        }));
        let answer = harness.answer();
        assert_eq!(answer["id"], "p1", "the client id survives the shim");
        assert_eq!(answer["result"]["stopReason"], "end_turn");

        a.exit();
        b.exit();
        harness.finish();
    }

    #[test]
    fn a_cancel_reaches_only_its_own_child() {
        let harness = Harness::new();
        let mut a = open_session(&harness, 1, SESSION_A, "a");
        let mut b = open_session(&harness, 2, SESSION_B, "b");

        harness.send(cancel_notification("b"));
        let forwarded = b.next_line();
        assert_eq!(forwarded["method"], "session/cancel");
        assert!(forwarded.get("id").is_none(), "a notification has no id");
        assert!(a.stdin.is_empty(), "only the owning child sees the cancel");

        a.exit();
        b.exit();
        harness.finish();
    }

    #[test]
    fn a_request_naming_an_unknown_session_is_resource_not_found() {
        let harness = Harness::new();

        harness.send(prompt_request(json!(3), "ghost"));

        let answer = harness.answer();
        assert_eq!(
            answer["error"]["code"],
            i32::from(AcpError::resource_not_found(None).code)
        );
        harness.finish();
    }

    #[test]
    fn a_request_without_a_session_is_method_not_found() {
        let harness = Harness::new();

        harness.send(json!({
            "jsonrpc": "2.0",
            "id": 4,
            "method": "session/list",
            "params": {},
        }));

        let answer = harness.answer();
        assert_eq!(answer["id"], 4);
        assert_eq!(
            answer["error"]["code"],
            i32::from(AcpError::method_not_found().code)
        );
        harness.finish();
    }

    #[test]
    fn reloading_a_live_session_with_another_cwd_still_forwards() {
        let harness = Harness::new();
        let mut child = open_session(&harness, 1, SESSION_A, "a");

        harness.send(load_request(2, "a", SESSION_B));
        let forwarded = child.next_line();
        assert_eq!(forwarded["method"], "session/load");
        assert_eq!(forwarded["params"]["cwd"], SESSION_B);
        assert!(
            harness.spawned.is_empty(),
            "a mapped reload reuses its child"
        );

        child.reply(json!({ "jsonrpc": "2.0", "id": 2, "result": {} }));
        assert_eq!(harness.answer()["id"], 2);
        child.exit();
        harness.finish();
    }

    #[test]
    fn child_notifications_are_forwarded_verbatim() {
        let harness = Harness::new();
        let mut child = open_session(&harness, 1, SESSION_A, "a");

        for update in [session_update("a", "hi"), session_update("unknown", "kept")] {
            child.reply(update.clone());
            assert_eq!(harness.answer(), update, "the notification is unchanged");
        }

        child.exit();
        harness.finish();
    }

    #[test]
    fn child_asks_are_renumbered_and_answered_in_place() {
        let harness = Harness::new();
        let mut a = open_session(&harness, 1, SESSION_A, "a");
        let mut b = open_session(&harness, 2, SESSION_B, "b");

        a.reply(child_ask(ASK_ID, "a"));
        b.reply(child_ask(ASK_ID, "b"));

        let mut shim_ids = HashMap::new();
        for _ in 0..2 {
            let ask = harness.answer();
            assert_eq!(ask["method"], ASK_METHOD);
            let marker = ask["params"]["marker"].as_str().unwrap().to_owned();
            let id = serde_json::from_value::<RequestId>(ask["id"].clone()).unwrap();
            assert!(
                matches!(id, RequestId::Number(n) if n >= FIRST_ASK_REQUEST_ID),
                "a child ask gets a shim id: {ask}"
            );
            shim_ids.insert(marker, id);
        }
        assert_ne!(shim_ids["a"], shim_ids["b"]);

        for (marker, child) in [("a", &mut a), ("b", &mut b)] {
            harness.send(json!({
                "jsonrpc": "2.0",
                "id": serde_json::to_value(&shim_ids[marker]).unwrap(),
                "result": { "marker": marker },
            }));
            let forwarded = child.next_line();
            assert_eq!(forwarded["id"], ASK_ID, "the child id comes back");
            assert_eq!(forwarded["result"]["marker"], marker);
        }

        a.exit();
        b.exit();
        harness.finish();
    }

    #[test]
    fn a_dead_child_errors_its_requests_and_leaves_others_running() {
        let harness = Harness::new();
        let mut a = open_session(&harness, 1, SESSION_A, "a");
        let mut b = open_session(&harness, 2, SESSION_B, "b");

        harness.send(prompt_request(json!("p1"), "a"));
        assert_eq!(a.next_line()["method"], "session/prompt");

        a.exit();
        let gone = harness.answer();
        assert_eq!(gone["id"], "p1");
        assert_eq!(gone["error"]["code"], EXITED_CODE);
        let message = gone["error"]["message"].as_str().unwrap();
        assert!(message.contains("session a"), "{message}");
        assert!(message.contains("exited"), "{message}");

        harness.send(prompt_request(json!("p2"), "b"));
        assert_eq!(b.next_line()["method"], "session/prompt");
        b.reply(json!({
            "jsonrpc": "2.0",
            "id": "p2",
            "result": { "stopReason": "end_turn" },
        }));
        assert_eq!(harness.answer()["id"], "p2");

        b.exit();
        harness.finish();
    }

    #[test]
    fn closing_a_session_reaps_its_child() {
        let harness = Harness::new();
        let mut child = open_session(&harness, 1, SESSION_A, "a");

        harness.send(close_request(5, "a"));
        let forwarded = child.next_line();
        assert_eq!(forwarded["method"], "session/close");
        assert_eq!(forwarded["id"], 5);

        child.reply(json!({ "jsonrpc": "2.0", "id": 5, "result": {} }));
        assert_eq!(harness.answer()["id"], 5);
        child.exit();

        assert!(
            child.stdin.recv_timeout(TEST_TIMEOUT).is_err(),
            "the closed session's child is reaped"
        );
        assert!(!child.killed.load(Ordering::Relaxed));
        harness.finish();
    }

    #[test]
    fn loading_after_a_close_spawns_a_fresh_child() {
        let harness = Harness::new();
        let mut first = open_session(&harness, 1, SESSION_A, "a");

        harness.send(close_request(5, "a"));
        first.next_line();
        first.reply(json!({ "jsonrpc": "2.0", "id": 5, "result": {} }));
        harness.answer();
        first.exit();

        harness.send(load_request(6, "a", SESSION_A));
        let (cwd, mut second) = harness.spawned();
        assert_eq!(cwd, PathBuf::from(SESSION_A));
        assert_eq!(second.next_line()["method"], "initialize");
        let forwarded = second.next_line();
        assert_eq!(forwarded["method"], "session/load");
        assert_eq!(forwarded["id"], 6);
        second.reply(json!({ "jsonrpc": "2.0", "id": 6, "result": {} }));
        assert_eq!(harness.answer()["id"], 6);

        second.exit();
        harness.finish();
    }

    #[test]
    fn a_session_new_error_retires_the_child() {
        let harness = Harness::new();
        harness.send(new_request(1, SESSION_A));
        let (_, mut child) = harness.spawned();
        child.next_line();
        child.next_line();

        child.reply(json!({
            "jsonrpc": "2.0",
            "id": 1,
            "error": { "code": -32000, "message": "trust refused" },
        }));

        let answer = harness.answer();
        assert_eq!(answer["id"], 1);
        assert_eq!(answer["error"]["code"], -32000);
        assert!(
            child.stdin.recv_timeout(TEST_TIMEOUT).is_err(),
            "a child that could not open a session is retired"
        );

        child.exit();
        harness.finish();
    }

    #[test]
    fn closing_an_unknown_session_is_resource_not_found() {
        let harness = Harness::new();

        harness.send(close_request(7, "ghost"));

        let answer = harness.answer();
        assert_eq!(answer["id"], 7);
        assert_eq!(
            answer["error"]["code"],
            i32::from(AcpError::resource_not_found(None).code)
        );
        harness.finish();
    }

    #[test]
    fn child_command_drops_the_multiplex_flag() {
        let command = ChildCommand::new(
            PathBuf::from("maki"),
            ["acp", MULTIPLEX_FLAG, "--yolo"].map(OsString::from),
        );
        assert_eq!(
            command.args,
            [OsString::from("acp"), OsString::from("--yolo")]
        );
    }
}
