//! Exercise hosted session opens through the production transport and flow.
//! Only the shell's port is scripted; paths use a real temporary filesystem.
#![cfg(unix)]

mod a20_common;

use std::path::PathBuf;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    mpsc,
};
use std::time::Duration;

use octos_app_transport::host::{HostPort, HostPortEvent};
use octoscode_module::flow::{Conversation, FlowEvent};
use serde_json::{json, Value};

struct Workspace(PathBuf);

impl Workspace {
    fn new() -> Self {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let path = std::env::temp_dir().join(format!(
            "octoscode-host-workspace-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&path).unwrap();
        std::fs::create_dir(path.join("canonical")).unwrap();
        std::fs::create_dir(path.join("other")).unwrap();
        std::os::unix::fs::symlink(path.join("canonical"), path.join("alias")).unwrap();
        Self(path)
    }

    fn path(&self, name: &str) -> String {
        self.0.join(name).to_str().unwrap().to_owned()
    }
}

impl Drop for Workspace {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}

struct ClosePort(mpsc::Sender<HostPortEvent>);

impl Drop for ClosePort {
    fn drop(&mut self) {
        let _ = self.0.send(HostPortEvent::Closed("test finished".into()));
    }
}

async fn open(requested: String, returned: String) -> (bool, Option<String>) {
    let (tx, rx) = mpsc::channel();
    let _close = ClosePort(tx.clone());
    let root = returned.clone();
    let port = HostPort {
        send: Box::new(move |text| {
            let request: Value = serde_json::from_str(&text).unwrap();
            let session = &request["params"]["session_id"];
            let result = match request["method"].as_str().unwrap() {
                "session/open" => json!({"opened": {
                    "session_id": session, "active_profile_id": "_main",
                    "workspace_root": root,
                    "cursor": {"stream": session, "seq": 1},
                    "capabilities": {
                        "version": {"protocol": "octos-ui/v1alpha1", "schema_version": 1, "jsonrpc": "2.0"},
                        "capabilities_schema_version": 2,
                        "supported_methods": ["session/open", "session/hydrate"],
                        "supported_notifications": [],
                        "supported_features": ["state.session_hydrate.v1"]
                    }
                }}),
                "session/hydrate" => {
                    json!({"session_id": session, "cursor": {"stream": session, "seq": 2}, "messages": []})
                }
                _ => json!({}),
            };
            tx.send(HostPortEvent::Frame(
                json!({
                    "jsonrpc": "2.0", "id": request["id"], "result": result
                })
                .to_string(),
            ))
            .map_err(|e| e.to_string())
        }),
        recv: Box::new(move || {
            rx.recv()
                .unwrap_or(HostPortEvent::Closed("test ended".into()))
        }),
    };
    let (conv, mut events) = Conversation::connect_host(port, "_main", None, None).unwrap();
    let session = conv.session_id();
    conv.open_workspace(Some(requested)).await.unwrap();
    let accepted = tokio::time::timeout(Duration::from_secs(5), async {
        while let Some(event) = events.recv().await {
            match conv.on_event(event) {
                FlowEvent::WorkspaceOpened(_) => return true,
                FlowEvent::Other(reason) if reason == "session/open-workspace-mismatch" => {
                    return false
                }
                _ => {}
            }
        }
        panic!("host port closed before session/open settled");
    })
    .await
    .expect("session/open reply");
    (
        accepted,
        conv.store.domains.session.workspace_root(&session),
    )
}

#[tokio::test]
async fn hosted_open_accepts_a_symlink_to_the_returned_workspace() {
    let workspace = Workspace::new();
    let canonical = std::fs::canonicalize(workspace.path("canonical"))
        .unwrap()
        .to_str()
        .unwrap()
        .to_owned();
    let (accepted, root) = open(workspace.path("alias"), canonical.clone()).await;
    assert!(
        accepted,
        "the shell canonicalized the requested workspace, not another directory"
    );
    assert_eq!(root, Some(canonical));
}

#[tokio::test]
async fn hosted_open_still_rejects_a_different_directory() {
    let workspace = Workspace::new();
    let (accepted, root) = open(workspace.path("alias"), workspace.path("other")).await;
    assert!(!accepted);
    assert_eq!(root, None);
}

#[tokio::test]
async fn hosted_open_does_not_equate_two_unresolvable_paths() {
    let workspace = Workspace::new();
    let (accepted, root) = open(workspace.path("missing-one"), workspace.path("missing-two")).await;
    assert!(!accepted);
    assert_eq!(root, None);
}

#[tokio::test]
async fn remote_open_does_not_use_the_clients_symlinks_as_server_identity() {
    let workspace = Workspace::new();
    let canonical = std::fs::canonicalize(workspace.path("canonical"))
        .unwrap()
        .to_str()
        .unwrap()
        .to_owned();
    let server = a20_common::Server::start(a20_common::Script {
        open_root: Some((a20_common::MAIN.to_owned(), canonical)),
        ..Default::default()
    })
    .await;
    let (conv, mut events) =
        Conversation::connect(&server.base, "dummy", a20_common::PROFILE, None, None).unwrap();
    conv.open_workspace_as(a20_common::MAIN, Some(workspace.path("alias")))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while let Some(event) = events.recv().await {
            match conv.on_event(event) {
                FlowEvent::Other(reason) if reason == "session/open-workspace-mismatch" => return,
                FlowEvent::WorkspaceOpened(_) => {
                    panic!("local path equivalence must not authorize a remote workspace")
                }
                _ => {}
            }
        }
        panic!("no workspace refusal");
    })
    .await
    .unwrap();
    assert_eq!(
        conv.store.domains.session.workspace_root(a20_common::MAIN),
        None
    );
}
