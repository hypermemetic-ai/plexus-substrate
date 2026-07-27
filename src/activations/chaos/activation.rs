use super::types::{ChaosError, ListRunningResult, RunningNode, InjectResult, ListProcessesResult, ProcessInfo, KillProcessResult, GraphSnapshotResult, NodeSnapshot};
use crate::activations::lattice::{LatticeStorage, NodeStatus};
use async_stream::stream;
use futures::Stream;
use std::sync::Arc;

/// Chaos activation — fault injection and observability for anti-fragility testing.
///
/// Exposes controlled chaos primitives:
/// - Observe running nodes across all graphs
/// - Inject failures or successes directly into lattice nodes
/// - List and kill system processes (Claude sessions, etc.)
/// - Snapshot graph execution state
/// - Hard-crash the substrate to test recovery
#[derive(Clone)]
pub struct Chaos {
    lattice: Arc<LatticeStorage>,
}

impl Chaos {
    pub const fn new(lattice: Arc<LatticeStorage>) -> Self {
        Self { lattice }
    }
}

/// Parse the outermost serde tag from a JSON spec blob
fn spec_type(spec_json: &str) -> String {
    serde_json::from_str::<serde_json::Value>(spec_json)
        .ok()
        .and_then(|v| v.get("type").and_then(|t| t.as_str()).map(std::string::ToString::to_string))
        .unwrap_or_else(|| "unknown".to_string())
}

/// Find all PIDs whose /proc/<pid>/cmdline contains `pattern`
fn find_pids_by_cmdline(pattern: &str) -> Vec<(u32, String)> {
    let mut results = Vec::new();
    let Ok(entries) = std::fs::read_dir("/proc") else { return results };
    for entry in entries.flatten() {
        let name = entry.file_name();
        let pid_str = name.to_string_lossy();
        let Ok(pid) = pid_str.parse::<u32>() else { continue };
        let cmdline_path = format!("/proc/{pid}/cmdline");
        let Ok(raw) = std::fs::read(&cmdline_path) else { continue };
        // cmdline is NUL-separated
        let cmdline = raw.iter().map(|&b| if b == 0 { b' ' } else { b }).collect::<Vec<_>>();
        let cmdline = String::from_utf8_lossy(&cmdline).to_string();
        if cmdline.contains(pattern) {
            results.push((pid, cmdline.trim().to_string()));
        }
    }
    results
}

#[plexus_macros::activation(namespace = "chaos",
version = "1.0.0",
description = "Fault injection and observability for anti-fragility testing")]
impl Chaos {
    /// List all nodes currently in Running state across every lattice graph.
    #[plexus_macros::method(description = "List all Running nodes across all lattice graphs")]
    async fn list_running_nodes(
        &self,
    ) -> impl Stream<Item = ListRunningResult> + Send + 'static {
        let lattice = self.lattice.clone();
        stream! {
            let graphs = match lattice.list_graphs().await {
                Ok(g) => g,
                Err(e) => { yield ListRunningResult::Err { message: e }; return; }
            };

            let mut count = 0;
            for graph in &graphs {
                let nodes = match lattice.get_nodes(&graph.id).await {
                    Ok(n) => n,
                    Err(_) => continue,
                };
                for node in nodes {
                    if node.status == NodeStatus::Running {
                        count += 1;
                        let st = spec_type(&serde_json::to_string(&node.spec).unwrap_or_default());
                        yield ListRunningResult::Node(RunningNode {
                            graph_id: graph.id.clone(),
                            node_id: node.id.clone(),
                            spec_type: st,
                        });
                    }
                }
            }
            yield ListRunningResult::Done { count };
        }
    }

    /// Force-fail a specific node. Calls `advance_graph` with an error token,
    /// triggering downstream failure propagation and retry logic.
    #[plexus_macros::method(description = "Inject a failure into a running node",
    params(
        graph_id = "Lattice graph ID",
        node_id = "Node to fail",
        error = "Error message to inject (default: 'chaos: injected failure')"
    ))]
    async fn inject_failure(
        &self,
        graph_id: String,
        node_id: String,
        error: Option<String>,
    ) -> Result<InjectResult, ChaosError> {
        let error_msg = error.unwrap_or_else(|| "chaos: injected failure".to_string());

        // Verify node is Running before injecting
        let node = self.lattice.get_node(&node_id).await?;
        if node.status != NodeStatus::Running {
            return Ok(InjectResult::Skipped {
                reason: format!("node is {:?}, not Running", node.status),
            });
        }

        self.lattice
            .advance_graph(&graph_id, &node_id, None, Some(error_msg.clone()))
            .await?;
        Ok(InjectResult::Ok {
            graph_id,
            node_id,
            action: format!("failed: {error_msg}"),
        })
    }

    /// Force-complete a specific node with an ok token.
    /// Useful for unblocking stuck nodes or skipping tasks in a test graph.
    #[plexus_macros::method(description = "Inject a success into a running node",
    params(
        graph_id = "Lattice graph ID",
        node_id = "Node to complete",
        value = "JSON value to use as the output token (default: null)"
    ))]
    async fn inject_success(
        &self,
        graph_id: String,
        node_id: String,
        value: Option<String>,
    ) -> Result<InjectResult, ChaosError> {
        use crate::activations::lattice::{NodeOutput, Token, TokenColor, TokenPayload};

        let node = self.lattice.get_node(&node_id).await?;
        if node.status != NodeStatus::Running {
            return Ok(InjectResult::Skipped {
                reason: format!("node is {:?}, not Running", node.status),
            });
        }

        let payload_value = value
            .as_deref()
            .and_then(|s| serde_json::from_str(s).ok())
            .unwrap_or(serde_json::Value::Null);

        let output = NodeOutput::Single(Token {
            color: TokenColor::Ok,
            payload: Some(TokenPayload::Data { value: payload_value }),
        });

        self.lattice
            .advance_graph(&graph_id, &node_id, Some(output), None)
            .await?;
        Ok(InjectResult::Ok {
            graph_id,
            node_id,
            action: "succeeded".to_string(),
        })
    }

    /// List system processes whose cmdline contains the given pattern.
    #[plexus_macros::method(description = "List processes matching a cmdline pattern",
    params(pattern = "Substring to search for in /proc/*/cmdline"))]
    async fn list_processes(
        &self,
        pattern: String,
    ) -> impl Stream<Item = ListProcessesResult> + Send + 'static {
        stream! {
            let procs = find_pids_by_cmdline(&pattern);
            let count = procs.len();
            for (pid, cmdline) in procs {
                yield ListProcessesResult::Process(ProcessInfo { pid, cmdline });
            }
            yield ListProcessesResult::Done { count };
        }
    }

    /// Send SIGKILL to a process by PID.
    #[plexus_macros::method(description = "Kill a process by PID (SIGKILL)",
    params(pid = "Process ID to kill"))]
    async fn kill_process(
        &self,
        pid: u32,
    ) -> Result<KillProcessResult, ChaosError> {
        // Verify process exists first
        let cmdline_path = format!("/proc/{pid}/cmdline");
        if !std::path::Path::new(&cmdline_path).exists() {
            return Ok(KillProcessResult::NotFound);
        }

        // SAFETY: libc::kill is a POSIX syscall with no safe wrapper in std.
        // pid fits in i32 (process IDs on supported platforms are <= i32::MAX);
        // SIGKILL is a const defined by libc. Neither dereferences user memory.
        #[allow(unsafe_code)]
        let result = unsafe { libc::kill(pid as i32, libc::SIGKILL) };
        if result == 0 {
            return Ok(KillProcessResult::Killed { pid });
        }
        let errno = std::io::Error::last_os_error();
        if errno.raw_os_error() == Some(libc::ESRCH) {
            Ok(KillProcessResult::NotFound)
        } else {
            Err(ChaosError::KillFailed(errno.to_string()))
        }
    }

    /// Snapshot all nodes in a graph with their current statuses.
    #[plexus_macros::method(description = "Get a full status snapshot of a lattice graph",
    params(graph_id = "Lattice graph ID"))]
    async fn graph_snapshot(
        &self,
        graph_id: String,
    ) -> impl Stream<Item = GraphSnapshotResult> + Send + 'static {
        let lattice = self.lattice.clone();
        stream! {
            let graph = match lattice.get_graph(&graph_id).await {
                Ok(g) => g,
                Err(e) => { yield GraphSnapshotResult::Err { message: e }; return; }
            };
            let nodes = match lattice.get_nodes(&graph_id).await {
                Ok(n) => n,
                Err(e) => { yield GraphSnapshotResult::Err { message: e }; return; }
            };

            let mut pending = 0usize;
            let mut ready = 0usize;
            let mut running = 0usize;
            let mut complete = 0usize;
            let mut failed = 0usize;

            for node in &nodes {
                match node.status {
                    NodeStatus::Pending  => pending  += 1,
                    NodeStatus::Ready    => ready    += 1,
                    NodeStatus::Running  => running  += 1,
                    NodeStatus::Complete => complete += 1,
                    NodeStatus::Failed   => failed   += 1,
                }
                let st = spec_type(&serde_json::to_string(&node.spec).unwrap_or_default());
                yield GraphSnapshotResult::Node(NodeSnapshot {
                    node_id: node.id.clone(),
                    status: format!("{:?}", node.status).to_lowercase(),
                    spec_type: st,
                    error: node.error.clone(),
                });
            }

            yield GraphSnapshotResult::Summary {
                graph_id,
                graph_status: format!("{}", graph.status),
                total: nodes.len(),
                pending, ready, running, complete, failed,
            };
        }
    }

    /// Hard-crash the substrate process (SIGKILL self).
    /// Used to test crash recovery — the substrate will not respond after this call.
    /// Restart with `make restart` and observe `recovery: re-dispatching` in the logs.
    ///
    /// PLX-118: this yields exactly once and would otherwise be on the unary
    /// conversion queue, but it is deliberately NOT converted. The generated
    /// handler emits a stream item as soon as it is yielded, whereas a unary
    /// `Result` is only serialized after the handler future **returns** — and
    /// this handler never returns, because it SIGKILLs its own process. The
    /// yield-then-sleep-then-kill ordering is load-bearing: as a `Result` the
    /// caller would receive nothing at all. It keeps `impl Stream`.
    #[plexus_macros::method(description = "Hard-crash the substrate (SIGKILL self) — use to test crash recovery")]
    async fn crash(&self) -> impl Stream<Item = InjectResult> + Send + 'static {
        stream! {
            tracing::warn!("chaos: crash() called — killing substrate process");
            yield InjectResult::Ok {
                graph_id: "self".to_string(),
                node_id: "self".to_string(),
                action: "crashing".to_string(),
            };
            // Small delay so the response can be flushed
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            // SAFETY: libc::kill is a POSIX syscall with no safe wrapper in std.
            // std::process::id() returns a valid pid; SIGKILL is a const defined
            // by libc. Neither dereferences user memory.
            #[allow(unsafe_code)]
            unsafe {
                libc::kill(std::process::id() as i32, libc::SIGKILL)
            };
        }
    }
}
