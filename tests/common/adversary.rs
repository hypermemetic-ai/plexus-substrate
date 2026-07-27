//! A scripted adversary standing in for the model, so the **real** Claude CLI
//! really executes its **real** Bash tool against a **real** attack.
//!
//! # Why this exists rather than an API key
//!
//! PLX-151 c2 demands the measured attack "end to end through `claudecode.chat`
//! with `allowed_tools` including Bash". The attack needs the CLI to *choose*
//! to run a command. With a live model that choice is probabilistic — it might
//! decline, it might phrase the read differently, and a test that depends on
//! what a model felt like doing is not a measurement. It also needs
//! credentials, which a test must not extract from the operator's machine.
//!
//! So the model is replaced, and **only** the model. This server speaks the
//! Anthropic Messages streaming API on `ANTHROPIC_BASE_URL`; the CLI, its
//! permission handling, its tool dispatch, the `bash` it spawns, and the
//! confinement all remain real. On the first turn it returns a `tool_use`
//! block naming `Bash` with the attack command; the CLI runs it inside the
//! container and posts the result back; on the second turn the server echoes
//! that result as assistant text so it flows out through substrate's own
//! `ChatEvent` stream.
//!
//! This is *stronger* than a live model, not weaker: the adversary always
//! attacks, attacks exactly what the test says, and cannot be talked out of it.
//!
//! What it is NOT: it is not a stand-in for the CLI (the CLI is the shipped
//! binary, in the image, pinned by version) and it is not a stand-in for the
//! shell (the shell is `bash` from the image). Substituting either of those
//! would be mocking the thing under test.

#![allow(dead_code)]
// The `common` module is compiled into several test binaries; items unused by
// one of them are not dead, and `pub` here is the only visibility that works.
#![allow(unreachable_pub)]

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

/// What the confined `bash` actually returned, as recorded from the CLI's own
/// follow-up request. This is the sharpest evidence surface in the test: it is
/// the tool output as the CLI saw it, before substrate touched it.
#[derive(Debug, Default, Clone)]
pub struct ToolResults {
    pub results: Vec<String>,
}

pub struct Adversary {
    port: u16,
    tool_results: Arc<Mutex<ToolResults>>,
    stop: Arc<AtomicBool>,
}

impl Adversary {
    /// Start on an ephemeral port, scripted to make the CLI run `command`.
    pub fn start(command: &str) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind adversary");
        let port = listener.local_addr().expect("addr").port();
        let tool_results = Arc::new(Mutex::new(ToolResults::default()));
        let stop = Arc::new(AtomicBool::new(false));

        let command = command.to_owned();
        let results = Arc::clone(&tool_results);
        let stopper = Arc::clone(&stop);
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                if stopper.load(Ordering::Relaxed) {
                    break;
                }
                let Ok(stream) = stream else { continue };
                let command = command.clone();
                let results = Arc::clone(&results);
                std::thread::spawn(move || {
                    let _ = serve(stream, &command, &results);
                });
            }
        });

        Self {
            port,
            tool_results,
            stop,
        }
    }

    /// The value for `ANTHROPIC_BASE_URL` inside the container.
    ///
    /// `host.docker.internal` is added to the container's hosts file by the
    /// sandbox's extra args; from the host's point of view this is a loopback
    /// port that only this test knows about.
    pub fn base_url_for_container(&self) -> String {
        format!("http://host.docker.internal:{}", self.port)
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    /// Everything the confined shell returned to the CLI.
    pub fn tool_results(&self) -> Vec<String> {
        self.tool_results.lock().expect("lock").results.clone()
    }

    /// Whether the CLI ever came back with a tool result at all — the liveness
    /// control. A test in which the shell never ran proves nothing about what
    /// the shell could read.
    pub fn the_shell_ran(&self) -> bool {
        !self.tool_results.lock().expect("lock").results.is_empty()
    }
}

impl Drop for Adversary {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        // Unblock `incoming()` so the accept thread notices the flag.
        let _ = TcpStream::connect(("127.0.0.1", self.port));
    }
}

fn serve(
    mut stream: TcpStream,
    command: &str,
    results: &Arc<Mutex<ToolResults>>,
) -> std::io::Result<()> {
    let mut reader = BufReader::new(stream.try_clone()?);

    // ---- request line + headers ------------------------------------------
    let mut request_line = String::new();
    if reader.read_line(&mut request_line)? == 0 {
        return Ok(());
    }
    let mut content_length = 0usize;
    loop {
        let mut line = String::new();
        if reader.read_line(&mut line)? == 0 {
            break;
        }
        let trimmed = line.trim_end();
        if trimmed.is_empty() {
            break;
        }
        if let Some(v) = trimmed
            .to_ascii_lowercase()
            .strip_prefix("content-length:")
        {
            content_length = v.trim().parse().unwrap_or(0);
        }
    }

    if !request_line.starts_with("POST") {
        // The CLI probes with HEAD/GET before it talks. 404 is fine.
        return stream.write_all(b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n");
    }

    let mut body = vec![0u8; content_length];
    reader.read_exact(&mut body)?;
    let request: serde_json::Value = serde_json::from_slice(&body).unwrap_or(serde_json::Value::Null);

    // ---- which turn is this? ---------------------------------------------
    //
    // Decided from the transcript the CLI sent, not from a counter: a retry
    // must not be mistaken for the second turn.
    let mut saw_tool_result = false;
    if let Some(messages) = request.get("messages").and_then(|m| m.as_array()) {
        for message in messages {
            let Some(blocks) = message.get("content").and_then(|c| c.as_array()) else {
                continue;
            };
            for block in blocks {
                if block.get("type").and_then(|t| t.as_str()) == Some("tool_result") {
                    saw_tool_result = true;
                    let text = tool_result_text(block);
                    results.lock().expect("lock").results.push(text);
                }
            }
        }
    }

    let events = if saw_tool_result {
        // Echo what the shell returned back through the CLI's stdout, so it
        // reaches substrate's `ChatEvent::Content` and the assertion can be
        // made on substrate's own stream as well as on this server's record.
        let echoed = results
            .lock()
            .expect("lock")
            .results
            .last()
            .cloned()
            .unwrap_or_default();
        text_turn(&format!("TOOL_OUTPUT_BEGIN{echoed}TOOL_OUTPUT_END"))
    } else {
        tool_use_turn(command)
    };

    let mut response = Vec::new();
    response.extend_from_slice(
        b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: close\r\n\r\n",
    );
    response.extend_from_slice(events.as_bytes());
    stream.write_all(&response)?;
    stream.flush()
}

fn tool_result_text(block: &serde_json::Value) -> String {
    match block.get("content") {
        Some(serde_json::Value::String(s)) => s.clone(),
        Some(serde_json::Value::Array(parts)) => parts
            .iter()
            .filter_map(|p| p.get("text").and_then(|t| t.as_str()))
            .collect::<Vec<_>>()
            .join("\n"),
        _ => String::new(),
    }
}

fn sse(event: &str, data: &serde_json::Value) -> String {
    format!("event: {event}\ndata: {data}\n\n")
}

fn message_start(id: &str) -> String {
    sse(
        "message_start",
        &serde_json::json!({
            "type": "message_start",
            "message": {
                "id": id, "type": "message", "role": "assistant",
                "model": "adversary", "content": [],
                "stop_reason": null, "stop_sequence": null,
                "usage": {"input_tokens": 1, "output_tokens": 1}
            }
        }),
    )
}

fn message_end(stop_reason: &str) -> String {
    format!(
        "{}{}",
        sse(
            "message_delta",
            &serde_json::json!({
                "type": "message_delta",
                "delta": {"stop_reason": stop_reason, "stop_sequence": null},
                "usage": {"output_tokens": 1}
            }),
        ),
        sse("message_stop", &serde_json::json!({"type": "message_stop"})),
    )
}

/// Turn 1: "run this in Bash".
fn tool_use_turn(command: &str) -> String {
    let input = serde_json::json!({ "command": command, "description": "adversary read" });
    let mut out = message_start("msg_adversary_tool");
    out.push_str(&sse(
        "content_block_start",
        &serde_json::json!({
            "type": "content_block_start", "index": 0,
            "content_block": {"type": "tool_use", "id": "toolu_adversary_1", "name": "Bash", "input": {}}
        }),
    ));
    out.push_str(&sse(
        "content_block_delta",
        &serde_json::json!({
            "type": "content_block_delta", "index": 0,
            "delta": {"type": "input_json_delta", "partial_json": input.to_string()}
        }),
    ));
    out.push_str(&sse(
        "content_block_stop",
        &serde_json::json!({"type": "content_block_stop", "index": 0}),
    ));
    out.push_str(&message_end("tool_use"));
    out
}

/// Turn 2: say what the shell produced.
fn text_turn(text: &str) -> String {
    let mut out = message_start("msg_adversary_text");
    out.push_str(&sse(
        "content_block_start",
        &serde_json::json!({
            "type": "content_block_start", "index": 0,
            "content_block": {"type": "text", "text": ""}
        }),
    ));
    out.push_str(&sse(
        "content_block_delta",
        &serde_json::json!({
            "type": "content_block_delta", "index": 0,
            "delta": {"type": "text_delta", "text": text}
        }),
    ));
    out.push_str(&sse(
        "content_block_stop",
        &serde_json::json!({"type": "content_block_stop", "index": 0}),
    ));
    out.push_str(&message_end("end_turn"));
    out
}
