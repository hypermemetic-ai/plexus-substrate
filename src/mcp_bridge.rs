//! MCP server bridge using rmcp with Plexus RPC backend
//!
//! This module implements the MCP protocol using the rmcp crate,
//! bridging MCP tool calls to Plexus RPC activation methods.

use std::sync::Arc;

use futures::StreamExt;
use rmcp::{
    ErrorData as McpError,
    ServerHandler,
    model::{Tool, ServerInfo, ProtocolVersion, ServerCapabilities, Implementation, PaginatedRequestParam, ListToolsResult, CallToolRequestParam, CallToolResult, ProgressNotificationParam, LoggingMessageNotificationParam, LoggingLevel, Content},
    service::{RequestContext, RoleServer},
};
use serde_json::json;

use crate::plexus::{DynamicHub, PlexusError, PluginSchema};
use crate::plexus::types::PlexusStreamItem;

// =============================================================================
// Schema Transformation
// =============================================================================

/// Convert Plexus RPC activation schemas to rmcp Tool format
///
/// MCP requires all tool inputSchema to have "type": "object" at root.
/// schemars may produce schemas without this (e.g., for unit types).
fn schemas_to_rmcp_tools(schemas: Vec<PluginSchema>) -> Vec<Tool> {
    schemas
        .into_iter()
        .flat_map(|activation| {
            let namespace = activation.namespace.clone();
            activation.methods.into_iter().map(move |method| {
                let name = format!("{}.{}", namespace, method.name);
                let description = method.description.clone();

                // Convert schemars::Schema to JSON, ensure "type": "object" exists
                let input_schema = method
                    .params
                    .and_then(|s| serde_json::to_value(s).ok())
                    .and_then(|v| v.as_object().cloned())
                    .map_or_else(
                        || {
                            // Empty params = empty object schema
                            Arc::new(serde_json::Map::from_iter([(
                                "type".to_string(),
                                json!("object"),
                            )]))
                        },
                        |mut obj| {
                            // MCP requires "type": "object" at schema root
                            if !obj.contains_key("type") {
                                obj.insert("type".to_string(), json!("object"));
                            }
                            Arc::new(obj)
                        },
                    );

                Tool::new(name, description, input_schema)
            })
        })
        .collect()
}

// =============================================================================
// Error Mapping
// =============================================================================

/// Convert `PlexusError` to `McpError`
fn plexus_to_mcp_error(e: PlexusError) -> McpError {
    match e {
        PlexusError::ActivationNotFound(name) => {
            McpError::invalid_params(format!("Unknown activation: {name}"), None)
        }
        PlexusError::MethodNotFound { activation, method } => {
            McpError::invalid_params(format!("Unknown method: {activation}.{method}"), None)
        }
        PlexusError::InvalidParams(reason) => McpError::invalid_params(reason, None),
        PlexusError::ExecutionError(error) => McpError::internal_error(error, None),
        PlexusError::HandleNotSupported(activation) => {
            McpError::invalid_params(format!("Handle resolution not supported: {activation}"), None)
        }
        PlexusError::TransportError(kind) => {
            McpError::internal_error(format!("Transport error: {kind:?}"), None)
        }
        PlexusError::Unauthenticated(reason) => {
            McpError::invalid_params(format!("Unauthenticated: {reason}"), None)
        }
        // core 0.6 (AUTHZ): authenticated-but-not-authorized. PlexusError's
        // Display renders the layered reason ("Forbidden: missing required
        // scope '…'" / "tenant boundary" / "call not accepted").
        forbidden @ PlexusError::Forbidden { .. } => {
            McpError::invalid_params(forbidden.to_string(), None)
        }
    }
}

// =============================================================================
// Plexus RPC MCP Bridge
// =============================================================================

/// MCP handler that bridges to Plexus RPC server
#[derive(Clone)]
pub struct PlexusMcpBridge {
    hub: Arc<DynamicHub>,
}

impl PlexusMcpBridge {
    pub const fn new(hub: Arc<DynamicHub>) -> Self {
        Self { hub }
    }
}

impl ServerHandler for PlexusMcpBridge {
    fn get_info(&self) -> ServerInfo {
        ServerInfo {
            protocol_version: ProtocolVersion::LATEST,
            capabilities: ServerCapabilities::builder()
                .enable_tools()
                .enable_logging()
                .build(),
            server_info: Implementation::from_build_env(),
            instructions: Some(
                "Plexus MCP server - provides access to all registered activations.".into(),
            ),
        }
    }

    async fn list_tools(
        &self,
        _request: Option<PaginatedRequestParam>,
        _ctx: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, McpError> {
        let schemas = self.hub.list_plugin_schemas();
        let tools = schemas_to_rmcp_tools(schemas);

        tracing::debug!("Listing {} tools", tools.len());

        Ok(ListToolsResult {
            tools,
            next_cursor: None,
            meta: None,
        })
    }

    async fn call_tool(
        &self,
        request: CallToolRequestParam,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, McpError> {
        let method_name = &request.name;
        let arguments = request
            .arguments
            .map(serde_json::Value::Object)
            .unwrap_or(json!({}));

        tracing::debug!("Calling tool: {} with args: {:?}", method_name, arguments);

        // Get progress token if provided
        let progress_token = ctx.meta.get_progress_token();

        // Logger name: plexus.namespace.method (e.g., plexus.bash.execute)
        let logger = format!("plexus.{method_name}");

        // Call Plexus RPC hub and get stream
        let stream = self
            .hub
            .route(method_name, arguments, None)
            .await
            .map_err(plexus_to_mcp_error)?;

        // Stream events via notifications AND buffer for final result
        let mut had_error = false;
        let mut buffered_data: Vec<serde_json::Value> = Vec::new();
        let mut error_messages: Vec<String> = Vec::new();

        tokio::pin!(stream);
        while let Some(item) = stream.next().await {
            // Check cancellation on each iteration
            if ctx.ct.is_cancelled() {
                return Err(McpError::internal_error("Cancelled", None));
            }

            match &item {
                PlexusStreamItem::Progress {
                    message,
                    percentage,
                    ..
                } => {
                    // Only send progress if client provided token
                    if let Some(ref token) = progress_token {
                        let _ = ctx
                            .peer
                            .notify_progress(ProgressNotificationParam {
                                progress_token: token.clone(),
                                progress: f64::from(percentage.unwrap_or(0.0)),
                                total: None,
                                message: Some(message.clone()),
                            })
                            .await;
                    }
                }

                PlexusStreamItem::Data {
                    content, content_type, ..
                } => {
                    // Buffer data for final result
                    buffered_data.push(content.clone());

                    // Also stream via notifications for real-time consumers
                    let _ = ctx
                        .peer
                        .notify_logging_message(LoggingMessageNotificationParam {
                            level: LoggingLevel::Info,
                            logger: Some(logger.clone()),
                            data: json!({
                                "type": "data",
                                "content_type": content_type,
                                "data": content,
                            }),
                        })
                        .await;
                }

                PlexusStreamItem::Error {
                    message, recoverable, ..
                } => {
                    // Buffer errors for final result
                    error_messages.push(message.clone());

                    let _ = ctx
                        .peer
                        .notify_logging_message(LoggingMessageNotificationParam {
                            level: LoggingLevel::Error,
                            logger: Some(logger.clone()),
                            data: json!({
                                "type": "error",
                                "error": message,
                                "recoverable": recoverable,
                            }),
                        })
                        .await;

                    if !recoverable {
                        had_error = true;
                    }
                }

                PlexusStreamItem::Done { .. } => {
                    break;
                }

                PlexusStreamItem::Request {
                    request_id,
                    request_data,
                    timeout_ms,
                } => {
                    // Send bidirectional request to client via logging notification
                    // Client should respond via _plexus_respond tool
                    let _ = ctx
                        .peer
                        .notify_logging_message(LoggingMessageNotificationParam {
                            level: LoggingLevel::Info,
                            logger: Some(logger.clone()),
                            data: json!({
                                "type": "request",
                                "request_id": request_id,
                                "request_data": request_data,
                                "timeout_ms": timeout_ms,
                            }),
                        })
                        .await;
                }
            }
        }

        // Return buffered data in the final result
        if had_error {
            let error_content = if error_messages.is_empty() {
                "Stream completed with errors".to_string()
            } else {
                error_messages.join("\n")
            };
            Ok(CallToolResult::error(vec![Content::text(error_content)]))
        } else {
            // PLX-105: there were two byte-identical copies of the
            // response-text rule — this one and `plexus-transport`'s
            // `mcp/bridge.rs`. Both decide what a spawned Claude CLI reads back
            // from `--permission-prompt-tool`, and neither was covered by a
            // test. The rule now lives once, in
            // `plexus_transport::mcp::bridge::render_tool_text`, where
            // `tests/mcp_tool_text.rs` pins it and
            // `tests/plx105_permission_wire.rs` drives the loopback through it
            // end to end. Behaviour is unchanged.
            Ok(CallToolResult::success(vec![Content::text(
                plexus_transport::mcp::bridge::render_tool_text(&buffered_data),
            )]))
        }
    }
}
