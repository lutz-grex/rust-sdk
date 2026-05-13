//! Reproduces issue #754: StreamableHttpClientTransport hangs on call_tool when
//! the server uses json_response + stateless mode and the tool is registered
//! via `ToolRoute::new_dyn`.
#![cfg(all(feature = "client", not(feature = "local")))]

use std::{sync::Arc, time::Duration};

use rmcp::{
    RoleServer, ServerHandler, ServiceExt,
    handler::server::{
        router::tool::{ToolRoute, ToolRouter},
        tool::ToolCallContext,
    },
    model::{
        CallToolRequestParams, CallToolResult, ClientInfo, Content, NumberOrString,
        ProgressNotificationParam, ProgressToken, ServerCapabilities, ServerInfo, Tool,
    },
    service::MaybeSendFuture,
    transport::{
        StreamableHttpClientTransport,
        streamable_http_client::StreamableHttpClientTransportConfig,
        streamable_http_server::{
            StreamableHttpServerConfig, StreamableHttpService, session::local::LocalSessionManager,
        },
    },
};
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
struct DynToolServer {
    router: Arc<ToolRouter<Self>>,
}

impl DynToolServer {
    fn new() -> Self {
        let mut tool_router = ToolRouter::<Self>::new();
        tool_router.add_route(ToolRoute::new_dyn(
            Tool::new(
                "progress_echo",
                "Emits a progress notification, then returns a fixed string",
                Arc::new(Default::default()),
            ),
            |ctx| {
                Box::pin(async move {
                    let _ = ctx
                        .request_context
                        .peer
                        .notify_progress(ProgressNotificationParam {
                            progress_token: ProgressToken(NumberOrString::Number(1)),
                            progress: 1.0,
                            total: Some(1.0),
                            message: Some("working".to_string()),
                        })
                        .await;
                    Ok(CallToolResult::success(vec![Content::text("hello")]))
                })
            },
        ));
        Self {
            router: Arc::new(tool_router),
        }
    }
}

impl ServerHandler for DynToolServer {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
    }

    fn call_tool(
        &self,
        request: CallToolRequestParams,
        context: rmcp::service::RequestContext<RoleServer>,
    ) -> impl std::future::Future<Output = Result<CallToolResult, rmcp::ErrorData>> + MaybeSendFuture + '_
    {
        async move {
            let tcc = ToolCallContext::new(self, request, context);
            self.router.call(tcc).await
        }
    }

    fn list_tools(
        &self,
        _request: Option<rmcp::model::PaginatedRequestParams>,
        _context: rmcp::service::RequestContext<RoleServer>,
    ) -> impl std::future::Future<Output = Result<rmcp::model::ListToolsResult, rmcp::ErrorData>>
    + MaybeSendFuture
    + '_ {
        let tools = self.router.list_all();
        async move {
            Ok(rmcp::model::ListToolsResult {
                tools,
                ..Default::default()
            })
        }
    }
}

/// Issue #754: a dyn-routed tool, called via the streamable HTTP client
/// against a stateless + json_response server, must return within a reasonable
/// timeout (currently it hangs forever).
#[tokio::test]
async fn dyn_tool_call_returns_under_stateless_json_response() -> anyhow::Result<()> {
    let ct = CancellationToken::new();

    let service: StreamableHttpService<DynToolServer, LocalSessionManager> =
        StreamableHttpService::new(
            || Ok(DynToolServer::new()),
            Default::default(),
            StreamableHttpServerConfig::default()
                .with_stateful_mode(false)
                .with_json_response(true)
                .with_sse_keep_alive(None)
                .with_cancellation_token(ct.child_token()),
        );

    let router = axum::Router::new().nest_service("/mcp", service);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;

    let server_handle = tokio::spawn({
        let ct = ct.clone();
        async move {
            let _ = axum::serve(listener, router)
                .with_graceful_shutdown(async move { ct.cancelled_owned().await })
                .await;
        }
    });

    let transport = StreamableHttpClientTransport::from_config(
        StreamableHttpClientTransportConfig::with_uri(format!("http://{addr}/mcp")),
    );
    let client = ClientInfo::default().serve(transport).await?;

    let call = client.call_tool(CallToolRequestParams::new("progress_echo"));
    let result = tokio::time::timeout(Duration::from_secs(5), call)
        .await
        .map_err(|_| {
            anyhow::anyhow!(
                "call_tool on a dyn-routed tool hung past 5s under stateless + json_response (issue #754)"
            )
        })??;

    assert_ne!(result.is_error, Some(true));
    let text = result
        .content
        .first()
        .and_then(|c| c.as_text())
        .map(|t| t.text.as_str())
        .unwrap_or_default();
    assert_eq!(
        text, "hello",
        "expected terminal tool output, got: {text:?} (a leaked progress notification would not have this text)"
    );

    let _ = client.cancel().await;
    ct.cancel();
    let _ = server_handle.await;
    Ok(())
}

#[derive(Clone)]
struct MultiProgressServer {
    router: Arc<ToolRouter<Self>>,
}

impl MultiProgressServer {
    fn new() -> Self {
        let mut tool_router = ToolRouter::<Self>::new();
        tool_router.add_route(ToolRoute::new_dyn(
            Tool::new(
                "multi_progress",
                "Emits three progress notifications, then returns a fixed string",
                Arc::new(Default::default()),
            ),
            |ctx| {
                Box::pin(async move {
                    for step in 1..=3u32 {
                        let _ = ctx
                            .request_context
                            .peer
                            .notify_progress(ProgressNotificationParam {
                                progress_token: ProgressToken(NumberOrString::Number(1)),
                                progress: step as f64,
                                total: Some(3.0),
                                message: Some(format!("step {step}")),
                            })
                            .await;
                    }
                    Ok(CallToolResult::success(vec![Content::text("done")]))
                })
            },
        ));
        Self {
            router: Arc::new(tool_router),
        }
    }
}

impl ServerHandler for MultiProgressServer {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
    }

    fn call_tool(
        &self,
        request: CallToolRequestParams,
        context: rmcp::service::RequestContext<RoleServer>,
    ) -> impl std::future::Future<Output = Result<CallToolResult, rmcp::ErrorData>> + MaybeSendFuture + '_
    {
        async move {
            let tcc = ToolCallContext::new(self, request, context);
            self.router.call(tcc).await
        }
    }

    fn list_tools(
        &self,
        _request: Option<rmcp::model::PaginatedRequestParams>,
        _context: rmcp::service::RequestContext<RoleServer>,
    ) -> impl std::future::Future<Output = Result<rmcp::model::ListToolsResult, rmcp::ErrorData>>
    + MaybeSendFuture
    + '_ {
        let tools = self.router.list_all();
        async move {
            Ok(rmcp::model::ListToolsResult {
                tools,
                ..Default::default()
            })
        }
    }
}

/// Issue #754, drain-many path: a dyn-routed tool that emits multiple
/// progress notifications before its terminal response must still resolve
/// cleanly under stateless + json_response.
#[tokio::test]
async fn dyn_tool_with_multiple_progress_notifications_returns_terminal_response()
-> anyhow::Result<()> {
    let ct = CancellationToken::new();

    let service: StreamableHttpService<MultiProgressServer, LocalSessionManager> =
        StreamableHttpService::new(
            || Ok(MultiProgressServer::new()),
            Default::default(),
            StreamableHttpServerConfig::default()
                .with_stateful_mode(false)
                .with_json_response(true)
                .with_sse_keep_alive(None)
                .with_cancellation_token(ct.child_token()),
        );

    let router = axum::Router::new().nest_service("/mcp", service);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;

    let server_handle = tokio::spawn({
        let ct = ct.clone();
        async move {
            let _ = axum::serve(listener, router)
                .with_graceful_shutdown(async move { ct.cancelled_owned().await })
                .await;
        }
    });

    let transport = StreamableHttpClientTransport::from_config(
        StreamableHttpClientTransportConfig::with_uri(format!("http://{addr}/mcp")),
    );
    let client = ClientInfo::default().serve(transport).await?;

    let call = client.call_tool(CallToolRequestParams::new("multi_progress"));
    let result = tokio::time::timeout(Duration::from_secs(5), call)
        .await
        .map_err(|_| {
            anyhow::anyhow!(
                "call_tool on a dyn-routed tool with multiple progress notifications hung past 5s (issue #754)"
            )
        })??;

    assert_ne!(result.is_error, Some(true));
    let text = result
        .content
        .first()
        .and_then(|c| c.as_text())
        .map(|t| t.text.as_str())
        .unwrap_or_default();
    assert_eq!(text, "done");

    let _ = client.cancel().await;
    ct.cancel();
    let _ = server_handle.await;
    Ok(())
}
