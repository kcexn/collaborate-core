// Copyright (C) 2025 Kevin Exton
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.
use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        State,
    },
    response::{Html, IntoResponse},
    routing::get,
    Router,
};
use tokio::net::TcpListener; // Import TcpListener
use std::net::SocketAddr;
use std::sync::Arc;

// Shared application state (if needed, e.g., for broadcasting messages)
#[derive(Clone)]
struct AppState {}

pub async fn run_server() -> anyhow::Result<()> {
    let app_state = Arc::new(AppState {});

    let app = Router::new()
        .route("/", get(root_handler))
        .route("/ws", get(websocket_handler))
        .with_state(app_state);

    let addr = SocketAddr::from(([127, 0, 0, 1], 3000));
    let listener = TcpListener::bind(addr).await?;
    println!("HTTP server listening on {}", listener.local_addr()?); // Use listener.local_addr()
    axum::serve(listener, app.into_make_service()).await?;

    Ok(())
}

async fn root_handler() -> Html<&'static str> {
    Html("<h1>Hello, World!</h1><p><a href='/ws'>Connect to WebSocket</a> (use a WebSocket client)</p>\n")
}

async fn websocket_handler(
    ws: WebSocketUpgrade,
    State(_state): State<Arc<AppState>>, // Example of accessing shared state
) -> impl IntoResponse {
    ws.on_upgrade(handle_socket)
}

async fn handle_socket(mut socket: WebSocket) {
    println!("WebSocket client connected");
    while let Some(Ok(msg)) = socket.recv().await {
        if let Message::Text(text) = msg {
            println!("Received WebSocket message: {}", text);
            if socket.send(Message::Text(format!("You said: {}", text))).await.is_err() {
                // Client disconnected
                println!("WebSocket client disconnected");
                break;
            }
        }
    }
}