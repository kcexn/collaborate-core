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
// GNU General Public License for more details.s
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.
mod db;
mod document_service;
mod http_server;

use anyhow::Result;
use std::sync::Arc;
use db::Manager;
use document_service::DocumentService;

#[tokio::main]
async fn main() -> Result<()> {
    println!("Attempting to connect to database...");
    let manager = Arc::new(Manager::new(
        &"root@localhost:26257",
        "collaborate_app"
    ).await?);

    manager.check_connection().await?;

    println!("Initializing DocumentService...");
    let doc_service = Arc::new(DocumentService::new(manager.clone()).await?);
    println!("DocumentService initialized.");

    println!("Starting HTTP server...");
    http_server::run_server(doc_service).await?; // Pass DocumentService to the HTTP server

    Ok(())
}
