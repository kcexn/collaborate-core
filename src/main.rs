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
use anyhow::Result;

// Keep the module declaration if you plan to use it later
// mod scylla_connector;
// use scylla_connector::ScyllaManager;

mod http_server; // Add the new http_server module

#[tokio::main]
async fn main() -> Result<()> {
    // Optional: Initialize tracing for better logs
    // tracing_subscriber::fmt::init();

    println!("Starting HTTP server...");
    http_server::run_server().await?;

    // ScyllaDB connection code (can be re-enabled later)
    // println!("Attempting to connect to ScyllaDB...");
    // let _manager = ScyllaManager::new(
    //     &["127.0.0.1:9042"],
    //     "collaborate_core"
    // ).await?;
    // let (version,) = _manager.session
    //     .query_unpaged("SELECT release_version FROM system.local", &[])
    //     .await?
    //     .into_rows_result()?
    //     .first_row::<(String,)>()?;
    // println!("ScyllaDB release_version: {}", version);
    Ok(())
}
