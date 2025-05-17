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
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.
use scylla::{Session, SessionBuilder};
use anyhow::Result; // Using anyhow for convenient error handling

#[tokio::main]
async fn main() -> Result<()> {
    println!("Attempting to connect to ScyllaDB...");

    // The URI for your ScyllaDB instance.
    // "127.0.0.1:9042" is the default if Docker is running locally and port is mapped.
    let uri = "127.0.0.1:9042";

    let session: Session = SessionBuilder::new()
        .known_node(uri)
        .build()
        .await?;

    println!("Successfully connected to ScyllaDB at {}!", uri);

    // Let's perform a simple query to verify the connection
    // and get the ScyllaDB version.
    let query_result = session.query("SELECT release_version FROM system.local", &[]).await?;

    if let Some(rows) = query_result.rows {
        for row in rows {
            let version: String = row.into_typed::<(String,)>()?.0;
            println!("ScyllaDB release_version: {}", version);
        }
    }
    Ok(())
}