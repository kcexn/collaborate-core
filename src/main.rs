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
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    println!("Attempting to connect to ScyllaDB...");
    let uri = "127.0.0.1:9042";
    let session: Session = SessionBuilder::new()
        .known_node(uri)
        .use_keyspace("collaborate_core", true)
        .build()
        .await?;
    println!("Successfully connected to ScyllaDB at {}!", uri);
    let (version,): (String,) = session
        .query_unpaged("SELECT release_version FROM system.local", &[])
        .await?
        .into_rows_result()?
        .first_row::<(String,)>()?;
    println!("ScyllaDB release_version: {}", version);
    Ok(())
}
