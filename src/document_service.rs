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

use crate::scylla_connector::ScyllaManager;
use anyhow::{Context, Result}; // Use anyhow::Result for convenience
use chrono::Utc; // Needed for Utc::now()
use scylla::DeserializeRow;
use scylla::value::CqlTimestamp; // Updated to use CqlTimestamp
use std::sync::Arc;
use uuid::Uuid;

#[derive(Clone, Debug, DeserializeRow, PartialEq)] // Updated: FromRow -> DeserializeRow
pub struct DocumentMetadata {
    pub id: Uuid,
    pub name: String,
    pub created_at: CqlTimestamp,
    pub updated_at: CqlTimestamp,
}

#[derive(Clone, Debug, DeserializeRow, PartialEq)] // Updated: FromRow -> DeserializeRow
pub struct DocumentContent {
    pub document_id: Uuid,
    pub crdt_data: Vec<u8>, // Opaque CRDT data blob
    pub updated_at: CqlTimestamp, // Changed to CqlTimestamp
}

#[derive(Clone, Debug, PartialEq)]
pub struct Document {
    pub metadata: DocumentMetadata,
    pub content: Option<DocumentContent>, // Content might not exist or might be fetched separately
}

#[derive(Clone)]
pub struct DocumentService {
    scylla_manager: Arc<ScyllaManager>,
}

impl DocumentService {
    pub async fn new(scylla_manager: Arc<ScyllaManager>) -> Result<Self> { // Return anyhow::Result
        let service = DocumentService { scylla_manager };
        service.initialize_schema().await?;
        Ok(service)
    }

    async fn initialize_schema(&self) -> Result<()> { // Return anyhow::Result
        self.scylla_manager.session
            .query_unpaged( // Changed to query_unpaged
                "CREATE TABLE IF NOT EXISTS documents_metadata (
                    id UUID PRIMARY KEY,
                    name TEXT,
                    created_at TIMESTAMP,
                    updated_at TIMESTAMP
                )",
                &[],
            )
            .await
            .context("Failed to create documents_metadata table")?;

        self.scylla_manager.session
            .query_unpaged( // Changed to query_unpaged
                "CREATE TABLE IF NOT EXISTS documents_content (
                    document_id UUID PRIMARY KEY,
                    crdt_data BLOB,
                    updated_at TIMESTAMP
                )",
                &[],
            )
            .await
            .context("Failed to create documents_content table")?;
        println!("Document service schema initialized.");
        Ok(())
    }

    pub async fn create_document(&self, name: &str) -> Result<DocumentMetadata> { // Return anyhow::Result
        let id = Uuid::new_v4();
        let current_cql_timestamp = CqlTimestamp(Utc::now().timestamp_millis());
        let metadata = DocumentMetadata {
            id,
            name: name.to_string(),
            created_at: current_cql_timestamp,
            updated_at: current_cql_timestamp,
        };

        self.scylla_manager.session
            .query_unpaged( // Changed to query_unpaged
                "INSERT INTO documents_metadata (id, name, created_at, updated_at) VALUES (?, ?, ?, ?)",
                // Directly use the CqlTimestamp fields from the metadata struct
                (&metadata.id, &metadata.name, metadata.created_at, metadata.updated_at),
            )
            .await
            .context(format!("Failed to insert document metadata for ID {}", id))?;
        
        // Optionally, create an initial empty content entry
        self.update_document_content(id, Vec::new()).await.ok(); // Best effort for initial empty content

        println!("Created document '{}' with ID: {}", name, id);
        Ok(metadata)
    }

    pub async fn get_document_metadata(&self, doc_id: Uuid) -> Result<Option<DocumentMetadata>> {
        let query_result = self.scylla_manager.session
                .query_unpaged("SELECT id, name, created_at, updated_at FROM documents_metadata WHERE id = ?", (doc_id,))
                .await
                .context(format!("Failed to query document metadata for ID {}", doc_id))?;
        let result_rows = query_result.into_rows_result()?;

        // Iterate over the rows. Since we expect at most one row for a given ID,
        // this loop will execute at most once.
        for row_result in result_rows.rows::<DocumentMetadata>()? {
            // If a row is found, deserialize it and return.
            return Ok(Some(row_result?));
        }
        // If the loop completes without returning, no row was found.
        Ok(None)
    }

    pub async fn update_document_content(&self, doc_id: Uuid, content_data: Vec<u8>) -> Result<()> { // Return anyhow::Result
        let current_cql_timestamp = CqlTimestamp(Utc::now().timestamp_millis());

        // Upsert content
        self.scylla_manager.session
            .query_unpaged(
                "INSERT INTO documents_content (document_id, crdt_data, updated_at) VALUES (?, ?, ?)",
                (doc_id, content_data, current_cql_timestamp),
            )
            .await
            .context(format!("Failed to update document content for ID {}", doc_id))?;

        // Update metadata's updated_at timestamp
        self.scylla_manager.session
            .query_unpaged(
                "UPDATE documents_metadata SET updated_at = ? WHERE id = ?",
                (current_cql_timestamp, doc_id),
            )
            .await
            .context(format!("Failed to update metadata timestamp for ID {}", doc_id))?;
        
        println!("Updated content for document ID: {}", doc_id);
        Ok(())
    }

    pub async fn get_document_content(&self, doc_id: Uuid) -> Result<Option<DocumentContent>> { // Return anyhow::Result
        let query_result = self.scylla_manager.session
            .query_unpaged("SELECT document_id, crdt_data, updated_at FROM documents_content WHERE document_id = ?", (doc_id,))
            .await
            .context(format!("Failed to query document content for ID {}", doc_id))?;
        let result_rows = query_result.into_rows_result()?;

        // Iterate over the rows. Since we expect at most one row for a given ID,
        // this loop will execute at most once.
        for row_result in result_rows.rows::<DocumentContent>()? {
            // If a row is found, deserialize it and return.
            return Ok(Some(row_result?));
        }
        // If the loop completes without returning, no row was found.
        Ok(None)
    }

    pub async fn get_document(&self, doc_id: Uuid) -> Result<Option<Document>> { // Return anyhow::Result
        let metadata_opt = self.get_document_metadata(doc_id).await?;
        match metadata_opt {
            Some(metadata) => {
                let content_opt = self.get_document_content(doc_id).await?;
                Ok(Some(Document {
                    metadata,
                    content: content_opt,
                }))
            }
            None => Ok(None),
        }
    }
}

// Basic tests can be added here later on, potentially using a test keyspace
// or mocking ScyllaManager.