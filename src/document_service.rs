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

#[cfg(test)]
mod tests {
    use super::*; // Import items from parent module (DocumentService, DocumentMetadata, etc.)
    use crate::scylla_connector::ScyllaManager; // For ScyllaManager::new
    use anyhow::{Context, Result};
    use scylla::client::session_builder::SessionBuilder; // For initial session to create keyspace
    use std::sync::Arc;
    // Uuid is already imported in the parent module and brought into scope by `use super::*;`

    const TEST_KEYSPACE_NAME: &str = "collaborate_core_test";
    const SCYLLA_URIS: &[&str] = &["127.0.0.1:9042"]; // Ensure this matches your test ScyllaDB

    // Helper to get a ScyllaManager configured for the test keyspace.
    // This function will also ensure the test keyspace exists.
    async fn get_test_scylla_manager() -> Result<Arc<ScyllaManager>> {
        // 1. Create a temporary session to ensure the keyspace exists
        let temp_session = SessionBuilder::new()
            .known_nodes(SCYLLA_URIS)
            .build()
            .await
            .context("Failed to connect to ScyllaDB for test keyspace creation")?;

        temp_session
            .query_unpaged(
                format!(
                    "CREATE KEYSPACE IF NOT EXISTS {} WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': '1'}}",
                    TEST_KEYSPACE_NAME
                ),
                &[],
            )
            .await
            .context(format!("Failed to create test keyspace: {}", TEST_KEYSPACE_NAME))?;
        
        println!("Test keyspace '{}' ensured or created.", TEST_KEYSPACE_NAME);

        // 2. Create the ScyllaManager which will connect and use the test keyspace
        let scylla_manager = ScyllaManager::new(SCYLLA_URIS, TEST_KEYSPACE_NAME).await?;
        Ok(Arc::new(scylla_manager))
    }

    // Helper to get a DocumentService instance initialized for tests.
    async fn get_test_document_service() -> Result<DocumentService> {
        let scylla_manager = get_test_scylla_manager().await?;
        // DocumentService::new will call initialize_schema, creating tables in the test keyspace
        DocumentService::new(scylla_manager).await
    }

    #[tokio::test]
    async fn test_create_and_get_document_metadata() -> Result<()> {
        let doc_service = get_test_document_service().await
            .expect("Failed to initialize test document service");

        let doc_name = "Test Document for Metadata";
        let created_metadata = doc_service.create_document(doc_name).await?;
        
        assert_eq!(created_metadata.name, doc_name);

        let fetched_metadata_opt = doc_service.get_document_metadata(created_metadata.id).await?;
        
        assert!(fetched_metadata_opt.is_some(), "Fetched metadata should exist");
        let fetched_metadata = fetched_metadata_opt.unwrap();

        assert_eq!(fetched_metadata.id, created_metadata.id);
        assert_eq!(fetched_metadata.name, created_metadata.name);
        assert_eq!(fetched_metadata.created_at, created_metadata.created_at);
        
        // Check that an initial empty content was attempted
        let content_opt = doc_service.get_document_content(created_metadata.id).await?;
        assert!(content_opt.is_some(), "Initial content should exist");
        assert!(content_opt.unwrap().crdt_data.is_empty(), "Initial CRDT data should be empty");

        Ok(())
    }

    #[tokio::test]
    async fn test_update_and_get_document_content() -> Result<()> {
        let doc_service = get_test_document_service().await
            .expect("Failed to initialize test document service");

        let doc_name = "Test Document for Content";
        let metadata = doc_service.create_document(doc_name).await?;
        let doc_id = metadata.id;

        let new_content_data = vec![1, 2, 3, 4, 5];
        let original_updated_at = metadata.updated_at;

        doc_service.update_document_content(doc_id, new_content_data.clone()).await?;

        let fetched_content_opt = doc_service.get_document_content(doc_id).await?;
        assert!(fetched_content_opt.is_some(), "Fetched content should exist after update");
        let fetched_content = fetched_content_opt.unwrap();

        assert_eq!(fetched_content.document_id, doc_id);
        assert_eq!(fetched_content.crdt_data, new_content_data);
        
        let updated_metadata = doc_service.get_document_metadata(doc_id).await?.unwrap();
        assert!(updated_metadata.updated_at.0 >= original_updated_at.0, "Metadata updated_at should be same or newer.");
        assert!(fetched_content.updated_at.0 >= original_updated_at.0, "Content updated_at should be same or newer.");

        Ok(())
    }

    #[tokio::test]
    async fn test_get_full_document() -> Result<()> {
        let doc_service = get_test_document_service().await
            .expect("Failed to initialize test document service");

        let doc_name = "Test Full Document";
        let metadata = doc_service.create_document(doc_name).await?;
        let doc_id = metadata.id;

        let content_data = vec![10, 20, 30];
        doc_service.update_document_content(doc_id, content_data.clone()).await?;

        let full_document_opt = doc_service.get_document(doc_id).await?;
        assert!(full_document_opt.is_some(), "Full document should exist");
        let full_document = full_document_opt.unwrap();

        assert_eq!(full_document.metadata.id, doc_id);
        assert!(full_document.content.is_some());
        assert_eq!(full_document.content.unwrap().crdt_data, content_data);

        Ok(())
    }

    #[tokio::test]
    async fn test_get_non_existent_document() -> Result<()> {
        let doc_service = get_test_document_service().await
            .expect("Failed to initialize test document service");
        
        let non_existent_id = Uuid::new_v4();

        assert!(doc_service.get_document_metadata(non_existent_id).await?.is_none());
        assert!(doc_service.get_document_content(non_existent_id).await?.is_none());
        assert!(doc_service.get_document(non_existent_id).await?.is_none());
        
        Ok(())
    }
}