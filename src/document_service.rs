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

use crate::db::Manager; // Assuming db::Manager is your CockroachDB manager
use anyhow::{Context, Result}; // Use anyhow::Result for convenience
use chrono::{DateTime, Utc}; // Needed for Utc::now() and DateTime<Utc>
use sqlx::{Row, FromRow, Executor}; // For deriving FromRow for sqlx
use std::sync::Arc;
use uuid::Uuid;

// Helper trait and implementation for truncating DateTime<Utc> to milliseconds
trait TruncateToMillis {
    fn trunc_to_millis(self) -> Self;
}

impl TruncateToMillis for DateTime<Utc> {
    fn trunc_to_millis(self) -> Self {
        // Convert to millis since epoch and back to DateTime<Utc> to truncate sub-millisecond precision.
        DateTime::from_timestamp_millis(self.timestamp_millis())
            .expect("Failed to truncate DateTime<Utc> to milliseconds; timestamp out of range for valid input")
    }
}

#[derive(Clone, Debug, FromRow, PartialEq)] // Changed to sqlx::FromRow
pub struct DocumentMetadata {
    pub id: Uuid,
    pub name: String,
    pub created_at: DateTime<Utc>, // Changed to DateTime<Utc>
    pub updated_at: DateTime<Utc>, // Changed to DateTime<Utc>
}

#[derive(Clone, Debug, FromRow, PartialEq)] // Changed to sqlx::FromRow
pub struct DocumentContent {
    pub document_id: Uuid,
    pub crdt_data: Vec<u8>, // Opaque CRDT data blob
    pub updated_at: DateTime<Utc>, // Changed to DateTime<Utc>
}

#[derive(Clone, Debug, PartialEq)]
pub struct Document {
    pub metadata: DocumentMetadata,
    pub content: Option<DocumentContent>,
}

#[derive(Clone)]
pub struct DocumentService {
    db_manager: Arc<Manager>,
}

impl DocumentService {
    pub async fn new(db_manager: Arc<Manager>) -> Result<Self> {
        let service = DocumentService { db_manager };
        service.initialize_schema().await?;
        Ok(service)
    }

    async fn initialize_schema(&self) -> Result<()> {
        self.db_manager.pool
            .execute(
                "CREATE TABLE IF NOT EXISTS documents_metadata (
                    id UUID PRIMARY KEY,
                    name TEXT,
                    created_at TIMESTAMPTZ NOT NULL,
                    updated_at TIMESTAMPTZ NOT NULL
                )",
            )
            .await
            .context("Failed to create documents_metadata table")?;

        self.db_manager.pool
            .execute(
                "CREATE TABLE IF NOT EXISTS documents_content (
                    document_id UUID PRIMARY KEY,
                    crdt_data BYTEA,
                    updated_at TIMESTAMPTZ NOT NULL,
                    FOREIGN KEY (document_id) REFERENCES documents_metadata(id) ON DELETE CASCADE
                )",
            )
            .await
            .context("Failed to create documents_content table")?;
        println!("Document service schema initialized.");
        Ok(())
    }

    pub async fn create_document(&self, name: &str) -> Result<DocumentMetadata> {
        let id = Uuid::new_v4();
        let now = Utc::now().trunc_to_millis();
        let metadata = DocumentMetadata {
            id,
            name: name.to_string(),
            created_at: now,
            updated_at: now,
        };

        self.db_manager.pool
            .execute(sqlx::query(
                    "INSERT INTO documents_metadata (id, name, created_at, updated_at) VALUES ($1, $2, $3, $4)"
                )
                .bind(metadata.id)
                .bind(&metadata.name)
                .bind(metadata.created_at)
                .bind(metadata.updated_at)
            ).await
            .context(format!("Failed to insert document metadata for ID {}", id))?;
        
        // Optionally, create an initial empty content entry
        self.update_document_content(id, Vec::new()).await.ok(); // Best effort for initial empty content

        println!("Created document '{}' with ID: {}", name, id);
        Ok(metadata)
    }

    pub async fn get_document_metadata(&self, doc_id: Uuid) -> Result<Option<DocumentMetadata>> {
        let row_opt = sqlx::query(
                "SELECT id, name, created_at, updated_at FROM documents_metadata WHERE id = $1"
            )
            .bind(doc_id)
            .fetch_optional(&*self.db_manager.pool)
            .await
            .context(format!("Failed to query document metadata for ID {}", doc_id))?;

        match row_opt {
            Some(row) => {
            // Manually map the row to DocumentMetadata
            // try_get can be used for fallible conversions, or get for infallible ones if types are exact.
                let metadata = DocumentMetadata {
                    id: row.try_get("id").context("Failed to get 'id' from row")?, // UUIDs don't need truncation
                    name: row.try_get("name").context("Failed to get 'name' from row")?, // String doesn't need truncation
                    created_at: row.try_get::<DateTime<Utc>, _>("created_at").context("Failed to get 'created_at' from row")?.trunc_to_millis(),
                    updated_at: row.try_get::<DateTime<Utc>, _>("updated_at").context("Failed to get 'updated_at' from row")?.trunc_to_millis(),
                };
                Ok(Some(metadata))
            },
            None => Ok(None),
        }
    }


    pub async fn update_document_content(&self, doc_id: Uuid, content_data: Vec<u8>) -> Result<()> {
        let now = Utc::now().trunc_to_millis(); // Truncate to millisecond precision

        // Upsert content
        self.db_manager.pool
            .execute(sqlx::query(
                "INSERT INTO documents_content (document_id, crdt_data, updated_at)
                 VALUES ($1, $2, $3)
                 ON CONFLICT (document_id) DO UPDATE
                 SET crdt_data = EXCLUDED.crdt_data,
                     updated_at = EXCLUDED.updated_at"
                )
                .bind(doc_id)
                .bind(content_data) // Vec<u8> for BYTEA
                .bind(now)
            )
            .await
            .context(format!("Failed to update document content for ID {}", doc_id))?;

        // Update metadata's updated_at timestamp
        self.db_manager.pool
            .execute(sqlx::query(
                "UPDATE documents_metadata SET updated_at = $1 WHERE id = $2"
                )
                .bind(now)
                .bind(doc_id)
            )
            .await
            .context(format!("Failed to update metadata timestamp for ID {}", doc_id))?;
        
        println!("Updated content for document ID: {}", doc_id);
        Ok(())
    }

    pub async fn get_document_content(&self, doc_id: Uuid) -> Result<Option<DocumentContent>> {
        let row_opt = sqlx::query(
                "SELECT document_id, crdt_data, updated_at FROM documents_content WHERE document_id = $1"
            )
            .bind(doc_id)
            .fetch_optional(&*self.db_manager.pool)
            .await
            .context(format!("Failed to query document content for ID {}", doc_id))?;
        match row_opt {
            Some(row) => {
                let content = DocumentContent {
                    document_id: row.try_get("document_id").context("Failed to get 'document_id' from row")?, // UUID
                    crdt_data: row.try_get("crdt_data").context("Failed to get 'crdt_data' from row")?,       // Vec<u8>
                    updated_at: row.try_get::<DateTime<Utc>, _>("updated_at").context("Failed to get 'updated_at' from row")?.trunc_to_millis(),
                };
                Ok(Some(content))
            },
            None => Ok(None),
        }
    }

    pub async fn get_document(&self, doc_id: Uuid) -> Result<Option<Document>> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::Manager as DbManager;
    use anyhow::{Context, Result};
    use std::sync::Arc;

    // Configure these constants for your CockroachDB test environment
    const TEST_DB_NAME: &str = "collaborate_core_doc_service_test";
    const COCKROACH_BASE_URI: &str = "root@localhost:26257";

    // Helper to get a db::Manager configured for the test database.
    // This function will also ensure the test database exists via db::Manager::new.
    async fn get_test_db_manager() -> Result<Arc<DbManager>> {
        let manager = DbManager::new(COCKROACH_BASE_URI, TEST_DB_NAME)
            .await
            .context(format!("Failed to initialize DbManager for test database '{}'", TEST_DB_NAME))?;
        println!("Test database '{}' ensured or created via DbManager.", TEST_DB_NAME);
        Ok(Arc::new(manager))
    }

    // Helper to get a DocumentService instance initialized for tests.
    async fn get_test_document_service() -> Result<DocumentService> {
        let db_manager = get_test_db_manager().await?;
        // DocumentService::new will call initialize_schema, creating tables in the test database
        DocumentService::new(db_manager).await
            .context("Failed to create DocumentService for tests")
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
        assert!(updated_metadata.updated_at >= original_updated_at, "Metadata updated_at should be same or newer.");
        assert!(fetched_content.updated_at >= original_updated_at, "Content updated_at should be same or newer.");

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