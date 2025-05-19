use scylla::client::session_builder::SessionBuilder;
use scylla::client::session::Session;
use std::sync::Arc;
use anyhow::Result;

#[derive(Clone)] // Clone is useful if you plan to share the connector
pub struct ScyllaManager {
    pub session: Arc<Session>, // Arc for safe sharing across async tasks
}

impl ScyllaManager {
    /// Creates a new ScyllaManager instance and connects to the database.
    ///
    /// # Arguments
    /// * `uris` - A slice of ScyllaDB node URIs (e.g., `&["127.0.0.1:9042"]`).
    /// * `keyspace` - The name of the keyspace to use.
    pub async fn new(uris: &[&str], keyspace: &str) -> Result<Self> {
        let session = SessionBuilder::new()
            .known_nodes(uris)
            .build()
            .await?;

        // The `?` operator will convert the QueryError from use_keyspace
        // into an anyhow::Error if it occurs.
        session.use_keyspace(keyspace, true).await?;
        println!("Successfully connected to ScyllaDB and selected keyspace '{}'", keyspace);
        Ok(ScyllaManager { session: Arc::new(session) })
    }
}