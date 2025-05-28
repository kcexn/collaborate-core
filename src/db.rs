use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{Executor, PgPool};
use std::sync::Arc;
use std::str::FromStr;
use anyhow::{Context, Result};

#[derive(Clone)]
pub struct Manager {
    pub pool: Arc<PgPool>,
}

impl Manager {
    /// Creates a new DB Manager instance and connects to CockroachDB.
    /// It will also ensure the specified application database exists.
    ///
    /// # Arguments
    /// * `base_uri` - The base URI to connect to CockroachDB, typically pointing to a default
    ///                database like `defaultdb` or `postgres`. This connection is used to
    ///                create the application-specific database if it doesn't exist.
    ///                Example for your Docker setup: "postgres://root@localhost:26257/defaultdb?sslmode=disable"
    /// * `app_db_name` - The name of the application-specific database to use or create (e.g., "collaborate_app").
    pub async fn new(base_uri: &str, app_db_name: &str) -> Result<Self> {
        // 1. Connect to the base URI (e.g., pointing to defaultdb) to be able to create the app_db_name
        let initial_pool_options = PgPoolOptions::new()
            .max_connections(5)
            .acquire_timeout(std::time::Duration::from_secs(10));
        let initial_uri = format!("postgres://{}/defaultdb?sslmode=disable", base_uri);
        let initial_pool = initial_pool_options
            .connect(&initial_uri)
            .await
            .context(format!("Failed to connect to CockroachDB using base URI: {}", &initial_uri))?;

        // 2. Create the application-specific database if it doesn't exist.
        //    Quoting the database name ensures it's handled correctly if it contains
        //    special characters or matches keywords (though simple names are preferred).
        let create_db_query = format!("CREATE DATABASE IF NOT EXISTS \"{}\"", app_db_name);
        initial_pool.execute(create_db_query.as_str())
            .await
            .context(format!("Failed to create database: {}", app_db_name))?;
        
        println!("Successfully ensured database '{}' exists.", app_db_name);
        
        // Close the initial pool as we'll create a new one specifically for the application database.
        initial_pool.close().await;

        // 3. Construct the connection URI for the application database.
        //    We parse the base_uri and then set the database name to app_db_name.
        let uri = format!("postgres://{}/{}?sslmode=disable", base_uri, app_db_name);
        let mut app_conn_options = PgConnectOptions::from_str(&uri)
            .context("Failed to parse uri into connection options")?;
        app_conn_options = app_conn_options.database(app_db_name);
        
        // 4. Connect to the application-specific database with a new pool.
        let app_pool_options = PgPoolOptions::new()
            .max_connections(10) // Configure based on your application's needs
            .acquire_timeout(std::time::Duration::from_secs(10));

        let app_pool = app_pool_options
            .connect_with(app_conn_options.clone()) // PgConnectOptions implements Clone
            .await
            .context(format!("Failed to connect to CockroachDB application database: {}", app_db_name))?;

        println!("Successfully connected to CockroachDB database '{}'", app_db_name);
        
        Ok(Manager { pool: Arc::new(app_pool) })
    }

    /// Example method to check the connection by executing a simple query.
    pub async fn check_connection(&self) -> Result<()> {
        sqlx::query("SELECT 1").execute(&*self.pool).await?;
        println!("Connection check to CockroachDB successful.");
        Ok(())
    }
}