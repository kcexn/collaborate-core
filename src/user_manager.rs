use chrono::{DateTime, Utc};
use scylla::frame::value::Timestamp;
use scylla::prepared_statement::PreparedStatement;
use scylla::transport::errors::{NewSessionError, QueryError};
use scylla::transport::session::Session;
use scylla::batch::Batch;
use scylla::statement::SerialConsistency;
use scylla::FromRow;
use std::sync::Arc;
use uuid::Uuid;

// --- Constants ---
const KEYSPACE: &str = "collaborate_core";

// --- Error Types ---
#[derive(Debug, thiserror::Error)]
pub enum UserManagerError {
    #[error("Scylla query error: {0}")]
    QueryError(#[from] QueryError),
    #[error("Scylla session error: {0}")]
    NewSessionError(#[from] NewSessionError),
    #[error("User not found")]
    UserNotFound,
    #[error("Username '{0}' is already taken")]
    UsernameTaken(String),
    #[error("Email '{0}' is already taken")]
    EmailTaken(String),
    #[error("Username or email already exists")]
    UsernameOrEmailAlreadyExists, // Used when batch LWT fails generically
    #[error("Inconsistent data: Found in lookup but not main table for ID {0}")]
    InconsistentData(Uuid),
    #[error("Failed to apply batch operation: {0}")]
    BatchOperationFailed(String),
    #[error("Row not found when one was expected")]
    ExpectedRowNotFound,
    #[error("Failed to parse row: {0}")]
    RowParseError(#[from] scylla::cql_to_rust::FromRowError),
}

#[derive(Debug, thiserror::Error)]
pub enum AuthenticationError {
    #[error("User not found with the provided identifier")]
    UserNotFound,
    #[error("Account is not active")]
    AccountNotActive,
    #[error("Email not verified")]
    EmailNotVerified, // Depending on your application's rules
    #[error("Database error: {0}")]
    DatabaseError(#[from] UserManagerError),
}

// --- Data Structures ---
#[derive(Debug, Clone, FromRow, PartialEq)]
pub struct User {
    pub user_id: Uuid,
    pub username: String,
    pub email: String,
    pub hashed_password: String,
    pub first_name: Option<String>,
    pub last_name: Option<String>,
    pub is_active: bool,
    pub email_verified: bool,
    pub last_login_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AuthDetails {
    pub user_id: Uuid,
    pub username: String,
    pub email: String,
    pub hashed_password: String,
    pub is_active: bool,
    pub email_verified: bool,
}

// --- UserManager ---
#[derive(Clone)]
pub struct UserManager {
    session: Arc<Session>,
    prep_create_user_batch: PreparedStatement, // For the batch itself
    prep_insert_user: PreparedStatement,
    prep_insert_user_by_username: PreparedStatement,
    prep_insert_user_by_email: PreparedStatement,
    prep_get_user_by_id: PreparedStatement,
    prep_get_user_id_by_username: PreparedStatement,
    prep_get_user_id_by_email: PreparedStatement,
    prep_update_user_profile: PreparedStatement, // Example for basic profile update
    prep_update_user_password: PreparedStatement,
    prep_update_last_login: PreparedStatement,
    prep_get_user_for_delete: PreparedStatement, // To get username/email before delete
    prep_delete_user_by_username: PreparedStatement,
    prep_delete_user_by_email: PreparedStatement,
    prep_delete_user: PreparedStatement,
}

impl UserManager {
    pub async fn new(session: Arc<Session>) -> Result<Self, QueryError> {
        // Prepare statements for user operations
        // CREATE
        let prep_insert_user_by_username = session
            .prepare(format!(
                "INSERT INTO {}.users_by_username (username, user_id) VALUES (?, ?) IF NOT EXISTS",
                KEYSPACE
            ))
            .await?;
        let prep_insert_user_by_email = session
            .prepare(format!(
                "INSERT INTO {}.users_by_email (email, user_id) VALUES (?, ?) IF NOT EXISTS",
                KEYSPACE
            ))
            .await?;
        let prep_insert_user = session
            .prepare(format!(
                "INSERT INTO {}.users (user_id, username, email, hashed_password, first_name, last_name, is_active, email_verified, created_at, updated_at, last_login_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                KEYSPACE
            ))
            .await?;
        
        // This is a conceptual representation. Actual batching is done by adding statements to a Batch object.
        // We don't prepare a "batch" itself, but the individual statements that go into it.
        // For the create_user method, we'll construct a batch dynamically.
        // So, prep_create_user_batch is more of a placeholder for the logic.
        // We'll use the individual insert statements above within a batch.
        let prep_create_user_batch = prep_insert_user.clone(); // Placeholder, not directly used as a batch statement


        // READ
        let prep_get_user_by_id = session
            .prepare(format!("SELECT * FROM {}.users WHERE user_id = ?", KEYSPACE))
            .await?;
        let prep_get_user_id_by_username = session
            .prepare(format!("SELECT user_id FROM {}.users_by_username WHERE username = ?", KEYSPACE))
            .await?;
        let prep_get_user_id_by_email = session
            .prepare(format!("SELECT user_id FROM {}.users_by_email WHERE email = ?", KEYSPACE))
            .await?;

        // UPDATE
        let prep_update_user_profile = session
            .prepare(format!(
                "UPDATE {}.users SET first_name = ?, last_name = ?, is_active = ?, email_verified = ?, updated_at = ? WHERE user_id = ?",
                KEYSPACE
            ))
            .await?;
        let prep_update_user_password = session
            .prepare(format!(
                "UPDATE {}.users SET hashed_password = ?, updated_at = ? WHERE user_id = ?",
                KEYSPACE
            ))
            .await?;
        let prep_update_last_login = session
            .prepare(format!(
                "UPDATE {}.users SET last_login_at = ?, updated_at = ? WHERE user_id = ?",
                KEYSPACE
            ))
            .await?;

        // DELETE
        let prep_get_user_for_delete = session
            .prepare(format!("SELECT username, email FROM {}.users WHERE user_id = ?", KEYSPACE))
            .await?;
        let prep_delete_user_by_username = session
            .prepare(format!("DELETE FROM {}.users_by_username WHERE username = ?", KEYSPACE))
            .await?;
        let prep_delete_user_by_email = session
            .prepare(format!("DELETE FROM {}.users_by_email WHERE email = ?", KEYSPACE))
            .await?;
        let prep_delete_user = session
            .prepare(format!("DELETE FROM {}.users WHERE user_id = ?", KEYSPACE))
            .await?;


        Ok(Self {
            session,
            prep_create_user_batch,
            prep_insert_user,
            prep_insert_user_by_username,
            prep_insert_user_by_email,
            prep_get_user_by_id,
            prep_get_user_id_by_username,
            prep_get_user_id_by_email,
            prep_update_user_profile,
            prep_update_user_password,
            prep_update_last_login,
            prep_get_user_for_delete,
            prep_delete_user_by_username,
            prep_delete_user_by_email,
            prep_delete_user,
        })
    }

    pub async fn create_user(
        &self,
        username: &str,
        email: &str,
        hashed_password: &str,
        first_name: Option<&str>,
        last_name: Option<&str>,
        is_active: bool,
        email_verified: bool,
    ) -> Result<User, UserManagerError> {
        let user_id = Uuid::new_v4();
        let now = Utc::now();

        let mut batch: Batch = Default::default();
        batch.set_serial_consistency(Some(SerialConsistency::LocalSerial)); // Important for LWTs

        // Order matters for how you might interpret failure, but Scylla handles atomicity.
        batch.add_statement(self.prep_insert_user_by_username.clone(), (username, user_id));
        batch.add_statement(self.prep_insert_user_by_email.clone(), (email, user_id));
        batch.add_statement(
            self.prep_insert_user.clone(),
            (
                user_id,
                username,
                email,
                hashed_password,
                first_name,
                last_name,
                is_active,
                email_verified,
                Timestamp(now),
                Timestamp(now),
                None::<Timestamp>, // last_login_at initially null
            ),
        );
        
        let result = self.session.batch(&batch, Default::default()).await?;

        if !result.was_applied() {
            // The batch with LWTs (IF NOT EXISTS) failed.
            // Check username first
            if let Some(_row) = self.session.execute(&self.prep_get_user_id_by_username, (username,)).await?.rows_typed::<(Uuid,)>()?.next().transpose()? {
                return Err(UserManagerError::UsernameTaken(username.to_string()));
            }
            // Check email next
            if let Some(_row) = self.session.execute(&self.prep_get_user_id_by_email, (email,)).await?.rows_typed::<(Uuid,)>()?.next().transpose()? {
                 return Err(UserManagerError::EmailTaken(email.to_string()));
            }
            // If neither specific check caught it, return a generic error.
            // This part might need refinement based on how `was_applied` behaves with multiple LWTs.
            // The driver docs state: "If any of the conditions are not met, the batch will not be applied".
            return Err(UserManagerError::UsernameOrEmailAlreadyExists);
        }

        Ok(User {
            user_id,
            username: username.to_string(),
            email: email.to_string(),
            hashed_password: hashed_password.to_string(),
            first_name: first_name.map(String::from),
            last_name: last_name.map(String::from),
            is_active,
            email_verified,
            last_login_at: None,
            created_at: now,
            updated_at: now,
        })
    }

    pub async fn get_user_by_id(&self, user_id: Uuid) -> Result<Option<User>, UserManagerError> {
        let result_opt = self
            .session
            .execute(&self.prep_get_user_by_id, (user_id,))
            .await?
            .rows_typed::<User>()?
            .next();
        
        match result_opt {
            Some(Ok(user)) => Ok(Some(user)),
            Some(Err(e)) => Err(UserManagerError::RowParseError(e)),
            None => Ok(None),
        }
    }

    async fn get_user_id_by_username(&self, username: &str) -> Result<Option<Uuid>, UserManagerError> {
        let row_opt = self
            .session
            .execute(&self.prep_get_user_id_by_username, (username,))
            .await?
            .rows_typed::<(Uuid,)>()? // Expecting a tuple with one Uuid
            .next();

        match row_opt {
            Some(Ok((id,))) => Ok(Some(id)),
            Some(Err(e)) => Err(UserManagerError::RowParseError(e)),
            None => Ok(None),
        }
    }
    
    async fn get_user_id_by_email(&self, email: &str) -> Result<Option<Uuid>, UserManagerError> {
         let row_opt = self
            .session
            .execute(&self.prep_get_user_id_by_email, (email,))
            .await?
            .rows_typed::<(Uuid,)>()?
            .next();
        
        match row_opt {
            Some(Ok((id,))) => Ok(Some(id)),
            Some(Err(e)) => Err(UserManagerError::RowParseError(e)),
            None => Ok(None),
        }
    }

    pub async fn get_user_by_username(&self, username: &str) -> Result<Option<User>, UserManagerError> {
        if let Some(user_id) = self.get_user_id_by_username(username).await? {
            self.get_user_by_id(user_id).await
        } else {
            Ok(None)
        }
    }

    pub async fn get_user_by_email(&self, email: &str) -> Result<Option<User>, UserManagerError> {
        if let Some(user_id) = self.get_user_id_by_email(email).await? {
            self.get_user_by_id(user_id).await
        } else {
            Ok(None)
        }
    }
    
    pub async fn get_user_for_authentication(
        &self,
        identifier: &str, // Can be username or email
    ) -> Result<AuthDetails, AuthenticationError> {
        let user = if identifier.contains('@') { // Simple check for email
            self.get_user_by_email(identifier).await?
        } else {
            self.get_user_by_username(identifier).await?
        };

        match user {
            Some(u) => {
                // Caller should verify password against u.hashed_password
                // Caller can also check u.is_active and u.email_verified
                Ok(AuthDetails {
                    user_id: u.user_id,
                    username: u.username,
                    email: u.email,
                    hashed_password: u.hashed_password,
                    is_active: u.is_active,
                    email_verified: u.email_verified,
                })
            }
            None => Err(AuthenticationError::UserNotFound),
        }
    }

    pub async fn update_user_profile(
        &self,
        user_id: Uuid,
        first_name: Option<String>,
        last_name: Option<String>,
        is_active: Option<bool>,
        email_verified: Option<bool>,
    ) -> Result<User, UserManagerError> {
        // First, get the current user data to fill in unchanged fields if necessary
        // or to ensure the user exists.
        let mut current_user = self.get_user_by_id(user_id).await?
            .ok_or(UserManagerError::UserNotFound)?;

        let new_first_name = first_name.or(current_user.first_name);
        let new_last_name = last_name.or(current_user.last_name);
        let new_is_active = is_active.unwrap_or(current_user.is_active);
        let new_email_verified = email_verified.unwrap_or(current_user.email_verified);
        let updated_at = Utc::now();

        self.session
            .execute(
                &self.prep_update_user_profile,
                (
                    new_first_name.clone(),
                    new_last_name.clone(),
                    new_is_active,
                    new_email_verified,
                    Timestamp(updated_at),
                    user_id,
                ),
            )
            .await?;
        
        // Update current_user struct for return
        current_user.first_name = new_first_name;
        current_user.last_name = new_last_name;
        current_user.is_active = new_is_active;
        current_user.email_verified = new_email_verified;
        current_user.updated_at = updated_at;

        Ok(current_user)
    }

    pub async fn update_user_password(
        &self,
        user_id: Uuid,
        new_hashed_password: &str,
    ) -> Result<(), UserManagerError> {
        let updated_at = Utc::now();
        self.session
            .execute(
                &self.prep_update_user_password,
                (new_hashed_password, Timestamp(updated_at), user_id),
            )
            .await?;
        Ok(())
    }

    pub async fn update_last_login(&self, user_id: Uuid) -> Result<(), UserManagerError> {
        let now = Utc::now();
        self.session
            .execute(&self.prep_update_last_login, (Some(Timestamp(now)), Timestamp(now), user_id))
            .await?;
        Ok(())
    }
    
    pub async fn delete_user(&self, user_id: Uuid) -> Result<(), UserManagerError> {
        // 1. Get username and email for the user_id
        let row = self.session.execute(&self.prep_get_user_for_delete, (user_id,)).await?
            .rows_typed::<(String, String)>()? // (username, email)
            .next();

        let (username, email) = match row {
            Some(Ok(data)) => data,
            Some(Err(e)) => return Err(UserManagerError::RowParseError(e)),
            None => return Err(UserManagerError::UserNotFound), // User to delete not found
        };

        // 2. Create and execute batch delete
        let mut batch: Batch = Default::default();
        batch.add_statement(self.prep_delete_user_by_username.clone(), (username,));
        batch.add_statement(self.prep_delete_user_by_email.clone(), (email,));
        batch.add_statement(self.prep_delete_user.clone(), (user_id,));

        self.session.batch(&batch, Default::default()).await?;
        Ok(())
    }
}

// --- Helper for Connection (Example) ---
pub async fn connect_to_db(nodes: &[&str]) -> Result<Arc<Session>, NewSessionError> {
    let session = Session::builder().known_nodes(nodes).build().await?;
    session
        .use_keyspace(KEYSPACE, false)
        .await
        .map_err(|e| NewSessionError::Other(format!("Failed to use keyspace {}: {}", KEYSPACE, e)))?;
    Ok(Arc::new(session))
}

// --- Example Usage (typically in main.rs or tests) ---
#[cfg(test)]
mod tests {
    use super::*;
    use scylla::test_utils::unique_keyspace_name;
    use std::env;

    // Helper to set up a temporary keyspace and tables for testing
    async fn setup_test_session() -> (Arc<Session>, String) {
        let node = env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".to_string());
        let session_uninit = Session::builder().known_node(&node).build().await.unwrap();
        
        let keyspace_name = unique_keyspace_name();
        
        session_uninit.query(format!("CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION = {{ 'class' : 'SimpleStrategy', 'replication_factor' : 1 }}", keyspace_name), &[]).await.unwrap();
        session_uninit.use_keyspace(keyspace_name.clone(), false).await.unwrap();

        // Create tables (simplified from your init-db.cql for brevity, ensure they match)
        session_uninit.query(format!(r#"
            CREATE TABLE IF NOT EXISTS {}.users (
                user_id UUID PRIMARY KEY, username TEXT, email TEXT, hashed_password TEXT,
                first_name TEXT, last_name TEXT, is_active BOOLEAN, email_verified BOOLEAN,
                last_login_at TIMESTAMP, created_at TIMESTAMP, updated_at TIMESTAMP
            );"#, keyspace_name), &[]).await.unwrap();
        session_uninit.query(format!(r#"
            CREATE TABLE IF NOT EXISTS {}.users_by_username (username TEXT PRIMARY KEY, user_id UUID);"#, keyspace_name), &[]).await.unwrap();
        session_uninit.query(format!(r#"
            CREATE TABLE IF NOT EXISTS {}.users_by_email (email TEXT PRIMARY KEY, user_id UUID);"#, keyspace_name), &[]).await.unwrap();
        
        (Arc::new(session_uninit), keyspace_name)
    }
    
    // Redefine UserManager to take keyspace_name for testing flexibility
    // This is a simplified version for testing, in real app you'd use the main UserManager
    struct TestUserManager {
        session: Arc<Session>,
        keyspace: String,
        // ... (prepared statements would also need to be dynamic or use the keyspace)
        // For simplicity, tests might directly use session.query or prepare statements dynamically
    }

    impl TestUserManager {
        async fn new(session: Arc<Session>, keyspace: String) -> Self {
            // In a real test setup, you'd prepare statements using the dynamic keyspace name
            Self { session, keyspace }
        }

        // Example: simplified create_user for test, not using prepared statements for brevity
        async fn create_user_test(&self, user: &User) -> Result<(), QueryError> {
            let ks = &self.keyspace;
            // LWTs for uniqueness
            let username_taken_res = self.session.query(format!("INSERT INTO {}.users_by_username (username, user_id) VALUES ('{}', {}) IF NOT EXISTS", ks, user.username, user.user_id), &[]).await?;
            if !username_taken_res.was_applied() { return Err(QueryError::Other("Username taken".into())) }

            let email_taken_res = self.session.query(format!("INSERT INTO {}.users_by_email (email, user_id) VALUES ('{}', {}) IF NOT EXISTS", ks, user.email, user.user_id), &[]).await?;
             if !email_taken_res.was_applied() {
                // Rollback username insert (simplified)
                self.session.query(format!("DELETE FROM {}.users_by_username WHERE username = '{}'", ks, user.username), &[]).await?;
                return Err(QueryError::Other("Email taken".into())) 
            }
            
            self.session.query(format!(
                "INSERT INTO {}.users (user_id, username, email, hashed_password, first_name, last_name, is_active, email_verified, created_at, updated_at, last_login_at) VALUES ({}, '{}', '{}', '{}', {}, {}, {}, {}, {}, {}, {})",
                ks, user.user_id, user.username, user.email, user.hashed_password,
                user.first_name.as_ref().map_or("null".to_string(), |s| format!("'{}'", s)),
                user.last_name.as_ref().map_or("null".to_string(), |s| format!("'{}'", s)),
                user.is_active, user.email_verified,
                scylla::frame::value::Timestamp(user.created_at),
                scylla::frame::value::Timestamp(user.updated_at),
                user.last_login_at.map_or("null".to_string(), |ts| format!("{}", scylla::frame::value::Timestamp(ts)))
            ), &[]).await?;
            Ok(())
        }
         async fn get_user_by_id_test(&self, user_id: Uuid) -> Result<Option<User>, QueryError> {
            let query = format!("SELECT * FROM {}.users WHERE user_id = {}", self.keyspace, user_id);
            Ok(self.session.query(query, &[])
                .await?
                .rows_typed::<User>()?
                .next()
                .transpose()?)
        }
    }


    #[tokio::test]
    #[ignore] // Ignored because it requires a running ScyllaDB instance and SCYLLA_URI env var
    async fn test_user_crud_operations() {
        let (session, ks_name) = setup_test_session().await;
        // For this test, we'll use the TestUserManager which is simpler for dynamic keyspaces
        // In a real app, you'd instantiate the main UserManager with the session.
        // let user_manager = UserManager::new(session.clone()).await.unwrap(); 
        let test_user_manager = TestUserManager::new(session.clone(), ks_name.clone());


        let user_id = Uuid::new_v4();
        let now = Utc::now();
        let test_user = User {
            user_id,
            username: "testuser".to_string(),
            email: "test@example.com".to_string(),
            hashed_password: "hashed_password_example".to_string(),
            first_name: Some("Test".to_string()),
            last_name: Some("User".to_string()),
            is_active: true,
            email_verified: false,
            last_login_at: None,
            created_at: now,
            updated_at: now,
        };

        // Create (using simplified test method)
        test_user_manager.create_user_test(&test_user).await.expect("Failed to create user");

        // Read
        let fetched_user = test_user_manager.get_user_by_id_test(user_id).await
            .expect("Failed to fetch user")
            .expect("User not found after creation");
        
        assert_eq!(fetched_user.username, "testuser");
        assert_eq!(fetched_user.email, "test@example.com");

        // Test uniqueness (attempt to create same user - should fail due to LWT in create_user_test)
        let duplicate_user_result = test_user_manager.create_user_test(&test_user).await;
        assert!(duplicate_user_result.is_err(), "Should not be able to create user with duplicate username/email");


        // To test the main UserManager, you'd need to ensure KEYSPACE constant matches the test keyspace,
        // or make UserManager configurable with keyspace name.
        // For now, this demonstrates the structure.
        // Example with main UserManager (if KEYSPACE was dynamic or matched test):
        // let main_user_manager = UserManager::new(session.clone()).await.unwrap(); // Assuming session uses the test keyspace
        // let created_user_main = main_user_manager.create_user(
        //     "testuser_main", "main@example.com", "pass", Some("Main"), None, true, false
        // ).await.unwrap();
        // let fetched_main = main_user_manager.get_user_by_id(created_user_main.user_id).await.unwrap().unwrap();
        // assert_eq!(fetched_main.username, "testuser_main");

        // Cleanup (optional, as keyspace is unique)
        session.query(format!("DROP KEYSPACE IF EXISTS {}", ks_name), &[]).await.unwrap();
    }
}
