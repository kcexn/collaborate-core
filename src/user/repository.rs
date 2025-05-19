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
use super::models::User;
use super::errors::UserManagerError;
use scylla::frame::value::Timestamp;
use scylla::prepared_statement::PreparedStatement;
use scylla::transport::errors::QueryError;
use scylla::transport::session::Session;
use scylla::batch::{Batch, BatchStatement};
use scylla::statement::SerialConsistency;
use std::sync::Arc;
use uuid::Uuid;
use chrono::{DateTime, Utc};

#[derive(Clone)] // Clone is fine as it clones Arcs and PreparedStatements
pub struct UserRepository {
    session: Arc<Session>,
    keyspace: String, // Store keyspace name for dynamic query construction
    // Prepared Statements
    prep_insert_user: PreparedStatement,
    prep_insert_user_by_username: PreparedStatement,
    prep_insert_user_by_email: PreparedStatement,
    prep_get_user_by_id: PreparedStatement,
    prep_get_user_id_by_username: PreparedStatement,
    prep_get_user_id_by_email: PreparedStatement,
    prep_update_user_profile: PreparedStatement,
    prep_update_user_password: PreparedStatement,
    prep_update_last_login: PreparedStatement,
    prep_get_user_details_for_delete: PreparedStatement,
    prep_delete_user_by_username: PreparedStatement,
    prep_delete_user_by_email: PreparedStatement,
    prep_delete_user: PreparedStatement,
}

impl UserRepository {
    pub async fn new(session: Arc<Session>, keyspace_name: &str) -> Result<Self, QueryError> {
        let ks = keyspace_name;

        let prep_insert_user_by_username = session
            .prepare(format!(
                "INSERT INTO {}.users_by_username (username, user_id) VALUES (?, ?) IF NOT EXISTS",
                ks
            ))
            .await?;
        let prep_insert_user_by_email = session
            .prepare(format!(
                "INSERT INTO {}.users_by_email (email, user_id) VALUES (?, ?) IF NOT EXISTS",
                ks
            ))
            .await?;
        let prep_insert_user = session
            .prepare(format!(
                "INSERT INTO {}.users (user_id, username, email, hashed_password, first_name, last_name, is_active, email_verified, created_at, updated_at, last_login_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                ks
            ))
            .await?;
        let prep_get_user_by_id = session
            .prepare(format!("SELECT * FROM {}.users WHERE user_id = ?", ks))
            .await?;
        let prep_get_user_id_by_username = session
            .prepare(format!("SELECT user_id FROM {}.users_by_username WHERE username = ?", ks))
            .await?;
        let prep_get_user_id_by_email = session
            .prepare(format!("SELECT user_id FROM {}.users_by_email WHERE email = ?", ks))
            .await?;
        let prep_update_user_profile = session
            .prepare(format!(
                "UPDATE {}.users SET first_name = ?, last_name = ?, is_active = ?, email_verified = ?, updated_at = ? WHERE user_id = ?",
                ks
            ))
            .await?;
        let prep_update_user_password = session
            .prepare(format!(
                "UPDATE {}.users SET hashed_password = ?, updated_at = ? WHERE user_id = ?",
                ks
            ))
            .await?;
        let prep_update_last_login = session
            .prepare(format!(
                "UPDATE {}.users SET last_login_at = ?, updated_at = ? WHERE user_id = ?",
                ks
            ))
            .await?;
        let prep_get_user_details_for_delete = session
            .prepare(format!("SELECT username, email FROM {}.users WHERE user_id = ?", ks))
            .await?;
        let prep_delete_user_by_username = session
            .prepare(format!("DELETE FROM {}.users_by_username WHERE username = ?", ks))
            .await?;
        let prep_delete_user_by_email = session
            .prepare(format!("DELETE FROM {}.users_by_email WHERE email = ?", ks))
            .await?;
        let prep_delete_user = session
            .prepare(format!("DELETE FROM {}.users WHERE user_id = ?", ks))
            .await?;

        Ok(Self {
            session,
            keyspace: keyspace_name.to_string(),
            prep_insert_user,
            prep_insert_user_by_username,
            prep_insert_user_by_email,
            prep_get_user_by_id,
            prep_get_user_id_by_username,
            prep_get_user_id_by_email,
            prep_update_user_profile,
            prep_update_user_password,
            prep_update_last_login,
            prep_get_user_details_for_delete,
            prep_delete_user_by_username,
            prep_delete_user_by_email,
            prep_delete_user,
        })
    }

    // --- Methods for adding statements to a batch ---
    // These are used by the UserService to construct a batch for user creation/deletion

    pub fn add_insert_user_to_batch(&self, batch: &mut Batch, user_id: Uuid, username: &str, email: &str, hashed_password: &str, first_name: Option<&str>, last_name: Option<&str>, is_active: bool, email_verified: bool, created_at: DateTime<Utc>, updated_at: DateTime<Utc>, last_login_at: Option<DateTime<Utc>>) {
        batch.add_statement(self.prep_insert_user.clone(), (user_id, username, email, hashed_password, first_name, last_name, is_active, email_verified, Timestamp(created_at), Timestamp(updated_at), last_login_at.map(Timestamp)));
    }

    pub fn add_insert_user_by_username_to_batch(&self, batch: &mut Batch, username: &str, user_id: Uuid) {
        batch.add_statement(self.prep_insert_user_by_username.clone(), (username, user_id));
    }

    pub fn add_insert_user_by_email_to_batch(&self, batch: &mut Batch, email: &str, user_id: Uuid) {
        batch.add_statement(self.prep_insert_user_by_email.clone(), (email, user_id));
    }

    pub fn add_delete_user_by_username_to_batch(&self, batch: &mut Batch, username: &str) {
        batch.add_statement(self.prep_delete_user_by_username.clone(), (username,));
    }
    
    pub fn add_delete_user_by_email_to_batch(&self, batch: &mut Batch, email: &str) {
        batch.add_statement(self.prep_delete_user_by_email.clone(), (email,));
    }

    pub fn add_delete_user_to_batch(&self, batch: &mut Batch, user_id: Uuid) {
        batch.add_statement(self.prep_delete_user.clone(), (user_id,));
    }

    // --- Direct execution methods ---

    pub async fn execute_batch(&self, batch: &Batch) -> Result<scylla::QueryResult, QueryError> {
        self.session.batch(batch, batch.get_serial_consistency().map_or_else(Default::default, |sc| (sc, None))).await // Use batch's serial consistency
    }

    pub async fn find_user_by_id(&self, user_id: Uuid) -> Result<Option<User>, UserManagerError> {
        self.session.execute(&self.prep_get_user_by_id, (user_id,))
            .await?
            .rows_typed::<User>()?
            .next()
            .transpose()
            .map_err(UserManagerError::from)
    }

    pub async fn find_user_id_by_username(&self, username: &str) -> Result<Option<Uuid>, UserManagerError> {
        self.session.execute(&self.prep_get_user_id_by_username, (username,))
            .await?
            .rows_typed::<(Uuid,)>()?
            .next()
            .transpose()
            .map_err(UserManagerError::from)
            .map(|opt_tuple| opt_tuple.map(|(id,)| id))
    }

    pub async fn find_user_id_by_email(&self, email: &str) -> Result<Option<Uuid>, UserManagerError> {
        self.session.execute(&self.prep_get_user_id_by_email, (email,))
            .await?
            .rows_typed::<(Uuid,)>()?
            .next()
            .transpose()
            .map_err(UserManagerError::from)
            .map(|opt_tuple| opt_tuple.map(|(id,)| id))
    }

    pub async fn find_user_details_for_delete(&self, user_id: Uuid) -> Result<Option<(String, String)>, UserManagerError> {
        self.session.execute(&self.prep_get_user_details_for_delete, (user_id,))
            .await?
            .rows_typed::<(String, String)>()?
            .next()
            .transpose()
            .map_err(UserManagerError::from)
    }

    // ... other direct query methods for updates, etc. ...
    // For example:
    pub async fn update_profile_direct(&self, user_id: Uuid, first_name: Option<&str>, last_name: Option<&str>, is_active: bool, email_verified: bool, updated_at: DateTime<Utc>) -> Result<(), QueryError> {
        self.session.execute(&self.prep_update_user_profile, (first_name, last_name, is_active, email_verified, Timestamp(updated_at), user_id)).await?;
        Ok(())
    }

    pub async fn update_password_direct(&self, user_id: Uuid, new_hashed_password: &str, updated_at: DateTime<Utc>) -> Result<(), QueryError> {
        self.session.execute(&self.prep_update_user_password, (new_hashed_password, Timestamp(updated_at), user_id)).await?;
        Ok(())
    }

    pub async fn update_last_login_direct(&self, user_id: Uuid, login_time: DateTime<Utc>, updated_at: DateTime<Utc>) -> Result<(), QueryError> {
        self.session.execute(&self.prep_update_last_login, (Some(Timestamp(login_time)), Timestamp(updated_at), user_id)).await?;
        Ok(())
    }
}