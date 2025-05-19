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
use scylla::transport::errors::{NewSessionError, QueryError};
use uuid::Uuid;

#[derive(Debug, thiserror::Error)]
pub enum UserManagerError {
    #[error("Scylla query error: {0}")]
    QueryError(#[from] QueryError),
    #[error("Scylla session error: {0}")]
    NewSessionError(#[from] NewSessionError), // Might be less relevant if session is passed in
    #[error("User not found")]
    UserNotFound,
    #[error("Username '{0}' is already taken")]
    UsernameTaken(String),
    #[error("Email '{0}' is already taken")]
    EmailTaken(String),
    #[error("Username or email already exists")]
    UsernameOrEmailAlreadyExists,
    #[error("Inconsistent data: Found in lookup but not main table for ID {0}")]
    InconsistentData(Uuid),
    #[error("Failed to apply batch operation: {0}")]
    BatchOperationFailed(String), // This might become more specific or be part of QueryError
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
    EmailNotVerified,
    #[error("Database error: {0}")]
    DatabaseError(#[from] UserManagerError), // This shows a dependency, good to keep in mind
}
