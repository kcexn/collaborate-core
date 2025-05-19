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
use chrono::{DateTime, Utc};
use scylla::FromRow;
use uuid::Uuid;

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
