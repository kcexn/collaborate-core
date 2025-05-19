-- init-db.cql

-- Create a keyspace if it doesn't already exist.
-- For development, SimpleStrategy is often used. For production, you'd use NetworkTopologyStrategy.
CREATE KEYSPACE IF NOT EXISTS collaborate_core
WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };

-- Use the new keyspace
USE collaborate_core;

-- Main table to store user details
CREATE TABLE IF NOT EXISTS collaborate_core.users (
    user_id UUID PRIMARY KEY,
    username TEXT,          -- Denormalized for easy access after lookup, canonical in users_by_username
    email TEXT,             -- Denormalized for easy access after lookup, canonical in users_by_email
    hashed_password TEXT,   -- Store salted and hashed password
    first_name TEXT,        -- Optional: User's first name
    last_name TEXT,         -- Optional: User's last name
    is_active BOOLEAN,      -- To enable/disable account (e.g., true for active, false for suspended)
    email_verified BOOLEAN, -- Whether the user has verified their email address
    last_login_at TIMESTAMP,-- Timestamp of the last successful login
    created_at TIMESTAMP,   -- Timestamp of account creation
    updated_at TIMESTAMP    -- Timestamp of the last profile update
);

-- Lookup table for finding user_id by username and ensuring username uniqueness
CREATE TABLE IF NOT EXISTS collaborate_core.users_by_username (
    username TEXT PRIMARY KEY,
    user_id UUID
);

-- Lookup table for finding user_id by email and ensuring email uniqueness
CREATE TABLE IF NOT EXISTS collaborate_core.users_by_email (
    email TEXT PRIMARY KEY,
    user_id UUID
);

-- You can add some initial data if needed for development
-- INSERT INTO users (user_id, username, first_name, last_name, email, created_at) VALUES (uuid(), 'john_doe', 'John', 'Doe', 'john.doe@example.com', toTimestamp(now()));
-- INSERT INTO users (user_id, username, first_name, last_name, email, created_at) VALUES (uuid(), 'jane_smith', 'Jane', 'Smith', 'jane.smith@example.com', toTimestamp(now()));