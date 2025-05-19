-- init-db.cql

-- Create a keyspace if it doesn't already exist.
-- For development, SimpleStrategy is often used. For production, you'd use NetworkTopologyStrategy.
CREATE KEYSPACE IF NOT EXISTS collaborate_core
WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };

-- Use the new keyspace
USE collaborate_core;

-- Create a users table if it doesn't already exist
CREATE TABLE IF NOT EXISTS users (
    user_id UUID PRIMARY KEY,
    username TEXT,
    first_name TEXT,
    last_name TEXT,
    email TEXT,
    created_at TIMESTAMP
);

-- You can add some initial data if needed for development
-- INSERT INTO users (user_id, username, first_name, last_name, email, created_at) VALUES (uuid(), 'john_doe', 'John', 'Doe', 'john.doe@example.com', toTimestamp(now()));
-- INSERT INTO users (user_id, username, first_name, last_name, email, created_at) VALUES (uuid(), 'jane_smith', 'Jane', 'Smith', 'jane.smith@example.com', toTimestamp(now()));