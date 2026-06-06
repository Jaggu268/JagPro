-- Runs once when the Postgres container first starts.
-- Creates the taxi data warehouse database and user.

CREATE DATABASE taxi_db;
CREATE USER taxi_user WITH ENCRYPTED PASSWORD 'taxi_pass';
GRANT ALL PRIVILEGES ON DATABASE taxi_db TO taxi_user;
