-- 1) Create database (if not already created)
CREATE DATABASE marketdata;

-- 2) Create dedicated user for the application
CREATE USER ib_app WITH PASSWORD 'CHANGE_THIS_STRONG_PASSWORD';

-- 3) Allow the app to connect to the DB
GRANT CONNECT ON DATABASE marketdata TO ib_app;

-- 4) Switch to the database
\c marketdata

-- 5) Create schema for app objects (optional but recommended)
CREATE SCHEMA IF NOT EXISTS market AUTHORIZATION ib_app;

-- 6) Let app use the schema
GRANT USAGE, CREATE ON SCHEMA market TO ib_app;

-- 7) If you created tables in public earlier, you can either:
--    A) keep using public schema, OR
--    B) move everything into market schema (recommended going forward)
