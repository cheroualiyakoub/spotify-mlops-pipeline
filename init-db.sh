#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE USER mlflow_user WITH PASSWORD 'mlflow_password';
    CREATE DATABASE mlflow OWNER mlflow_user;
    
    CREATE USER dagster WITH PASSWORD 'dagster_password';
    CREATE DATABASE dagster OWNER dagster;

    CREATE USER lakefs_user WITH PASSWORD 'lakefs_password';
    CREATE DATABASE lakefs OWNER lakefs_user;
    
    \c dagster;
    GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO dagster;
    ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL PRIVILEGES ON TABLES TO dagster;

    \c mlflow;
    GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO mlflow_user;
    ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL PRIVILEGES ON TABLES TO mlflow_user;

    \c lakefs;
    GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO lakefs_user;
    ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL PRIVILEGES ON TABLES TO lakefs_user;

    GRANT ALL PRIVILEGES ON DATABASE mlflow TO mlflow_user;
    GRANT ALL PRIVILEGES ON DATABASE dagster TO dagster;
    GRANT ALL PRIVILEGES ON DATABASE lakefs TO lakefs_user;
EOSQL