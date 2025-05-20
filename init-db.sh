#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE USER mlflow_user WITH PASSWORD 'mlflow_password';
    CREATE DATABASE mlflow OWNER mlflow_user;
    
    CREATE USER dagster WITH PASSWORD 'dagster_password';
    CREATE DATABASE dagster OWNER dagster;
    
    \c dagster;
    GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO dagster;
    ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL PRIVILEGES ON TABLES TO dagster;
EOSQL