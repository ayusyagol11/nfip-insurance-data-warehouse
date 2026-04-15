#!/bin/bash
# Usage: ./scripts/run_sql.sh scripts/bronze/01_create_database.sql
if [ -z "$1" ]; then
  echo "Usage: ./scripts/run_sql.sh <path-to-sql-file>"
  exit 1
fi
docker exec -i nfip-sqlserver /opt/mssql-tools/bin/sqlcmd \
  -S localhost -U sa -P 'NfipWarehouse2026!' \
  -i /dev/stdin < "$1"
