#!/bin/bash
# Usage: ./scripts/run_sql.sh scripts/bronze/01_create_database.sql
if [ -z "$1" ]; then
  echo "Usage: ./scripts/run_sql.sh <path-to-sql-file>"
  exit 1
fi
echo "Executing: $1"
sqlcmd -S localhost,1433 -U sa -P 'NfipWarehouse2026!' -i "$1" -C
echo ""
echo "Done: $1"
