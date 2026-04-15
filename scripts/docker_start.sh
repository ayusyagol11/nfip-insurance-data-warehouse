#!/bin/bash
echo "Starting Azure SQL Edge container..."
docker-compose up -d
echo "Waiting 15 seconds for SQL Server to initialise..."
sleep 15
echo "Testing connection..."
docker exec nfip-sqlserver /opt/mssql-tools/bin/sqlcmd \
  -S localhost -U sa -P 'NfipWarehouse2026!' \
  -Q "SELECT @@VERSION"
echo "SQL Server is ready. Connect via Azure Data Studio at localhost,1433"
