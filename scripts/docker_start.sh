#!/bin/bash
echo "Starting Azure SQL Edge container..."
docker-compose up -d
echo "Waiting 15 seconds for SQL Server to initialise..."
sleep 15
echo "Testing connection..."
sqlcmd -S localhost,1433 -U sa -P 'NfipWarehouse2026!' -Q "SELECT @@VERSION" -C
echo ""
echo "SQL Server is ready."
echo "Connect via Azure Data Studio at localhost,1433 (user: sa)"
