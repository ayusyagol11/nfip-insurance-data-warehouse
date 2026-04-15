-- ================================================================
-- 01_create_database.sql
-- Creates the NfipInsuranceWarehouse database and schemas
-- Target: Azure SQL Edge via Docker
-- ================================================================

-- Create database if it doesn't exist
IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'NfipInsuranceWarehouse')
BEGIN
    CREATE DATABASE NfipInsuranceWarehouse;
    PRINT 'Database NfipInsuranceWarehouse created.';
END
ELSE
    PRINT 'Database NfipInsuranceWarehouse already exists.';
GO

USE NfipInsuranceWarehouse;
GO

-- Create schemas (Azure SQL Edge does not support IF NOT EXISTS on CREATE SCHEMA)
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'bronze')
    EXEC('CREATE SCHEMA bronze');
GO

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'silver')
    EXEC('CREATE SCHEMA silver');
GO

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'gold')
    EXEC('CREATE SCHEMA gold');
GO

PRINT 'Schemas bronze, silver, gold ready.';
