-- NOTE: BULK INSERT is not supported on Azure SQL Edge.
-- Use the Python loader instead: python scripts/bronze/load_via_python.py
-- This script is retained for SQL Server Express/Developer Edition environments.

USE NfipInsuranceWarehouse;
GO

-- ================================================================
-- Script:    03_load_claims.sql
-- Layer:     Bronze
-- Purpose:   Load claims CSV files into bronze.nfip_claims_raw
--            via BULK INSERT from Docker container paths
-- Author:    Aayush Yagol
-- Execute:   ./scripts/run_sql.sh scripts/bronze/03_load_claims.sql
-- ================================================================
--
-- EXECUTION OPTIONS:
--
-- Option A: Via Azure Data Studio
--   1. docker-compose up -d (if not already running)
--   2. Open Azure Data Studio, connect to localhost,1433
--   3. Run this script
--
-- Option B: Via command line (requires local sqlcmd)
--   brew install sqlcmd (if not installed)
--   ./scripts/run_sql.sh scripts/bronze/03_load_claims.sql
--
-- Option C: Python fallback (if BULK INSERT fails)
--   python scripts/bronze/load_via_python.py
--
-- CSV files are mounted inside the container at /data/
-- via docker-compose volume: ./datasets:/data
-- ================================================================

-- Truncate before reload (idempotent)
TRUNCATE TABLE bronze.nfip_claims_raw;
PRINT 'Truncated bronze.nfip_claims_raw';

-- ----------------------------------------------------------------
-- Florida
-- ----------------------------------------------------------------
BULK INSERT bronze.nfip_claims_raw
FROM '/data/claims/FL_claims.csv'
WITH (
    FORMAT = 'CSV',
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '0x0a',
    FIRSTROW = 2,
    TABLOCK,
    MAXERRORS = 100
);
PRINT 'FL claims loaded: ' + CAST(@@ROWCOUNT AS VARCHAR);
GO

-- ----------------------------------------------------------------
-- Louisiana
-- ----------------------------------------------------------------
BULK INSERT bronze.nfip_claims_raw
FROM '/data/claims/LA_claims.csv'
WITH (
    FORMAT = 'CSV',
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '0x0a',
    FIRSTROW = 2,
    TABLOCK,
    MAXERRORS = 100
);
PRINT 'LA claims loaded: ' + CAST(@@ROWCOUNT AS VARCHAR);
GO

-- ----------------------------------------------------------------
-- Texas
-- ----------------------------------------------------------------
BULK INSERT bronze.nfip_claims_raw
FROM '/data/claims/TX_claims.csv'
WITH (
    FORMAT = 'CSV',
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '0x0a',
    FIRSTROW = 2,
    TABLOCK,
    MAXERRORS = 100
);
PRINT 'TX claims loaded: ' + CAST(@@ROWCOUNT AS VARCHAR);
GO

-- ----------------------------------------------------------------
-- New Jersey
-- ----------------------------------------------------------------
BULK INSERT bronze.nfip_claims_raw
FROM '/data/claims/NJ_claims.csv'
WITH (
    FORMAT = 'CSV',
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '0x0a',
    FIRSTROW = 2,
    TABLOCK,
    MAXERRORS = 100
);
PRINT 'NJ claims loaded: ' + CAST(@@ROWCOUNT AS VARCHAR);
GO

-- ----------------------------------------------------------------
-- New York
-- ----------------------------------------------------------------
BULK INSERT bronze.nfip_claims_raw
FROM '/data/claims/NY_claims.csv'
WITH (
    FORMAT = 'CSV',
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '0x0a',
    FIRSTROW = 2,
    TABLOCK,
    MAXERRORS = 100
);
PRINT 'NY claims loaded: ' + CAST(@@ROWCOUNT AS VARCHAR);
GO

-- ----------------------------------------------------------------
-- Post-load: populate source_state from the state column
-- ----------------------------------------------------------------
UPDATE bronze.nfip_claims_raw
SET source_state = state
WHERE source_state IS NULL;
PRINT 'source_state populated from state column';

-- Final count
SELECT 'Total Bronze claims: ' + CAST(COUNT(*) AS VARCHAR) FROM bronze.nfip_claims_raw;
GO
