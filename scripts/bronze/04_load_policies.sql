USE NfipInsuranceWarehouse;
GO

-- ================================================================
-- Script:    04_load_policies.sql
-- Layer:     Bronze
-- Purpose:   Load policies CSV files into bronze.nfip_policies_raw
--            via BULK INSERT from Docker container paths
-- Author:    Aayush Yagol
-- Execute:   ./scripts/run_sql.sh scripts/bronze/04_load_policies.sql
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
--   ./scripts/run_sql.sh scripts/bronze/04_load_policies.sql
--
-- Option C: Python fallback (if BULK INSERT fails)
--   python scripts/bronze/load_via_python.py
--
-- CSV files are mounted inside the container at /data/
-- via docker-compose volume: ./datasets:/data
-- ================================================================

-- Truncate before reload (idempotent)
TRUNCATE TABLE bronze.nfip_policies_raw;
PRINT 'Truncated bronze.nfip_policies_raw';

-- ----------------------------------------------------------------
-- Florida
-- ----------------------------------------------------------------
BULK INSERT bronze.nfip_policies_raw
FROM '/data/policies/FL_policies.csv'
WITH (
    FORMAT = 'CSV',
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '0x0a',
    FIRSTROW = 2,
    TABLOCK,
    MAXERRORS = 100
);
PRINT 'FL policies loaded: ' + CAST(@@ROWCOUNT AS VARCHAR);
GO

-- ----------------------------------------------------------------
-- Louisiana
-- ----------------------------------------------------------------
BULK INSERT bronze.nfip_policies_raw
FROM '/data/policies/LA_policies.csv'
WITH (
    FORMAT = 'CSV',
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '0x0a',
    FIRSTROW = 2,
    TABLOCK,
    MAXERRORS = 100
);
PRINT 'LA policies loaded: ' + CAST(@@ROWCOUNT AS VARCHAR);
GO

-- ----------------------------------------------------------------
-- Texas
-- ----------------------------------------------------------------
BULK INSERT bronze.nfip_policies_raw
FROM '/data/policies/TX_policies.csv'
WITH (
    FORMAT = 'CSV',
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '0x0a',
    FIRSTROW = 2,
    TABLOCK,
    MAXERRORS = 100
);
PRINT 'TX policies loaded: ' + CAST(@@ROWCOUNT AS VARCHAR);
GO

-- ----------------------------------------------------------------
-- New Jersey
-- ----------------------------------------------------------------
BULK INSERT bronze.nfip_policies_raw
FROM '/data/policies/NJ_policies.csv'
WITH (
    FORMAT = 'CSV',
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '0x0a',
    FIRSTROW = 2,
    TABLOCK,
    MAXERRORS = 100
);
PRINT 'NJ policies loaded: ' + CAST(@@ROWCOUNT AS VARCHAR);
GO

-- ----------------------------------------------------------------
-- New York
-- ----------------------------------------------------------------
BULK INSERT bronze.nfip_policies_raw
FROM '/data/policies/NY_policies.csv'
WITH (
    FORMAT = 'CSV',
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '0x0a',
    FIRSTROW = 2,
    TABLOCK,
    MAXERRORS = 100
);
PRINT 'NY policies loaded: ' + CAST(@@ROWCOUNT AS VARCHAR);
GO

-- ----------------------------------------------------------------
-- Post-load: populate source_state from the propertyState column
-- ----------------------------------------------------------------
UPDATE bronze.nfip_policies_raw
SET source_state = propertyState
WHERE source_state IS NULL;
PRINT 'source_state populated from propertyState column';

-- Final count
SELECT 'Total Bronze policies: ' + CAST(COUNT(*) AS VARCHAR) FROM bronze.nfip_policies_raw;
GO
