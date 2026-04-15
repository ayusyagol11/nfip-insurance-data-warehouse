USE NfipInsuranceWarehouse;
GO

-- ================================================================
-- 03_load_claims.sql
-- Loads claims CSV files into bronze.nfip_claims_raw via BULK INSERT
--
-- EXECUTION OPTIONS:
--
-- Option A: Via Azure Data Studio
--   1. docker-compose up -d (if not already running)
--   2. Open Azure Data Studio, connect to localhost,1433
--   3. Run scripts in order: 01 → 02 → 03 → 04
--
-- Option B: Via command line
--   ./scripts/run_sql.sh scripts/bronze/01_create_database.sql
--   ./scripts/run_sql.sh scripts/bronze/02_create_bronze_tables.sql
--   ./scripts/run_sql.sh scripts/bronze/03_load_claims.sql
--   ./scripts/run_sql.sh scripts/bronze/04_load_policies.sql
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
