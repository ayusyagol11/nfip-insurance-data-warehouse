-- ================================================================
-- Script:    06_load_dim_occupancy.sql
-- Layer:     Gold
-- Purpose:   Populate dim_occupancy from distinct occupancy types
--            in Silver claims and policies, with residential flag
-- Author:    Aayush Yagol
-- Execute:   ./scripts/run_sql.sh scripts/gold/06_load_dim_occupancy.sql
-- ================================================================

USE NfipInsuranceWarehouse;
GO

-- Clear and reload
DELETE FROM gold.dim_occupancy;
PRINT 'Cleared gold.dim_occupancy';

-- Insert Unknown row (key = -1)
INSERT INTO gold.dim_occupancy (occupancy_key, occupancy_type, residential_flag)
VALUES (-1, 'Unknown', 0);

-- NFIP occupancy type codes:
--   1  = Single Family Residence
--   2  = 2-4 Unit Residential
--   3  = Other Residential (5+ units)
--   4  = Non-Residential
--   6  = Non-Residential Business
--  11  = Residential Condo (unit)
--  12  = Residential Condo (building)
--  13  = Non-Residential Condo (unit)
--  14  = Non-Residential Condo (building)
--  15  = Residential Manufactured/Mobile Home
--  16  = Non-Residential Manufactured/Mobile Home
--  17  = Other Non-Residential
--  18  = Other Residential
--  19  = Unknown / Other

INSERT INTO gold.dim_occupancy (occupancy_key, occupancy_type, residential_flag)
SELECT
    ROW_NUMBER() OVER (ORDER BY occupancy_type) AS occupancy_key,
    occupancy_type,
    CASE
        WHEN occupancy_type IN ('1', '2', '3', '11', '12', '15', '18')  THEN 1
        ELSE 0
    END AS residential_flag
FROM (
    SELECT DISTINCT COALESCE(occupancyType, 'Unknown') AS occupancy_type
    FROM silver.nfip_claims_cleaned

    UNION

    SELECT DISTINCT COALESCE(occupancyType, 'Unknown') AS occupancy_type
    FROM silver.nfip_policies_cleaned
) occ;

-- Validation
DECLARE @row_count INT;
SELECT @row_count = COUNT(*) FROM gold.dim_occupancy;
PRINT 'gold.dim_occupancy loaded: ' + CAST(@row_count AS VARCHAR) + ' rows (including Unknown)';

DECLARE @null_keys INT;
SELECT @null_keys = COUNT(*) FROM gold.dim_occupancy WHERE occupancy_key IS NULL;
PRINT 'NULL occupancy_key count: ' + CAST(@null_keys AS VARCHAR) + ' (should be 0)';

DECLARE @dup_keys INT;
SELECT @dup_keys = COUNT(*) FROM (
    SELECT occupancy_key FROM gold.dim_occupancy GROUP BY occupancy_key HAVING COUNT(*) > 1
) d;
PRINT 'Duplicate occupancy_key count: ' + CAST(@dup_keys AS VARCHAR) + ' (should be 0)';
GO
