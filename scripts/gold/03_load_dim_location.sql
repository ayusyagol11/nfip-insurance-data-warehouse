-- ================================================================
-- Script:    03_load_dim_location.sql
-- Layer:     Gold
-- Purpose:   Populate dim_location from distinct state + county
--            combinations in Silver claims and policies, joined
--            to ref_state_info for names and FEMA regions
-- Author:    Aayush Yagol
-- Execute:   ./scripts/run_sql.sh scripts/gold/03_load_dim_location.sql
-- ================================================================

USE NfipInsuranceWarehouse;
GO

-- Clear and reload
DELETE FROM gold.dim_location;
PRINT 'Cleared gold.dim_location';

-- Insert Unknown row (key = -1)
INSERT INTO gold.dim_location (location_key, state_abbrev, state_name, county_fips, fema_region)
VALUES (-1, 'ZZ', 'Unknown', '00000', 0);

-- Populate from Silver: distinct state + county from both claims and policies
INSERT INTO gold.dim_location (location_key, state_abbrev, state_name, county_fips, fema_region)
SELECT
    ROW_NUMBER() OVER (ORDER BY loc.state_abbrev, loc.county_fips) AS location_key,
    loc.state_abbrev,
    COALESCE(s.state_name, 'Unknown')   AS state_name,
    loc.county_fips,
    COALESCE(s.fema_region, 0)          AS fema_region
FROM (
    SELECT DISTINCT state AS state_abbrev, countyCode AS county_fips
    FROM silver.nfip_claims_cleaned
    WHERE state IS NOT NULL

    UNION

    SELECT DISTINCT propertyState AS state_abbrev, countyCode AS county_fips
    FROM silver.nfip_policies_cleaned
    WHERE propertyState IS NOT NULL
) loc
LEFT JOIN silver.ref_state_info s ON s.state_abbrev = loc.state_abbrev;

-- Validation
DECLARE @row_count INT;
SELECT @row_count = COUNT(*) FROM gold.dim_location;
PRINT 'gold.dim_location loaded: ' + CAST(@row_count AS VARCHAR) + ' rows (including Unknown)';

DECLARE @null_keys INT;
SELECT @null_keys = COUNT(*) FROM gold.dim_location WHERE location_key IS NULL;
PRINT 'NULL location_key count: ' + CAST(@null_keys AS VARCHAR) + ' (should be 0)';

DECLARE @dup_keys INT;
SELECT @dup_keys = COUNT(*) FROM (
    SELECT location_key FROM gold.dim_location GROUP BY location_key HAVING COUNT(*) > 1
) d;
PRINT 'Duplicate location_key count: ' + CAST(@dup_keys AS VARCHAR) + ' (should be 0)';
GO
