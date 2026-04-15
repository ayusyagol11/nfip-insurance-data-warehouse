-- ================================================================
-- Script:    04_load_dim_flood_zone.sql
-- Layer:     Gold
-- Purpose:   Populate dim_flood_zone from distinct flood zone values
--            in Silver, mapped to zone categories via ref_flood_zone_info
-- Author:    Aayush Yagol
-- Execute:   ./scripts/run_sql.sh scripts/gold/04_load_dim_flood_zone.sql
-- ================================================================

USE NfipInsuranceWarehouse;
GO

-- Clear and reload
DELETE FROM gold.dim_flood_zone;
PRINT 'Cleared gold.dim_flood_zone';

-- Insert Unknown row (key = -1)
INSERT INTO gold.dim_flood_zone (flood_zone_key, flood_zone_code, zone_category, zone_description, is_special_flood_hazard)
VALUES (-1, 'Unknown', 'Unknown', 'Zone not mapped', 0);

-- Populate from distinct flood zones across claims and policies
INSERT INTO gold.dim_flood_zone (flood_zone_key, flood_zone_code, zone_category, zone_description, is_special_flood_hazard)
SELECT
    ROW_NUMBER() OVER (ORDER BY fz.flood_zone_code) AS flood_zone_key,
    fz.flood_zone_code,
    fz.zone_category,
    COALESCE(ref.zone_description, 'Zone not mapped')   AS zone_description,
    COALESCE(ref.is_special_flood_hazard, 0)             AS is_special_flood_hazard
FROM (
    SELECT DISTINCT
        floodZone AS flood_zone_code,
        zone_category
    FROM silver.nfip_claims_cleaned
    WHERE floodZone IS NOT NULL

    UNION

    SELECT DISTINCT
        floodZone AS flood_zone_code,
        zone_category
    FROM silver.nfip_policies_cleaned
    WHERE floodZone IS NOT NULL
) fz
LEFT JOIN silver.ref_flood_zone_info ref ON ref.zone_category = fz.zone_category;

-- Validation
DECLARE @row_count INT;
SELECT @row_count = COUNT(*) FROM gold.dim_flood_zone;
PRINT 'gold.dim_flood_zone loaded: ' + CAST(@row_count AS VARCHAR) + ' rows (including Unknown)';

DECLARE @null_keys INT;
SELECT @null_keys = COUNT(*) FROM gold.dim_flood_zone WHERE flood_zone_key IS NULL;
PRINT 'NULL flood_zone_key count: ' + CAST(@null_keys AS VARCHAR) + ' (should be 0)';

DECLARE @dup_keys INT;
SELECT @dup_keys = COUNT(*) FROM (
    SELECT flood_zone_key FROM gold.dim_flood_zone GROUP BY flood_zone_key HAVING COUNT(*) > 1
) d;
PRINT 'Duplicate flood_zone_key count: ' + CAST(@dup_keys AS VARCHAR) + ' (should be 0)';
GO
