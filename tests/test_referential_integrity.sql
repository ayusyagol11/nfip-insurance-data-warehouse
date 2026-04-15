-- ================================================================
-- Script:    test_referential_integrity.sql
-- Layer:     Tests
-- Purpose:   Validate that every foreign key in fact tables resolves
--            to a valid dimension row (no orphan keys)
-- Author:    Aayush Yagol
-- Execute:   ./scripts/run_sql.sh tests/test_referential_integrity.sql
-- ================================================================

USE NfipInsuranceWarehouse;
GO

PRINT '=========================================';
PRINT 'REFERENTIAL INTEGRITY VALIDATION';
PRINT '=========================================';
PRINT '';

DECLARE @orphans INT;

-- ----------------------------------------------------------------
-- fact_claims (5 checks)
-- ----------------------------------------------------------------
PRINT '--- fact_claims ---';

-- 1. date_key
SELECT @orphans = COUNT(*)
FROM gold.fact_claims fc
LEFT JOIN gold.dim_date dd ON fc.date_key = dd.date_key
WHERE dd.date_key IS NULL;
PRINT CASE WHEN @orphans = 0 THEN '[PASS]' ELSE '[FAIL]' END
    + ' date_key orphans: ' + CAST(@orphans AS VARCHAR);

-- 2. location_key
SELECT @orphans = COUNT(*)
FROM gold.fact_claims fc
LEFT JOIN gold.dim_location dl ON fc.location_key = dl.location_key
WHERE dl.location_key IS NULL;
PRINT CASE WHEN @orphans = 0 THEN '[PASS]' ELSE '[FAIL]' END
    + ' location_key orphans: ' + CAST(@orphans AS VARCHAR);

-- 3. flood_zone_key
SELECT @orphans = COUNT(*)
FROM gold.fact_claims fc
LEFT JOIN gold.dim_flood_zone dfz ON fc.flood_zone_key = dfz.flood_zone_key
WHERE dfz.flood_zone_key IS NULL;
PRINT CASE WHEN @orphans = 0 THEN '[PASS]' ELSE '[FAIL]' END
    + ' flood_zone_key orphans: ' + CAST(@orphans AS VARCHAR);

-- 4. building_type_key
SELECT @orphans = COUNT(*)
FROM gold.fact_claims fc
LEFT JOIN gold.dim_building_type dbt ON fc.building_type_key = dbt.building_type_key
WHERE dbt.building_type_key IS NULL;
PRINT CASE WHEN @orphans = 0 THEN '[PASS]' ELSE '[FAIL]' END
    + ' building_type_key orphans: ' + CAST(@orphans AS VARCHAR);

-- 5. occupancy_key
SELECT @orphans = COUNT(*)
FROM gold.fact_claims fc
LEFT JOIN gold.dim_occupancy doc ON fc.occupancy_key = doc.occupancy_key
WHERE doc.occupancy_key IS NULL;
PRINT CASE WHEN @orphans = 0 THEN '[PASS]' ELSE '[FAIL]' END
    + ' occupancy_key orphans: ' + CAST(@orphans AS VARCHAR);

PRINT '';

-- ----------------------------------------------------------------
-- fact_policies (7 checks)
-- ----------------------------------------------------------------
PRINT '--- fact_policies ---';

-- 1. effective_date_key
SELECT @orphans = COUNT(*)
FROM gold.fact_policies fp
LEFT JOIN gold.dim_date dd ON fp.effective_date_key = dd.date_key
WHERE dd.date_key IS NULL;
PRINT CASE WHEN @orphans = 0 THEN '[PASS]' ELSE '[FAIL]' END
    + ' effective_date_key orphans: ' + CAST(@orphans AS VARCHAR);

-- 2. termination_date_key
SELECT @orphans = COUNT(*)
FROM gold.fact_policies fp
LEFT JOIN gold.dim_date dd ON fp.termination_date_key = dd.date_key
WHERE dd.date_key IS NULL;
PRINT CASE WHEN @orphans = 0 THEN '[PASS]' ELSE '[FAIL]' END
    + ' termination_date_key orphans: ' + CAST(@orphans AS VARCHAR);

-- 3. location_key
SELECT @orphans = COUNT(*)
FROM gold.fact_policies fp
LEFT JOIN gold.dim_location dl ON fp.location_key = dl.location_key
WHERE dl.location_key IS NULL;
PRINT CASE WHEN @orphans = 0 THEN '[PASS]' ELSE '[FAIL]' END
    + ' location_key orphans: ' + CAST(@orphans AS VARCHAR);

-- 4. flood_zone_key
SELECT @orphans = COUNT(*)
FROM gold.fact_policies fp
LEFT JOIN gold.dim_flood_zone dfz ON fp.flood_zone_key = dfz.flood_zone_key
WHERE dfz.flood_zone_key IS NULL;
PRINT CASE WHEN @orphans = 0 THEN '[PASS]' ELSE '[FAIL]' END
    + ' flood_zone_key orphans: ' + CAST(@orphans AS VARCHAR);

-- 5. building_type_key
SELECT @orphans = COUNT(*)
FROM gold.fact_policies fp
LEFT JOIN gold.dim_building_type dbt ON fp.building_type_key = dbt.building_type_key
WHERE dbt.building_type_key IS NULL;
PRINT CASE WHEN @orphans = 0 THEN '[PASS]' ELSE '[FAIL]' END
    + ' building_type_key orphans: ' + CAST(@orphans AS VARCHAR);

-- 6. occupancy_key
SELECT @orphans = COUNT(*)
FROM gold.fact_policies fp
LEFT JOIN gold.dim_occupancy doc ON fp.occupancy_key = doc.occupancy_key
WHERE doc.occupancy_key IS NULL;
PRINT CASE WHEN @orphans = 0 THEN '[PASS]' ELSE '[FAIL]' END
    + ' occupancy_key orphans: ' + CAST(@orphans AS VARCHAR);

PRINT '';
PRINT 'REFERENTIAL INTEGRITY VALIDATION COMPLETE';
PRINT '=========================================';
GO
