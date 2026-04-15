-- ================================================================
-- Script:    05_validate_silver.sql
-- Layer:     Silver
-- Purpose:   Validate Silver layer data quality — row counts,
--            null checks, range checks, zone distributions
-- Author:    Aayush Yagol
-- Execute:   ./scripts/run_sql.sh scripts/silver/05_validate_silver.sql
-- ================================================================

USE NfipInsuranceWarehouse;
GO

PRINT '================================================================';
PRINT 'SILVER LAYER VALIDATION';
PRINT '================================================================';
PRINT '';

-- ----------------------------------------------------------------
-- 1. Row count: Bronze vs Silver — Claims
-- ----------------------------------------------------------------
PRINT '--- Claims Row Count ---';

DECLARE @bronze_claims INT, @silver_claims INT;

SELECT @bronze_claims = COUNT(*) FROM bronze.nfip_claims_raw;
SELECT @silver_claims = COUNT(*) FROM silver.nfip_claims_cleaned;

PRINT 'Bronze claims:  ' + CAST(@bronze_claims AS VARCHAR);
PRINT 'Silver claims:  ' + CAST(@silver_claims AS VARCHAR);
PRINT 'Delta (dropped): ' + CAST(@bronze_claims - @silver_claims AS VARCHAR);
PRINT '';

-- ----------------------------------------------------------------
-- 2. Row count: Bronze vs Silver — Policies
-- ----------------------------------------------------------------
PRINT '--- Policies Row Count ---';

DECLARE @bronze_policies INT, @silver_policies INT;

SELECT @bronze_policies = COUNT(*) FROM bronze.nfip_policies_raw;
SELECT @silver_policies = COUNT(*) FROM silver.nfip_policies_cleaned;

PRINT 'Bronze policies:  ' + CAST(@bronze_policies AS VARCHAR);
PRINT 'Silver policies:  ' + CAST(@silver_policies AS VARCHAR);
PRINT 'Delta (dropped):  ' + CAST(@bronze_policies - @silver_policies AS VARCHAR);
PRINT '';

-- ----------------------------------------------------------------
-- 3. NULL check on critical Silver claims fields
-- ----------------------------------------------------------------
PRINT '--- Silver Claims NULL Check (critical fields, should be 0) ---';

SELECT
    'claims_null_check' AS check_name,
    SUM(CASE WHEN dateOfLoss IS NULL THEN 1 ELSE 0 END)            AS null_dateOfLoss,
    SUM(CASE WHEN state IS NULL THEN 1 ELSE 0 END)                 AS null_state,
    SUM(CASE WHEN amountPaidTotal IS NULL THEN 1 ELSE 0 END)       AS null_amountPaidTotal,
    SUM(CASE WHEN yearOfLoss IS NULL THEN 1 ELSE 0 END)            AS null_yearOfLoss,
    SUM(CASE WHEN accident_year IS NULL THEN 1 ELSE 0 END)         AS null_accident_year
FROM silver.nfip_claims_cleaned;

-- ----------------------------------------------------------------
-- 4. NULL check on critical Silver policies fields
-- ----------------------------------------------------------------
PRINT '--- Silver Policies NULL Check (critical fields, should be 0) ---';

SELECT
    'policies_null_check' AS check_name,
    SUM(CASE WHEN propertyState IS NULL THEN 1 ELSE 0 END)         AS null_propertyState,
    SUM(CASE WHEN policyEffectiveDate IS NULL THEN 1 ELSE 0 END)   AS null_effectiveDate,
    SUM(CASE WHEN totalPremium IS NULL THEN 1 ELSE 0 END)          AS null_totalPremium,
    SUM(CASE WHEN buildingCoverage IS NULL THEN 1 ELSE 0 END)      AS null_buildingCoverage
FROM silver.nfip_policies_cleaned;

-- ----------------------------------------------------------------
-- 5. Negative amount check — Claims
-- ----------------------------------------------------------------
PRINT '--- Negative Amounts in Silver Claims ---';

SELECT
    'negative_amounts' AS check_name,
    SUM(CASE WHEN amountPaidOnBuildingClaim < 0 THEN 1 ELSE 0 END) AS neg_building_paid,
    SUM(CASE WHEN amountPaidOnContentsClaim < 0 THEN 1 ELSE 0 END) AS neg_contents_paid,
    SUM(CASE WHEN amountPaidTotal < 0 THEN 1 ELSE 0 END)           AS neg_total_paid
FROM silver.nfip_claims_cleaned;

-- ----------------------------------------------------------------
-- 6. Exposure range check — Policies (should be BETWEEN 0 AND 1)
-- ----------------------------------------------------------------
PRINT '--- Exposure Range Check (should be 0) ---';

SELECT
    'exposure_range' AS check_name,
    SUM(CASE WHEN exposure < 0 OR exposure > 1 THEN 1 ELSE 0 END)  AS out_of_range,
    MIN(exposure) AS min_exposure,
    MAX(exposure) AS max_exposure,
    AVG(exposure) AS avg_exposure
FROM silver.nfip_policies_cleaned;

-- ----------------------------------------------------------------
-- 7. Zone category distribution — Claims
-- ----------------------------------------------------------------
PRINT '--- Zone Category Distribution (Claims) ---';

SELECT
    zone_category,
    is_special_flood_hazard,
    COUNT(*)                                                        AS claim_count,
    CAST(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER() AS DECIMAL(5,2))  AS pct
FROM silver.nfip_claims_cleaned
GROUP BY zone_category, is_special_flood_hazard
ORDER BY claim_count DESC;

-- ----------------------------------------------------------------
-- 8. Zone category distribution — Policies
-- ----------------------------------------------------------------
PRINT '--- Zone Category Distribution (Policies) ---';

SELECT
    zone_category,
    is_special_flood_hazard,
    COUNT(*)                                                        AS policy_count,
    CAST(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER() AS DECIMAL(5,2))  AS pct
FROM silver.nfip_policies_cleaned
GROUP BY zone_category, is_special_flood_hazard
ORDER BY policy_count DESC;

PRINT '';
PRINT '================================================================';
PRINT 'SILVER VALIDATION COMPLETE';
PRINT '================================================================';
GO
