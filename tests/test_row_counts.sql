-- ================================================================
-- Script:    test_row_counts.sql
-- Layer:     Tests
-- Purpose:   Validate row counts across Bronze, Silver, and Gold
--            layers for both claims and policies pipelines
-- Author:    Aayush Yagol
-- Execute:   ./scripts/run_sql.sh tests/test_row_counts.sql
-- ================================================================

USE NfipInsuranceWarehouse;
GO

PRINT '=========================================';
PRINT 'ROW COUNT VALIDATION';
PRINT '=========================================';
PRINT '';

-- ----------------------------------------------------------------
-- Claims pipeline
-- ----------------------------------------------------------------
PRINT '--- Claims Pipeline ---';

DECLARE @bronze_claims INT, @silver_claims INT, @gold_claims INT;

SELECT @bronze_claims = COUNT(*) FROM bronze.nfip_claims_raw;
SELECT @silver_claims = COUNT(*) FROM silver.nfip_claims_cleaned;
SELECT @gold_claims   = COUNT(*) FROM gold.fact_claims;

PRINT 'Bronze claims:  ' + CAST(@bronze_claims AS VARCHAR);
PRINT 'Silver claims:  ' + CAST(@silver_claims AS VARCHAR);
PRINT 'Gold claims:    ' + CAST(@gold_claims AS VARCHAR);
PRINT 'Bronze → Silver delta (dedup/filter): ' + CAST(@bronze_claims - @silver_claims AS VARCHAR);
PRINT 'Silver → Gold delta (join loss):      ' + CAST(@silver_claims - @gold_claims AS VARCHAR);

IF @gold_claims > @silver_claims
    PRINT '[FAIL] Gold claims exceeds Silver — possible fan-out from dimension joins';
ELSE
    PRINT '[PASS] Gold claims <= Silver claims';

PRINT '';

-- ----------------------------------------------------------------
-- Policies pipeline
-- ----------------------------------------------------------------
PRINT '--- Policies Pipeline ---';

DECLARE @bronze_policies INT, @silver_policies INT, @gold_policies INT;

SELECT @bronze_policies = COUNT(*) FROM bronze.nfip_policies_raw;
SELECT @silver_policies = COUNT(*) FROM silver.nfip_policies_cleaned;
SELECT @gold_policies   = COUNT(*) FROM gold.fact_policies;

PRINT 'Bronze policies:  ' + CAST(@bronze_policies AS VARCHAR);
PRINT 'Silver policies:  ' + CAST(@silver_policies AS VARCHAR);
PRINT 'Gold policies:    ' + CAST(@gold_policies AS VARCHAR);
PRINT 'Bronze → Silver delta (dedup/filter): ' + CAST(@bronze_policies - @silver_policies AS VARCHAR);
PRINT 'Silver → Gold delta (join loss):      ' + CAST(@silver_policies - @gold_policies AS VARCHAR);

IF @gold_policies > @silver_policies
    PRINT '[FAIL] Gold policies exceeds Silver — possible fan-out from dimension joins';
ELSE
    PRINT '[PASS] Gold policies <= Silver policies';

PRINT '';

-- ----------------------------------------------------------------
-- Dimension tables
-- ----------------------------------------------------------------
PRINT '--- Dimension Row Counts ---';

DECLARE @dim_date INT, @dim_loc INT, @dim_fz INT, @dim_bt INT, @dim_occ INT;

SELECT @dim_date = COUNT(*) FROM gold.dim_date;
SELECT @dim_loc  = COUNT(*) FROM gold.dim_location;
SELECT @dim_fz   = COUNT(*) FROM gold.dim_flood_zone;
SELECT @dim_bt   = COUNT(*) FROM gold.dim_building_type;
SELECT @dim_occ  = COUNT(*) FROM gold.dim_occupancy;

PRINT 'dim_date:          ' + CAST(@dim_date AS VARCHAR);
PRINT 'dim_location:      ' + CAST(@dim_loc AS VARCHAR);
PRINT 'dim_flood_zone:    ' + CAST(@dim_fz AS VARCHAR);
PRINT 'dim_building_type: ' + CAST(@dim_bt AS VARCHAR);
PRINT 'dim_occupancy:     ' + CAST(@dim_occ AS VARCHAR);

PRINT '';
PRINT 'ROW COUNT VALIDATION COMPLETE';
PRINT '=========================================';
GO
