-- ================================================================
-- Script:    test_business_rules.sql
-- Layer:     Tests
-- Purpose:   Validate insurance business rules — premium signs,
--            paid amount signs, exposure range, coverage caps,
--            date ranges, loss ratio sanity, zone distribution
-- Author:    Aayush Yagol
-- Execute:   ./scripts/run_sql.sh tests/test_business_rules.sql
-- ================================================================

USE NfipInsuranceWarehouse;
GO

PRINT '=========================================';
PRINT 'BUSINESS RULES VALIDATION';
PRINT '=========================================';
PRINT '';

DECLARE @fail_count INT;

-- ----------------------------------------------------------------
-- 1. Premium >= 0 check
-- ----------------------------------------------------------------
SELECT @fail_count = COUNT(*)
FROM gold.fact_policies
WHERE total_premium < 0;

PRINT CASE WHEN @fail_count = 0 THEN '[PASS]' ELSE '[FAIL]' END
    + ' Premium >= 0: ' + CAST(@fail_count AS VARCHAR) + ' negative premiums found';

-- ----------------------------------------------------------------
-- 2. Paid amounts >= 0 check
-- ----------------------------------------------------------------
SELECT @fail_count = COUNT(*)
FROM gold.fact_claims
WHERE amount_paid_total < 0;

PRINT CASE WHEN @fail_count = 0 THEN '[PASS]' ELSE '[FAIL]' END
    + ' Paid total >= 0: ' + CAST(@fail_count AS VARCHAR) + ' negative paid amounts found';

SELECT @fail_count = COUNT(*)
FROM gold.fact_claims
WHERE amount_paid_building < 0;

PRINT CASE WHEN @fail_count = 0 THEN '[PASS]' ELSE '[FAIL]' END
    + ' Paid building >= 0: ' + CAST(@fail_count AS VARCHAR) + ' negative building amounts found';

SELECT @fail_count = COUNT(*)
FROM gold.fact_claims
WHERE amount_paid_contents < 0;

PRINT CASE WHEN @fail_count = 0 THEN '[PASS]' ELSE '[FAIL]' END
    + ' Paid contents >= 0: ' + CAST(@fail_count AS VARCHAR) + ' negative contents amounts found';

-- ----------------------------------------------------------------
-- 3. Exposure BETWEEN 0 AND 1
-- ----------------------------------------------------------------
SELECT @fail_count = COUNT(*)
FROM gold.fact_policies
WHERE exposure < 0 OR exposure > 1;

PRINT CASE WHEN @fail_count = 0 THEN '[PASS]' ELSE '[FAIL]' END
    + ' Exposure in [0,1]: ' + CAST(@fail_count AS VARCHAR) + ' out-of-range values';

-- ----------------------------------------------------------------
-- 4. Paid > coverage check (known NFIP data issue — WARNING only)
-- ----------------------------------------------------------------
DECLARE @over_building INT, @over_contents INT;

SELECT @over_building = COUNT(*)
FROM gold.fact_claims
WHERE amount_paid_building > total_building_coverage
  AND total_building_coverage > 0;

SELECT @over_contents = COUNT(*)
FROM gold.fact_claims
WHERE amount_paid_contents > total_contents_coverage
  AND total_contents_coverage > 0;

PRINT '[WARNING] Paid exceeds building coverage: ' + CAST(@over_building AS VARCHAR)
    + ' claims (known NFIP data issue — may include ICC or supplemental payments)';
PRINT '[WARNING] Paid exceeds contents coverage: ' + CAST(@over_contents AS VARCHAR)
    + ' claims';

-- ----------------------------------------------------------------
-- 5. Loss date range: should be between 1978 and 2026
-- ----------------------------------------------------------------
DECLARE @min_date DATE, @max_date DATE;

SELECT
    @min_date = MIN(dd.full_date),
    @max_date = MAX(dd.full_date)
FROM gold.fact_claims fc
JOIN gold.dim_date dd ON fc.date_key = dd.date_key;

PRINT 'Loss date range: ' + CAST(@min_date AS VARCHAR) + ' to ' + CAST(@max_date AS VARCHAR);

IF YEAR(@min_date) >= 1978 AND YEAR(@max_date) <= 2026
    PRINT '[PASS] Date range within expected bounds (1978-2026)';
ELSE
    PRINT '[FAIL] Date range outside expected bounds';

-- ----------------------------------------------------------------
-- 6. Loss ratio sanity: flag any state-year > 10.0
-- ----------------------------------------------------------------
PRINT '';
PRINT '--- Loss Ratio Sanity (state-years with ratio > 10.0) ---';

DECLARE @extreme_ratios INT;

SELECT @extreme_ratios = COUNT(*)
FROM gold.vw_loss_ratio_by_state
WHERE loss_ratio > 10.0;

IF @extreme_ratios = 0
    PRINT '[PASS] No state-year combinations with loss ratio > 10.0';
ELSE
BEGIN
    PRINT '[WARNING] ' + CAST(@extreme_ratios AS VARCHAR)
        + ' state-year combinations with loss ratio > 10.0:';

    SELECT
        state_name,
        year,
        CAST(loss_ratio AS DECIMAL(10,2)) AS loss_ratio,
        total_claims_paid,
        total_premium
    FROM gold.vw_loss_ratio_by_state
    WHERE loss_ratio > 10.0
    ORDER BY loss_ratio DESC;
END;

-- ----------------------------------------------------------------
-- 7. Zone category distribution (manual review)
-- ----------------------------------------------------------------
PRINT '';
PRINT '--- Zone Category Distribution (Claims) ---';

SELECT
    dfz.zone_category,
    dfz.is_special_flood_hazard,
    COUNT(*)                                                    AS claim_count,
    CAST(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER() AS DECIMAL(5,2)) AS pct
FROM gold.fact_claims fc
JOIN gold.dim_flood_zone dfz ON fc.flood_zone_key = dfz.flood_zone_key
GROUP BY dfz.zone_category, dfz.is_special_flood_hazard
ORDER BY claim_count DESC;

PRINT '';
PRINT '--- Zone Category Distribution (Policies) ---';

SELECT
    dfz.zone_category,
    dfz.is_special_flood_hazard,
    COUNT(*)                                                    AS policy_count,
    CAST(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER() AS DECIMAL(5,2)) AS pct
FROM gold.fact_policies fp
JOIN gold.dim_flood_zone dfz ON fp.flood_zone_key = dfz.flood_zone_key
GROUP BY dfz.zone_category, dfz.is_special_flood_hazard
ORDER BY policy_count DESC;

PRINT '';
PRINT '=========================================';
PRINT 'TEST SUITE COMPLETE';
PRINT '=========================================';
GO
