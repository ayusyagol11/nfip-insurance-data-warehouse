-- ================================================================
-- Script:    08_load_fact_policies.sql
-- Layer:     Gold
-- Purpose:   Load fact_policies by joining Silver policies to all Gold
--            dimension tables. Unknown keys (-1) for unresolved lookups.
-- Author:    Aayush Yagol
-- Execute:   ./scripts/run_sql.sh scripts/gold/08_load_fact_policies.sql
-- ================================================================

USE NfipInsuranceWarehouse;
GO

-- Idempotent reload
TRUNCATE TABLE gold.fact_policies;
PRINT 'Truncated gold.fact_policies';

INSERT INTO gold.fact_policies (
    effective_date_key,
    termination_date_key,
    location_key,
    flood_zone_key,
    building_type_key,
    occupancy_key,
    total_premium,
    deductible_amount,
    building_coverage,
    contents_coverage,
    exposure,
    crs_class,
    policy_count
)
SELECT
    -- Date dimensions
    CAST(CONVERT(VARCHAR(8), p.policyEffectiveDate, 112) AS INT)    AS effective_date_key,
    CAST(CONVERT(VARCHAR(8), p.policyTerminationDate, 112) AS INT)  AS termination_date_key,

    -- Location dimension
    COALESCE(dl.location_key, -1)                                   AS location_key,

    -- Flood zone dimension
    COALESCE(dfz.flood_zone_key, -1)                                AS flood_zone_key,

    -- Building type dimension
    COALESCE(dbt.building_type_key, -1)                             AS building_type_key,

    -- Occupancy dimension
    COALESCE(doc.occupancy_key, -1)                                 AS occupancy_key,

    -- Measures
    p.totalPremium,
    p.deductibleAmount,
    p.buildingCoverage,
    p.contentsCoverage,
    p.exposure,
    p.crsClassificationCode,
    1                                                               AS policy_count

FROM silver.nfip_policies_cleaned p

-- Join dim_location
LEFT JOIN gold.dim_location dl
    ON dl.state_abbrev = p.propertyState
   AND dl.county_fips  = p.countyCode

-- Join dim_flood_zone
LEFT JOIN gold.dim_flood_zone dfz
    ON dfz.flood_zone_code = p.floodZone

-- Join dim_building_type (must replicate the same derivation used in 05_load_dim_building_type)
LEFT JOIN gold.dim_building_type dbt
    ON dbt.construction_class = COALESCE(p.constructionClass, 'Unknown')
   AND dbt.number_of_floors   = COALESCE(p.numberOfFloors, 'Unknown')
   AND dbt.year_built_band    = CASE
            WHEN TRY_CAST(LEFT(p.yearBuilt, 4) AS INT) IS NULL           THEN 'Unknown'
            WHEN TRY_CAST(LEFT(p.yearBuilt, 4) AS INT) < 1970            THEN 'Pre-1970'
            WHEN TRY_CAST(LEFT(p.yearBuilt, 4) AS INT) BETWEEN 1970 AND 1979 THEN '1970-1979'
            WHEN TRY_CAST(LEFT(p.yearBuilt, 4) AS INT) BETWEEN 1980 AND 1989 THEN '1980-1989'
            WHEN TRY_CAST(LEFT(p.yearBuilt, 4) AS INT) BETWEEN 1990 AND 1999 THEN '1990-1999'
            WHEN TRY_CAST(LEFT(p.yearBuilt, 4) AS INT) BETWEEN 2000 AND 2009 THEN '2000-2009'
            WHEN TRY_CAST(LEFT(p.yearBuilt, 4) AS INT) BETWEEN 2010 AND 2019 THEN '2010-2019'
            WHEN TRY_CAST(LEFT(p.yearBuilt, 4) AS INT) >= 2020               THEN '2020+'
            ELSE 'Unknown'
        END
   AND dbt.elevated_flag      = CASE WHEN UPPER(p.elevatedBuildingIndicator) = 'TRUE' THEN 1 ELSE 0 END
   AND dbt.basement_type      = COALESCE(p.basementEnclosureCrawlspaceType, 'Unknown')

-- Join dim_occupancy
LEFT JOIN gold.dim_occupancy doc
    ON doc.occupancy_type = COALESCE(p.occupancyType, 'Unknown');

DECLARE @row_count INT;
SET @row_count = @@ROWCOUNT;
PRINT 'gold.fact_policies loaded: ' + CAST(@row_count AS VARCHAR) + ' rows';
GO

-- ================================================================
-- Orphan validation: every FK should resolve (orphan count = 0)
-- ================================================================
PRINT '';
PRINT '--- Orphan Validation ---';

SELECT 'effective_date_key orphans' AS check_name,
    COUNT(*) AS orphan_count
FROM gold.fact_policies fp
LEFT JOIN gold.dim_date dd ON fp.effective_date_key = dd.date_key
WHERE dd.date_key IS NULL;

SELECT 'termination_date_key orphans' AS check_name,
    COUNT(*) AS orphan_count
FROM gold.fact_policies fp
LEFT JOIN gold.dim_date dd ON fp.termination_date_key = dd.date_key
WHERE dd.date_key IS NULL;

SELECT 'location_key orphans' AS check_name,
    COUNT(*) AS orphan_count
FROM gold.fact_policies fp
LEFT JOIN gold.dim_location dl ON fp.location_key = dl.location_key
WHERE dl.location_key IS NULL;

SELECT 'flood_zone_key orphans' AS check_name,
    COUNT(*) AS orphan_count
FROM gold.fact_policies fp
LEFT JOIN gold.dim_flood_zone dfz ON fp.flood_zone_key = dfz.flood_zone_key
WHERE dfz.flood_zone_key IS NULL;

SELECT 'building_type_key orphans' AS check_name,
    COUNT(*) AS orphan_count
FROM gold.fact_policies fp
LEFT JOIN gold.dim_building_type dbt ON fp.building_type_key = dbt.building_type_key
WHERE dbt.building_type_key IS NULL;

SELECT 'occupancy_key orphans' AS check_name,
    COUNT(*) AS orphan_count
FROM gold.fact_policies fp
LEFT JOIN gold.dim_occupancy doc ON fp.occupancy_key = doc.occupancy_key
WHERE doc.occupancy_key IS NULL;

PRINT 'Fact policies orphan validation complete.';
GO

-- ================================================================
-- Sanity check: Claims by state and year (2018+)
-- ================================================================
PRINT '';
PRINT '--- Sanity Check: Claims by State and Year (2018+) ---';

SELECT
    l.state_name,
    d.year,
    COUNT(DISTINCT fc.claim_sk)     AS claims,
    SUM(fc.amount_paid_total)       AS total_paid
FROM gold.fact_claims fc
JOIN gold.dim_location l ON fc.location_key = l.location_key
JOIN gold.dim_date d     ON fc.date_key     = d.date_key
WHERE d.year >= 2018
GROUP BY l.state_name, d.year
ORDER BY d.year DESC, l.state_name;

PRINT 'Sanity check complete.';
GO
