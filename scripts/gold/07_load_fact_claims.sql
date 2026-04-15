-- ================================================================
-- Script:    07_load_fact_claims.sql
-- Layer:     Gold
-- Purpose:   Load fact_claims by joining Silver claims to all Gold
--            dimension tables. Unknown keys (-1) for unresolved lookups.
-- Author:    Aayush Yagol
-- Execute:   ./scripts/run_sql.sh scripts/gold/07_load_fact_claims.sql
-- ================================================================

USE NfipInsuranceWarehouse;
GO

-- Idempotent reload
TRUNCATE TABLE gold.fact_claims;
PRINT 'Truncated gold.fact_claims';

INSERT INTO gold.fact_claims (
    date_key,
    location_key,
    flood_zone_key,
    building_type_key,
    occupancy_key,
    amount_paid_building,
    amount_paid_contents,
    amount_paid_total,
    total_building_coverage,
    total_contents_coverage,
    cause_of_damage,
    year_of_loss
)
SELECT
    -- Date dimension
    CAST(CONVERT(VARCHAR(8), c.dateOfLoss, 112) AS INT)         AS date_key,

    -- Location dimension (COALESCE to Unknown = -1)
    COALESCE(dl.location_key, -1)                               AS location_key,

    -- Flood zone dimension
    COALESCE(dfz.flood_zone_key, -1)                            AS flood_zone_key,

    -- Building type dimension
    COALESCE(dbt.building_type_key, -1)                         AS building_type_key,

    -- Occupancy dimension
    COALESCE(doc.occupancy_key, -1)                             AS occupancy_key,

    -- Measures
    c.amountPaidOnBuildingClaim,
    c.amountPaidOnContentsClaim,
    c.amountPaidTotal,
    c.totalBuildingInsuranceCoverage,
    c.totalContentsInsuranceCoverage,
    c.causeOfDamage,
    c.yearOfLoss

FROM silver.nfip_claims_cleaned c

-- Join dim_location
LEFT JOIN gold.dim_location dl
    ON dl.state_abbrev = c.state
   AND dl.county_fips  = c.countyCode

-- Join dim_flood_zone
LEFT JOIN gold.dim_flood_zone dfz
    ON dfz.flood_zone_code = c.floodZone

-- Join dim_building_type (must replicate the same derivation used in 05_load_dim_building_type)
LEFT JOIN gold.dim_building_type dbt
    ON dbt.construction_class = 'N/A'
   AND dbt.number_of_floors   = COALESCE(c.numberOfFloors, 'Unknown')
   AND dbt.year_built_band    = CASE
            WHEN TRY_CAST(LEFT(c.yearBuilt, 4) AS INT) IS NULL           THEN 'Unknown'
            WHEN TRY_CAST(LEFT(c.yearBuilt, 4) AS INT) < 1970            THEN 'Pre-1970'
            WHEN TRY_CAST(LEFT(c.yearBuilt, 4) AS INT) BETWEEN 1970 AND 1979 THEN '1970-1979'
            WHEN TRY_CAST(LEFT(c.yearBuilt, 4) AS INT) BETWEEN 1980 AND 1989 THEN '1980-1989'
            WHEN TRY_CAST(LEFT(c.yearBuilt, 4) AS INT) BETWEEN 1990 AND 1999 THEN '1990-1999'
            WHEN TRY_CAST(LEFT(c.yearBuilt, 4) AS INT) BETWEEN 2000 AND 2009 THEN '2000-2009'
            WHEN TRY_CAST(LEFT(c.yearBuilt, 4) AS INT) BETWEEN 2010 AND 2019 THEN '2010-2019'
            WHEN TRY_CAST(LEFT(c.yearBuilt, 4) AS INT) >= 2020               THEN '2020+'
            ELSE 'Unknown'
        END
   AND dbt.elevated_flag      = CASE WHEN UPPER(c.elevatedBuildingIndicator) = 'TRUE' THEN 1 ELSE 0 END
   AND dbt.basement_type      = COALESCE(c.basementEnclosureCrawlspaceType, 'Unknown')

-- Join dim_occupancy
LEFT JOIN gold.dim_occupancy doc
    ON doc.occupancy_type = COALESCE(c.occupancyType, 'Unknown');

DECLARE @row_count INT;
SET @row_count = @@ROWCOUNT;
PRINT 'gold.fact_claims loaded: ' + CAST(@row_count AS VARCHAR) + ' rows';
GO

-- ================================================================
-- Orphan validation: every FK should resolve (orphan count = 0)
-- ================================================================
PRINT '';
PRINT '--- Orphan Validation ---';

SELECT 'date_key orphans' AS check_name,
    COUNT(*) AS orphan_count
FROM gold.fact_claims fc
LEFT JOIN gold.dim_date dd ON fc.date_key = dd.date_key
WHERE dd.date_key IS NULL;

SELECT 'location_key orphans' AS check_name,
    COUNT(*) AS orphan_count
FROM gold.fact_claims fc
LEFT JOIN gold.dim_location dl ON fc.location_key = dl.location_key
WHERE dl.location_key IS NULL;

SELECT 'flood_zone_key orphans' AS check_name,
    COUNT(*) AS orphan_count
FROM gold.fact_claims fc
LEFT JOIN gold.dim_flood_zone dfz ON fc.flood_zone_key = dfz.flood_zone_key
WHERE dfz.flood_zone_key IS NULL;

SELECT 'building_type_key orphans' AS check_name,
    COUNT(*) AS orphan_count
FROM gold.fact_claims fc
LEFT JOIN gold.dim_building_type dbt ON fc.building_type_key = dbt.building_type_key
WHERE dbt.building_type_key IS NULL;

SELECT 'occupancy_key orphans' AS check_name,
    COUNT(*) AS orphan_count
FROM gold.fact_claims fc
LEFT JOIN gold.dim_occupancy doc ON fc.occupancy_key = doc.occupancy_key
WHERE doc.occupancy_key IS NULL;

PRINT 'Fact claims orphan validation complete.';
GO
