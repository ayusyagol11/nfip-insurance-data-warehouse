-- ================================================================
-- Script:    05_load_dim_building_type.sql
-- Layer:     Gold
-- Purpose:   Populate dim_building_type from distinct combinations of
--            construction class, floors, year-built band, elevated
--            flag, and basement type across Silver claims and policies
-- Author:    Aayush Yagol
-- Execute:   ./scripts/run_sql.sh scripts/gold/05_load_dim_building_type.sql
-- ================================================================

USE NfipInsuranceWarehouse;
GO

-- Clear and reload
DELETE FROM gold.dim_building_type;
PRINT 'Cleared gold.dim_building_type';

-- Insert Unknown row (key = -1)
INSERT INTO gold.dim_building_type (building_type_key, construction_class, number_of_floors, year_built_band, elevated_flag, basement_type)
VALUES (-1, 'Unknown', 'Unknown', 'Unknown', 0, 'Unknown');

-- Populate from distinct combos in Silver claims and policies
-- Claims do not have constructionClass — use 'N/A'
-- yearBuilt is grouped into decade bands
INSERT INTO gold.dim_building_type (building_type_key, construction_class, number_of_floors, year_built_band, elevated_flag, basement_type)
SELECT
    ROW_NUMBER() OVER (ORDER BY construction_class, number_of_floors, year_built_band, elevated_flag, basement_type)
        AS building_type_key,
    construction_class,
    number_of_floors,
    year_built_band,
    elevated_flag,
    basement_type
FROM (
    -- Claims building attributes
    SELECT DISTINCT
        'N/A'                                                               AS construction_class,
        COALESCE(numberOfFloors, 'Unknown')                                 AS number_of_floors,
        CASE
            WHEN TRY_CAST(LEFT(yearBuilt, 4) AS INT) IS NULL           THEN 'Unknown'
            WHEN TRY_CAST(LEFT(yearBuilt, 4) AS INT) < 1970            THEN 'Pre-1970'
            WHEN TRY_CAST(LEFT(yearBuilt, 4) AS INT) BETWEEN 1970 AND 1979 THEN '1970-1979'
            WHEN TRY_CAST(LEFT(yearBuilt, 4) AS INT) BETWEEN 1980 AND 1989 THEN '1980-1989'
            WHEN TRY_CAST(LEFT(yearBuilt, 4) AS INT) BETWEEN 1990 AND 1999 THEN '1990-1999'
            WHEN TRY_CAST(LEFT(yearBuilt, 4) AS INT) BETWEEN 2000 AND 2009 THEN '2000-2009'
            WHEN TRY_CAST(LEFT(yearBuilt, 4) AS INT) BETWEEN 2010 AND 2019 THEN '2010-2019'
            WHEN TRY_CAST(LEFT(yearBuilt, 4) AS INT) >= 2020               THEN '2020+'
            ELSE 'Unknown'
        END                                                                 AS year_built_band,
        CASE WHEN UPPER(elevatedBuildingIndicator) = 'TRUE' THEN 1 ELSE 0 END AS elevated_flag,
        COALESCE(basementEnclosureCrawlspaceType, 'Unknown')                AS basement_type
    FROM silver.nfip_claims_cleaned

    UNION

    -- Policies building attributes
    SELECT DISTINCT
        COALESCE(constructionClass, 'Unknown')                              AS construction_class,
        COALESCE(numberOfFloors, 'Unknown')                                 AS number_of_floors,
        CASE
            WHEN TRY_CAST(LEFT(yearBuilt, 4) AS INT) IS NULL           THEN 'Unknown'
            WHEN TRY_CAST(LEFT(yearBuilt, 4) AS INT) < 1970            THEN 'Pre-1970'
            WHEN TRY_CAST(LEFT(yearBuilt, 4) AS INT) BETWEEN 1970 AND 1979 THEN '1970-1979'
            WHEN TRY_CAST(LEFT(yearBuilt, 4) AS INT) BETWEEN 1980 AND 1989 THEN '1980-1989'
            WHEN TRY_CAST(LEFT(yearBuilt, 4) AS INT) BETWEEN 1990 AND 1999 THEN '1990-1999'
            WHEN TRY_CAST(LEFT(yearBuilt, 4) AS INT) BETWEEN 2000 AND 2009 THEN '2000-2009'
            WHEN TRY_CAST(LEFT(yearBuilt, 4) AS INT) BETWEEN 2010 AND 2019 THEN '2010-2019'
            WHEN TRY_CAST(LEFT(yearBuilt, 4) AS INT) >= 2020               THEN '2020+'
            ELSE 'Unknown'
        END                                                                 AS year_built_band,
        CASE WHEN UPPER(elevatedBuildingIndicator) = 'TRUE' THEN 1 ELSE 0 END AS elevated_flag,
        COALESCE(basementEnclosureCrawlspaceType, 'Unknown')                AS basement_type
    FROM silver.nfip_policies_cleaned
) bt;

-- Validation
DECLARE @row_count INT;
SELECT @row_count = COUNT(*) FROM gold.dim_building_type;
PRINT 'gold.dim_building_type loaded: ' + CAST(@row_count AS VARCHAR) + ' rows (including Unknown)';

DECLARE @null_keys INT;
SELECT @null_keys = COUNT(*) FROM gold.dim_building_type WHERE building_type_key IS NULL;
PRINT 'NULL building_type_key count: ' + CAST(@null_keys AS VARCHAR) + ' (should be 0)';

DECLARE @dup_keys INT;
SELECT @dup_keys = COUNT(*) FROM (
    SELECT building_type_key FROM gold.dim_building_type GROUP BY building_type_key HAVING COUNT(*) > 1
) d;
PRINT 'Duplicate building_type_key count: ' + CAST(@dup_keys AS VARCHAR) + ' (should be 0)';
GO
