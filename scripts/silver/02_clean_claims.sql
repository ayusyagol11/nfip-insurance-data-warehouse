-- ================================================================
-- Script:    02_clean_claims.sql
-- Layer:     Silver
-- Purpose:   Clean, type-cast, deduplicate, and enrich claims data
--            from Bronze into Silver
-- Author:    Aayush Yagol
-- Execute:   ./scripts/run_sql.sh scripts/silver/02_clean_claims.sql
-- ================================================================

USE NfipInsuranceWarehouse;
GO

TRUNCATE TABLE silver.nfip_claims_cleaned;
PRINT 'Truncated silver.nfip_claims_cleaned';

INSERT INTO silver.nfip_claims_cleaned (
    dateOfLoss,
    state,
    countyCode,
    floodZone,
    amountPaidOnBuildingClaim,
    amountPaidOnContentsClaim,
    amountPaidTotal,
    totalBuildingInsuranceCoverage,
    totalContentsInsuranceCoverage,
    occupancyType,
    yearOfLoss,
    causeOfDamage,
    ratedFloodZone,
    primaryResidence,
    yearBuilt,
    numberOfFloors,
    elevatedBuildingIndicator,
    basementEnclosureCrawlspaceType,
    accident_year,
    zone_category,
    is_special_flood_hazard
)
SELECT
    -- Core dimensions
    TRY_CAST(b.dateOfLoss AS DATE)                                      AS dateOfLoss,
    LTRIM(RTRIM(b.state))                                               AS state,
    LTRIM(RTRIM(b.countyCode))                                          AS countyCode,
    LTRIM(RTRIM(COALESCE(b.ratedFloodZone, b.floodZoneCurrent)))        AS floodZone,

    -- Claim amounts (COALESCE nulls to 0)
    COALESCE(TRY_CAST(b.amountPaidOnBuildingClaim AS DECIMAL(18,2)), 0) AS amountPaidOnBuildingClaim,
    COALESCE(TRY_CAST(b.amountPaidOnContentsClaim AS DECIMAL(18,2)), 0) AS amountPaidOnContentsClaim,
    COALESCE(TRY_CAST(b.amountPaidOnBuildingClaim AS DECIMAL(18,2)), 0)
        + COALESCE(TRY_CAST(b.amountPaidOnContentsClaim AS DECIMAL(18,2)), 0)
                                                                        AS amountPaidTotal,

    -- Coverage amounts
    COALESCE(TRY_CAST(b.totalBuildingInsuranceCoverage AS DECIMAL(18,2)), 0)
                                                                        AS totalBuildingInsuranceCoverage,
    COALESCE(TRY_CAST(b.totalContentsInsuranceCoverage AS DECIMAL(18,2)), 0)
                                                                        AS totalContentsInsuranceCoverage,

    -- Policy/property attributes
    LTRIM(RTRIM(b.occupancyType))                                       AS occupancyType,
    TRY_CAST(b.yearOfLoss AS INT)                                       AS yearOfLoss,
    LTRIM(RTRIM(b.causeOfDamage))                                       AS causeOfDamage,
    LTRIM(RTRIM(b.ratedFloodZone))                                      AS ratedFloodZone,
    LTRIM(RTRIM(b.primaryResidenceIndicator))                           AS primaryResidence,
    LTRIM(RTRIM(b.originalConstructionDate))                            AS yearBuilt,
    LTRIM(RTRIM(b.numberOfFloorsInTheInsuredBuilding))                  AS numberOfFloors,
    LTRIM(RTRIM(b.elevatedBuildingIndicator))                           AS elevatedBuildingIndicator,
    LTRIM(RTRIM(b.basementEnclosureCrawlspaceType))                     AS basementEnclosureCrawlspaceType,

    -- Derived: accident_year
    YEAR(TRY_CAST(b.dateOfLoss AS DATE))                                AS accident_year,

    -- Derived: zone_category (from the resolved floodZone)
    CASE
        WHEN COALESCE(b.ratedFloodZone, b.floodZoneCurrent) LIKE 'V%'  THEN 'V'
        WHEN COALESCE(b.ratedFloodZone, b.floodZoneCurrent) LIKE 'A%'  THEN 'A'
        WHEN COALESCE(b.ratedFloodZone, b.floodZoneCurrent) IN ('B','C','X')
          OR COALESCE(b.ratedFloodZone, b.floodZoneCurrent) LIKE 'X%'
          OR COALESCE(b.ratedFloodZone, b.floodZoneCurrent) LIKE 'B%'
          OR COALESCE(b.ratedFloodZone, b.floodZoneCurrent) LIKE 'C%'  THEN 'X'
        WHEN COALESCE(b.ratedFloodZone, b.floodZoneCurrent) = 'D'      THEN 'D'
        ELSE 'Unknown'
    END                                                                 AS zone_category,

    -- Derived: is_special_flood_hazard
    CASE
        WHEN COALESCE(b.ratedFloodZone, b.floodZoneCurrent) LIKE 'V%'  THEN 1
        WHEN COALESCE(b.ratedFloodZone, b.floodZoneCurrent) LIKE 'A%'  THEN 1
        ELSE 0
    END                                                                 AS is_special_flood_hazard

FROM (
    -- Deduplicate: keep the most recent ingestion per unique claim
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY state, countyCode, dateOfLoss,
                         amountPaidOnBuildingClaim, amountPaidOnContentsClaim
            ORDER BY ingestion_timestamp DESC
        ) AS rn
    FROM bronze.nfip_claims_raw
) b
WHERE b.rn = 1
  AND TRY_CAST(b.dateOfLoss AS DATE) IS NOT NULL;

PRINT 'Silver claims loaded: ' + CAST(@@ROWCOUNT AS VARCHAR);
GO
