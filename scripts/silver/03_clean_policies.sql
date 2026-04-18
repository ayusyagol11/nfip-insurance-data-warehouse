-- ================================================================
-- Script:    03_clean_policies.sql
-- Layer:     Silver
-- Purpose:   Clean, type-cast, deduplicate, and enrich policies data
--            from Bronze into Silver
-- Author:    Aayush Yagol
-- Execute:   ./scripts/run_sql.sh scripts/silver/03_clean_policies.sql
-- ================================================================

USE NfipInsuranceWarehouse;
GO

TRUNCATE TABLE silver.nfip_policies_cleaned;
PRINT 'Truncated silver.nfip_policies_cleaned';

INSERT INTO silver.nfip_policies_cleaned (
    propertyState,
    countyCode,
    floodZone,
    totalPremium,
    deductibleAmount,
    buildingCoverage,
    contentsCoverage,
    policyEffectiveDate,
    policyTerminationDate,
    constructionClass,
    occupancyType,
    numberOfFloors,
    primaryResidenceIndicator,
    crsClassificationCode,
    ratedFloodZone,
    yearBuilt,
    elevatedBuildingIndicator,
    basementEnclosureCrawlspaceType,
    exposure,
    zone_category,
    is_special_flood_hazard
)
SELECT
    -- Core dimensions
    LTRIM(RTRIM(b.propertyState))                                       AS propertyState,
    LTRIM(RTRIM(b.countyCode))                                          AS countyCode,
    LTRIM(RTRIM(COALESCE(b.ratedFloodZone, b.floodZoneCurrent)))        AS floodZone,

    -- Financial columns
    COALESCE(TRY_CAST(b.totalInsurancePremiumOfThePolicy AS DECIMAL(18,2)), 0)
                                                                        AS totalPremium,
    COALESCE(TRY_CAST(b.buildingDeductibleCode AS DECIMAL(18,2)), 0)    AS deductibleAmount,
    COALESCE(TRY_CAST(b.totalBuildingInsuranceCoverage AS DECIMAL(18,2)), 0)
                                                                        AS buildingCoverage,
    COALESCE(TRY_CAST(b.totalContentsInsuranceCoverage AS DECIMAL(18,2)), 0)
                                                                        AS contentsCoverage,

    -- Date columns
    TRY_CAST(b.policyEffectiveDate AS DATE)                             AS policyEffectiveDate,
    TRY_CAST(b.policyTerminationDate AS DATE)                           AS policyTerminationDate,

    -- Policy/property attributes
    LTRIM(RTRIM(b.construction))                                        AS constructionClass,
    LTRIM(RTRIM(b.occupancyType))                                       AS occupancyType,
    LTRIM(RTRIM(b.numberOfFloorsInInsuredBuilding))                     AS numberOfFloors,
    LTRIM(RTRIM(b.primaryResidenceIndicator))                           AS primaryResidenceIndicator,
    LTRIM(RTRIM(b.crsClassCode))                                        AS crsClassificationCode,
    LTRIM(RTRIM(b.ratedFloodZone))                                      AS ratedFloodZone,
    LEFT(LTRIM(RTRIM(b.originalConstructionDate)), 4)                   AS yearBuilt,
    LTRIM(RTRIM(b.elevatedBuildingIndicator))                           AS elevatedBuildingIndicator,
    LTRIM(RTRIM(b.basementEnclosureCrawlspaceType))                     AS basementEnclosureCrawlspaceType,

    -- Derived: exposure (policy term in years, capped 0-1)
    CASE
        WHEN TRY_CAST(b.policyEffectiveDate AS DATE) IS NULL
          OR TRY_CAST(b.policyTerminationDate AS DATE) IS NULL          THEN 0
        WHEN DATEDIFF(DAY,
                TRY_CAST(b.policyEffectiveDate AS DATE),
                TRY_CAST(b.policyTerminationDate AS DATE)
             ) / 365.25 > 1.0                                          THEN 1.0
        WHEN DATEDIFF(DAY,
                TRY_CAST(b.policyEffectiveDate AS DATE),
                TRY_CAST(b.policyTerminationDate AS DATE)
             ) / 365.25 < 0.0                                          THEN 0.0
        ELSE CAST(
                DATEDIFF(DAY,
                    TRY_CAST(b.policyEffectiveDate AS DATE),
                    TRY_CAST(b.policyTerminationDate AS DATE)
                ) / 365.25
             AS DECIMAL(8,6))
    END                                                                 AS exposure,

    -- Derived: zone_category
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
    -- Deduplicate: keep the most recent ingestion per unique policy
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY propertyState, countyCode, policyEffectiveDate,
                         totalInsurancePremiumOfThePolicy, totalBuildingInsuranceCoverage
            ORDER BY ingestion_timestamp DESC
        ) AS rn
    FROM bronze.nfip_policies_raw
) b
WHERE b.rn = 1
  AND TRY_CAST(b.policyEffectiveDate AS DATE) IS NOT NULL;

PRINT 'Silver policies loaded: ' + CAST(@@ROWCOUNT AS VARCHAR);
GO
