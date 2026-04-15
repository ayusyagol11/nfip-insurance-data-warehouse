-- ================================================================
-- Script:    01_create_silver_tables.sql
-- Layer:     Silver
-- Purpose:   Create cleaned, typed Silver tables for claims and policies
-- Author:    Aayush Yagol
-- Execute:   ./scripts/run_sql.sh scripts/silver/01_create_silver_tables.sql
-- ================================================================

USE NfipInsuranceWarehouse;
GO

-- ----------------------------------------------------------------
-- Claims cleaned table
-- ----------------------------------------------------------------
DROP TABLE IF EXISTS silver.nfip_claims_cleaned;
GO

CREATE TABLE silver.nfip_claims_cleaned (
    -- Core dimensions
    dateOfLoss                      DATE,
    state                           VARCHAR(2),
    countyCode                      VARCHAR(5),
    floodZone                       VARCHAR(20),

    -- Claim amounts
    amountPaidOnBuildingClaim       DECIMAL(18,2),
    amountPaidOnContentsClaim       DECIMAL(18,2),
    amountPaidTotal                 DECIMAL(18,2),

    -- Coverage amounts
    totalBuildingInsuranceCoverage  DECIMAL(18,2),
    totalContentsInsuranceCoverage  DECIMAL(18,2),

    -- Policy/property attributes
    occupancyType                   VARCHAR(50),
    yearOfLoss                      INT,
    causeOfDamage                   VARCHAR(100),
    ratedFloodZone                  VARCHAR(20),
    primaryResidence                VARCHAR(10),
    yearBuilt                       VARCHAR(10),
    numberOfFloors                  VARCHAR(10),
    elevatedBuildingIndicator       VARCHAR(10),
    basementEnclosureCrawlspaceType VARCHAR(10),

    -- Derived columns
    accident_year                   INT,
    zone_category                   VARCHAR(10),
    is_special_flood_hazard         BIT
);
GO

PRINT 'Table silver.nfip_claims_cleaned created.';

-- ----------------------------------------------------------------
-- Policies cleaned table
-- ----------------------------------------------------------------
DROP TABLE IF EXISTS silver.nfip_policies_cleaned;
GO

CREATE TABLE silver.nfip_policies_cleaned (
    -- Core dimensions
    propertyState                   VARCHAR(2),
    countyCode                      VARCHAR(5),
    floodZone                       VARCHAR(20),

    -- Financial columns
    totalPremium                    DECIMAL(18,2),
    deductibleAmount                DECIMAL(18,2),
    buildingCoverage                DECIMAL(18,2),
    contentsCoverage                DECIMAL(18,2),

    -- Date columns
    policyEffectiveDate             DATE,
    policyTerminationDate           DATE,

    -- Policy/property attributes
    constructionClass               VARCHAR(50),
    occupancyType                   VARCHAR(50),
    numberOfFloors                  VARCHAR(10),
    primaryResidenceIndicator       VARCHAR(10),
    crsClassificationCode           VARCHAR(10),
    ratedFloodZone                  VARCHAR(20),
    yearBuilt                       VARCHAR(10),
    elevatedBuildingIndicator       VARCHAR(10),
    basementEnclosureCrawlspaceType VARCHAR(10),

    -- Derived columns
    exposure                        DECIMAL(8,6),
    zone_category                   VARCHAR(10),
    is_special_flood_hazard         BIT
);
GO

PRINT 'Table silver.nfip_policies_cleaned created.';
