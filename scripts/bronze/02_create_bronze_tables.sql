USE NfipInsuranceWarehouse;
GO

-- ================================================================
-- 02_create_bronze_tables.sql
-- Creates Bronze layer raw staging tables
-- All source columns are VARCHAR — no type casting at Bronze
-- Column names match the actual CSV headers from the OpenFEMA API
-- ================================================================

-- ----------------------------------------------------------------
-- Claims raw table
-- ----------------------------------------------------------------
DROP TABLE IF EXISTS bronze.nfip_claims_raw;
GO

CREATE TABLE bronze.nfip_claims_raw (
    -- Source columns (match CSV headers exactly)
    dateOfLoss                          VARCHAR(255),
    state                               VARCHAR(255),
    countyCode                          VARCHAR(255),
    floodZoneCurrent                    VARCHAR(255),
    amountPaidOnBuildingClaim           VARCHAR(255),
    amountPaidOnContentsClaim           VARCHAR(255),
    totalBuildingInsuranceCoverage      VARCHAR(255),
    totalContentsInsuranceCoverage      VARCHAR(255),
    occupancyType                       VARCHAR(255),
    yearOfLoss                          VARCHAR(255),
    causeOfDamage                       VARCHAR(255),
    ratedFloodZone                      VARCHAR(255),
    primaryResidenceIndicator           VARCHAR(255),
    originalConstructionDate            VARCHAR(255),
    numberOfFloorsInTheInsuredBuilding  VARCHAR(255),
    elevatedBuildingIndicator           VARCHAR(255),
    basementEnclosureCrawlspaceType     VARCHAR(255),

    -- Metadata columns
    batch_id                INT             DEFAULT 1,
    ingestion_timestamp     DATETIME2       DEFAULT GETDATE(),
    source_state            VARCHAR(2),
    source_api_endpoint     VARCHAR(200)    DEFAULT 'https://www.fema.gov/api/open/v2/FimaNfipClaims'
);
GO

PRINT 'Table bronze.nfip_claims_raw created.';

-- ----------------------------------------------------------------
-- Policies raw table
-- ----------------------------------------------------------------
DROP TABLE IF EXISTS bronze.nfip_policies_raw;
GO

CREATE TABLE bronze.nfip_policies_raw (
    -- Source columns (match CSV headers exactly)
    propertyState                       VARCHAR(255),
    countyCode                          VARCHAR(255),
    floodZoneCurrent                    VARCHAR(255),
    totalInsurancePremiumOfThePolicy    VARCHAR(255),
    buildingDeductibleCode              VARCHAR(255),
    totalBuildingInsuranceCoverage      VARCHAR(255),
    totalContentsInsuranceCoverage      VARCHAR(255),
    policyEffectiveDate                 VARCHAR(255),
    policyTerminationDate               VARCHAR(255),
    construction                        VARCHAR(255),
    occupancyType                       VARCHAR(255),
    numberOfFloorsInInsuredBuilding     VARCHAR(255),
    primaryResidenceIndicator           VARCHAR(255),
    crsClassCode                        VARCHAR(255),
    ratedFloodZone                      VARCHAR(255),
    originalConstructionDate            VARCHAR(255),
    elevatedBuildingIndicator           VARCHAR(255),
    basementEnclosureCrawlspaceType     VARCHAR(255),

    -- Metadata columns
    batch_id                INT             DEFAULT 1,
    ingestion_timestamp     DATETIME2       DEFAULT GETDATE(),
    source_state            VARCHAR(2),
    source_api_endpoint     VARCHAR(200)    DEFAULT 'https://www.fema.gov/api/open/v2/FimaNfipPolicies'
);
GO

PRINT 'Table bronze.nfip_policies_raw created.';
