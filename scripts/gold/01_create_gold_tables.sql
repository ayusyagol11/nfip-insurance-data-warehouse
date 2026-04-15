-- ================================================================
-- Script:    01_create_gold_tables.sql
-- Layer:     Gold
-- Purpose:   Create star schema — fact and dimension tables for the
--            NFIP insurance data warehouse
-- Author:    Aayush Yagol
-- Execute:   ./scripts/run_sql.sh scripts/gold/01_create_gold_tables.sql
-- ================================================================

USE NfipInsuranceWarehouse;
GO

-- ================================================================
-- DROP in reverse dependency order: facts first, then dims
-- ================================================================

DROP TABLE IF EXISTS gold.fact_claims;
DROP TABLE IF EXISTS gold.fact_policies;
DROP TABLE IF EXISTS gold.dim_date;
DROP TABLE IF EXISTS gold.dim_location;
DROP TABLE IF EXISTS gold.dim_flood_zone;
DROP TABLE IF EXISTS gold.dim_building_type;
DROP TABLE IF EXISTS gold.dim_occupancy;
GO

-- ================================================================
-- DIMENSION TABLES
-- ================================================================

-- ----------------------------------------------------------------
-- dim_date
-- ----------------------------------------------------------------
CREATE TABLE gold.dim_date (
    date_key            INT             NOT NULL PRIMARY KEY,
    full_date           DATE            NOT NULL,
    year                INT             NOT NULL,
    quarter             INT             NOT NULL,
    month               INT             NOT NULL,
    month_name          VARCHAR(20)     NOT NULL,
    fiscal_year         INT             NOT NULL,
    fiscal_quarter      INT             NOT NULL,
    day_of_week         VARCHAR(10)     NOT NULL,
    is_weekend          BIT             NOT NULL
);
GO

PRINT 'Table gold.dim_date created.';

-- ----------------------------------------------------------------
-- dim_location
-- ----------------------------------------------------------------
CREATE TABLE gold.dim_location (
    location_key        INT             NOT NULL PRIMARY KEY,
    state_abbrev        CHAR(2),
    state_name          VARCHAR(50),
    county_fips         VARCHAR(5),
    fema_region         INT
);
GO

PRINT 'Table gold.dim_location created.';

-- ----------------------------------------------------------------
-- dim_flood_zone
-- ----------------------------------------------------------------
CREATE TABLE gold.dim_flood_zone (
    flood_zone_key          INT             NOT NULL PRIMARY KEY,
    flood_zone_code         VARCHAR(20),
    zone_category           VARCHAR(10),
    zone_description        VARCHAR(100),
    is_special_flood_hazard BIT
);
GO

PRINT 'Table gold.dim_flood_zone created.';

-- ----------------------------------------------------------------
-- dim_building_type
-- ----------------------------------------------------------------
CREATE TABLE gold.dim_building_type (
    building_type_key       INT             NOT NULL PRIMARY KEY,
    construction_class      VARCHAR(50),
    number_of_floors        VARCHAR(10),
    year_built_band         VARCHAR(20),
    elevated_flag           BIT,
    basement_type           VARCHAR(10)
);
GO

PRINT 'Table gold.dim_building_type created.';

-- ----------------------------------------------------------------
-- dim_occupancy
-- ----------------------------------------------------------------
CREATE TABLE gold.dim_occupancy (
    occupancy_key       INT             NOT NULL PRIMARY KEY,
    occupancy_type      VARCHAR(50),
    residential_flag    BIT
);
GO

PRINT 'Table gold.dim_occupancy created.';

-- ================================================================
-- FACT TABLES
-- ================================================================

-- ----------------------------------------------------------------
-- fact_claims
-- ----------------------------------------------------------------
CREATE TABLE gold.fact_claims (
    claim_sk                    INT IDENTITY(1,1)   NOT NULL PRIMARY KEY,
    date_key                    INT                 NOT NULL,
    location_key                INT                 NOT NULL,
    flood_zone_key              INT                 NOT NULL,
    building_type_key           INT                 NOT NULL,
    occupancy_key               INT                 NOT NULL,
    amount_paid_building        DECIMAL(18,2),
    amount_paid_contents        DECIMAL(18,2),
    amount_paid_total           DECIMAL(18,2),
    total_building_coverage     DECIMAL(18,2),
    total_contents_coverage     DECIMAL(18,2),
    cause_of_damage             VARCHAR(100),
    year_of_loss                INT,

    CONSTRAINT FK_fact_claims_date
        FOREIGN KEY (date_key) REFERENCES gold.dim_date(date_key),
    CONSTRAINT FK_fact_claims_location
        FOREIGN KEY (location_key) REFERENCES gold.dim_location(location_key),
    CONSTRAINT FK_fact_claims_flood_zone
        FOREIGN KEY (flood_zone_key) REFERENCES gold.dim_flood_zone(flood_zone_key),
    CONSTRAINT FK_fact_claims_building_type
        FOREIGN KEY (building_type_key) REFERENCES gold.dim_building_type(building_type_key),
    CONSTRAINT FK_fact_claims_occupancy
        FOREIGN KEY (occupancy_key) REFERENCES gold.dim_occupancy(occupancy_key)
);
GO

PRINT 'Table gold.fact_claims created.';

-- ----------------------------------------------------------------
-- fact_policies
-- ----------------------------------------------------------------
CREATE TABLE gold.fact_policies (
    policy_sk                   INT IDENTITY(1,1)   NOT NULL PRIMARY KEY,
    effective_date_key          INT                 NOT NULL,
    termination_date_key        INT                 NOT NULL,
    location_key                INT                 NOT NULL,
    flood_zone_key              INT                 NOT NULL,
    building_type_key           INT                 NOT NULL,
    occupancy_key               INT                 NOT NULL,
    total_premium               DECIMAL(18,2),
    deductible_amount           DECIMAL(18,2),
    building_coverage           DECIMAL(18,2),
    contents_coverage           DECIMAL(18,2),
    exposure                    DECIMAL(8,6),
    crs_class                   VARCHAR(10),
    policy_count                INT                 DEFAULT 1,

    CONSTRAINT FK_fact_policies_eff_date
        FOREIGN KEY (effective_date_key) REFERENCES gold.dim_date(date_key),
    CONSTRAINT FK_fact_policies_term_date
        FOREIGN KEY (termination_date_key) REFERENCES gold.dim_date(date_key),
    CONSTRAINT FK_fact_policies_location
        FOREIGN KEY (location_key) REFERENCES gold.dim_location(location_key),
    CONSTRAINT FK_fact_policies_flood_zone
        FOREIGN KEY (flood_zone_key) REFERENCES gold.dim_flood_zone(flood_zone_key),
    CONSTRAINT FK_fact_policies_building_type
        FOREIGN KEY (building_type_key) REFERENCES gold.dim_building_type(building_type_key),
    CONSTRAINT FK_fact_policies_occupancy
        FOREIGN KEY (occupancy_key) REFERENCES gold.dim_occupancy(occupancy_key)
);
GO

PRINT 'Table gold.fact_policies created.';
PRINT 'All Gold tables created with FK constraints.';
