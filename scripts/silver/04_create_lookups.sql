-- ================================================================
-- Script:    04_create_lookups.sql
-- Layer:     Silver
-- Purpose:   Create reference/lookup tables for state info and
--            flood zone classification
-- Author:    Aayush Yagol
-- Execute:   ./scripts/run_sql.sh scripts/silver/04_create_lookups.sql
-- ================================================================

USE NfipInsuranceWarehouse;
GO

-- ----------------------------------------------------------------
-- State reference table
-- ----------------------------------------------------------------
DROP TABLE IF EXISTS silver.ref_state_info;
GO

CREATE TABLE silver.ref_state_info (
    state_abbrev    CHAR(2)         PRIMARY KEY,
    state_name      VARCHAR(50)     NOT NULL,
    fema_region     INT             NOT NULL
);
GO

INSERT INTO silver.ref_state_info (state_abbrev, state_name, fema_region)
VALUES
    ('FL', 'Florida',     4),
    ('LA', 'Louisiana',   6),
    ('TX', 'Texas',       6),
    ('NJ', 'New Jersey',  2),
    ('NY', 'New York',    2);

PRINT 'silver.ref_state_info populated: ' + CAST(@@ROWCOUNT AS VARCHAR) + ' rows';
GO

-- ----------------------------------------------------------------
-- Flood zone reference table
-- ----------------------------------------------------------------
DROP TABLE IF EXISTS silver.ref_flood_zone_info;
GO

CREATE TABLE silver.ref_flood_zone_info (
    zone_category           VARCHAR(10)     PRIMARY KEY,
    zone_description        VARCHAR(100)    NOT NULL,
    is_special_flood_hazard BIT             NOT NULL
);
GO

INSERT INTO silver.ref_flood_zone_info (zone_category, zone_description, is_special_flood_hazard)
VALUES
    ('A',       'High-risk 1% annual chance',               1),
    ('V',       'Coastal high-risk with wave action',       1),
    ('X',       'Moderate-to-low risk',                     0),
    ('D',       'Undetermined risk',                        0),
    ('Unknown', 'Zone not mapped',                          0);

PRINT 'silver.ref_flood_zone_info populated: ' + CAST(@@ROWCOUNT AS VARCHAR) + ' rows';
GO
