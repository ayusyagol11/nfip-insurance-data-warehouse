-- ================================================================
-- Script:    09_create_analytics_views.sql
-- Layer:     Gold
-- Purpose:   Create 7 insurance KPI analytics views answering key
--            business questions: loss ratio, frequency/severity,
--            large loss concentration, premium adequacy, claims
--            development, and portfolio summary
-- Author:    Aayush Yagol
-- Execute:   ./scripts/run_sql.sh scripts/gold/09_create_analytics_views.sql
-- ================================================================

USE NfipInsuranceWarehouse;
GO

-- ================================================================
-- View 1: Loss Ratio by State and Year
-- Answers: Which states are profitable vs. unprofitable by year?
-- loss_ratio > 1.0 means claims exceed premiums collected
-- ================================================================
CREATE OR ALTER VIEW gold.vw_loss_ratio_by_state AS
WITH claims_agg AS (
    SELECT
        fc.location_key,
        dc.year,
        SUM(fc.amount_paid_total) AS total_claims_paid
    FROM gold.fact_claims fc
    JOIN gold.dim_date dc ON fc.date_key = dc.date_key
    GROUP BY fc.location_key, dc.year
),
policies_agg AS (
    SELECT
        fp.location_key,
        dp.year,
        SUM(fp.total_premium)   AS total_premium,
        SUM(fp.exposure)        AS total_exposure
    FROM gold.fact_policies fp
    JOIN gold.dim_date dp ON fp.effective_date_key = dp.date_key
    GROUP BY fp.location_key, dp.year
)
SELECT
    l.state_name,
    COALESCE(c.year, p.year)                                        AS year,
    COALESCE(c.total_claims_paid, 0)                                AS total_claims_paid,
    COALESCE(p.total_premium, 0)                                    AS total_premium,
    COALESCE(p.total_exposure, 0)                                   AS total_exposure,
    COALESCE(c.total_claims_paid, 0)
        / NULLIF(COALESCE(p.total_premium, 0), 0)                  AS loss_ratio
FROM claims_agg c
FULL OUTER JOIN policies_agg p
    ON c.location_key = p.location_key AND c.year = p.year
JOIN gold.dim_location l
    ON l.location_key = COALESCE(c.location_key, p.location_key);
GO

PRINT 'View gold.vw_loss_ratio_by_state created.';

-- ================================================================
-- View 2: Claims Frequency and Severity by Flood Zone and Year
-- frequency  = claim count / earned exposure
-- severity   = total paid / claim count
-- ================================================================
CREATE OR ALTER VIEW gold.vw_claims_frequency_severity AS
WITH claims_agg AS (
    SELECT
        dfz.zone_category,
        dc.year,
        COUNT(*)                    AS claim_count,
        SUM(fc.amount_paid_total)   AS total_paid
    FROM gold.fact_claims fc
    JOIN gold.dim_date dc       ON fc.date_key      = dc.date_key
    JOIN gold.dim_flood_zone dfz ON fc.flood_zone_key = dfz.flood_zone_key
    GROUP BY dfz.zone_category, dc.year
),
exposure_agg AS (
    SELECT
        dfz.zone_category,
        dp.year,
        SUM(fp.exposure) AS total_exposure
    FROM gold.fact_policies fp
    JOIN gold.dim_date dp        ON fp.effective_date_key = dp.date_key
    JOIN gold.dim_flood_zone dfz ON fp.flood_zone_key     = dfz.flood_zone_key
    GROUP BY dfz.zone_category, dp.year
)
SELECT
    COALESCE(c.zone_category, e.zone_category)                      AS zone_category,
    COALESCE(c.year, e.year)                                        AS year,
    COALESCE(c.claim_count, 0)                                      AS claim_count,
    COALESCE(c.total_paid, 0)                                       AS total_paid,
    COALESCE(e.total_exposure, 0)                                   AS total_exposure,
    CAST(COALESCE(c.claim_count, 0) AS FLOAT)
        / NULLIF(COALESCE(e.total_exposure, 0), 0)                  AS frequency,
    COALESCE(c.total_paid, 0)
        / NULLIF(COALESCE(c.claim_count, 0), 0)                    AS avg_severity
FROM claims_agg c
FULL OUTER JOIN exposure_agg e
    ON c.zone_category = e.zone_category AND c.year = e.year;
GO

PRINT 'View gold.vw_claims_frequency_severity created.';

-- ================================================================
-- View 3: Large Loss Concentration by State
-- Flags claims above the 95th percentile of amount_paid_total
-- Shows how large losses concentrate geographically
-- ================================================================
CREATE OR ALTER VIEW gold.vw_large_loss_concentration AS
WITH threshold AS (
    SELECT
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY amount_paid_total)
            OVER () AS p95_threshold
    FROM gold.fact_claims
    WHERE amount_paid_total > 0
),
p95 AS (
    SELECT DISTINCT p95_threshold FROM threshold
),
flagged AS (
    SELECT
        fc.claim_sk,
        fc.location_key,
        fc.amount_paid_total,
        CASE WHEN fc.amount_paid_total >= (SELECT p95_threshold FROM p95) THEN 1 ELSE 0 END AS is_large_loss
    FROM gold.fact_claims fc
)
SELECT
    l.state_name,
    SUM(f.is_large_loss)                                            AS large_loss_count,
    SUM(CASE WHEN f.is_large_loss = 1
             THEN f.amount_paid_total ELSE 0 END)                  AS large_loss_total,
    SUM(f.amount_paid_total)                                        AS total_paid,
    CAST(SUM(CASE WHEN f.is_large_loss = 1
                  THEN f.amount_paid_total ELSE 0 END) AS FLOAT)
        / NULLIF(SUM(f.amount_paid_total), 0) * 100                AS pct_of_total_paid,
    (SELECT p95_threshold FROM p95)                                 AS p95_threshold
FROM flagged f
JOIN gold.dim_location l ON f.location_key = l.location_key
GROUP BY l.state_name;
GO

PRINT 'View gold.vw_large_loss_concentration created.';

-- ================================================================
-- View 4: Severity by Flood Zone Category
-- avg severity, median severity, building vs contents split
-- ================================================================
CREATE OR ALTER VIEW gold.vw_severity_by_flood_zone AS
SELECT
    dfz.zone_category,
    dfz.zone_description,
    COUNT(*)                                                        AS claim_count,
    AVG(fc.amount_paid_total)                                       AS avg_severity,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY fc.amount_paid_total)
        OVER (PARTITION BY dfz.zone_category)                       AS median_severity,
    AVG(fc.amount_paid_building)                                    AS avg_building_paid,
    AVG(fc.amount_paid_contents)                                    AS avg_contents_paid,
    SUM(fc.amount_paid_building)
        / NULLIF(SUM(fc.amount_paid_total), 0)                     AS building_share,
    SUM(fc.amount_paid_contents)
        / NULLIF(SUM(fc.amount_paid_total), 0)                     AS contents_share
FROM gold.fact_claims fc
JOIN gold.dim_flood_zone dfz ON fc.flood_zone_key = dfz.flood_zone_key
WHERE fc.amount_paid_total > 0
GROUP BY dfz.zone_category, dfz.zone_description,
         PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY fc.amount_paid_total)
             OVER (PARTITION BY dfz.zone_category);
GO

PRINT 'View gold.vw_severity_by_flood_zone created.';

-- ================================================================
-- View 5: Premium Adequacy by Occupancy and Construction
-- pure_premium  = total claims / total exposure
-- avg_premium   = average premium charged
-- adequacy_flag = whether avg premium covers the pure premium
-- ================================================================
CREATE OR ALTER VIEW gold.vw_premium_adequacy AS
WITH claims_agg AS (
    SELECT
        doc.occupancy_type,
        dbt.construction_class,
        SUM(fc.amount_paid_total) AS total_claims
    FROM gold.fact_claims fc
    JOIN gold.dim_occupancy doc      ON fc.occupancy_key     = doc.occupancy_key
    JOIN gold.dim_building_type dbt  ON fc.building_type_key = dbt.building_type_key
    GROUP BY doc.occupancy_type, dbt.construction_class
),
policies_agg AS (
    SELECT
        doc.occupancy_type,
        dbt.construction_class,
        SUM(fp.exposure)        AS total_exposure,
        AVG(fp.total_premium)   AS avg_premium,
        COUNT(*)                AS policy_count
    FROM gold.fact_policies fp
    JOIN gold.dim_occupancy doc      ON fp.occupancy_key     = doc.occupancy_key
    JOIN gold.dim_building_type dbt  ON fp.building_type_key = dbt.building_type_key
    GROUP BY doc.occupancy_type, dbt.construction_class
)
SELECT
    COALESCE(c.occupancy_type, p.occupancy_type)                    AS occupancy_type,
    COALESCE(c.construction_class, p.construction_class)            AS construction_class,
    COALESCE(c.total_claims, 0)                                     AS total_claims,
    COALESCE(p.total_exposure, 0)                                   AS total_exposure,
    COALESCE(p.policy_count, 0)                                     AS policy_count,
    COALESCE(c.total_claims, 0)
        / NULLIF(COALESCE(p.total_exposure, 0), 0)                  AS pure_premium,
    p.avg_premium,
    CASE
        WHEN p.avg_premium >= COALESCE(c.total_claims, 0)
                / NULLIF(COALESCE(p.total_exposure, 0), 0)
            THEN 'Adequate'
        WHEN p.avg_premium IS NULL OR p.total_exposure IS NULL
            THEN 'No Policy Data'
        ELSE 'Inadequate'
    END                                                             AS adequacy_flag
FROM claims_agg c
FULL OUTER JOIN policies_agg p
    ON c.occupancy_type     = p.occupancy_type
   AND c.construction_class = p.construction_class;
GO

PRINT 'View gold.vw_premium_adequacy created.';

-- ================================================================
-- View 6: Claims Development (Simplified)
-- NOTE: This is a simplified development view. A true loss
-- development triangle requires incremental payment dates (paid-to-
-- date at successive evaluation points), which the NFIP dataset
-- does not provide. This view shows ultimate paid by accident year.
-- ================================================================
CREATE OR ALTER VIEW gold.vw_claims_development AS
SELECT
    fc.year_of_loss,
    COUNT(*)                                                        AS claim_count,
    SUM(fc.amount_paid_total)                                       AS total_paid,
    AVG(fc.amount_paid_total)                                       AS avg_paid_per_claim,
    SUM(fc.amount_paid_building)                                    AS total_building_paid,
    SUM(fc.amount_paid_contents)                                    AS total_contents_paid
FROM gold.fact_claims fc
WHERE fc.year_of_loss IS NOT NULL
GROUP BY fc.year_of_loss;
GO

PRINT 'View gold.vw_claims_development created.';

-- ================================================================
-- View 7: Portfolio Summary (One Row per Year)
-- Includes YoY growth via LAG()
-- ================================================================
CREATE OR ALTER VIEW gold.vw_portfolio_summary AS
WITH yearly AS (
    SELECT
        d.year,
        COUNT(DISTINCT fp.policy_sk)            AS total_policies,
        SUM(fp.exposure)                        AS total_exposure,
        SUM(fp.total_premium)                   AS total_premium
    FROM gold.fact_policies fp
    JOIN gold.dim_date d ON fp.effective_date_key = d.date_key
    GROUP BY d.year
),
claims_yearly AS (
    SELECT
        d.year,
        SUM(fc.amount_paid_total)               AS total_claims_paid,
        COUNT(*)                                AS claim_count
    FROM gold.fact_claims fc
    JOIN gold.dim_date d ON fc.date_key = d.date_key
    GROUP BY d.year
)
SELECT
    y.year,
    y.total_policies,
    y.total_exposure,
    y.total_premium,
    COALESCE(c.total_claims_paid, 0)                                AS total_claims_paid,
    COALESCE(c.claim_count, 0)                                      AS claim_count,
    COALESCE(c.total_claims_paid, 0)
        / NULLIF(y.total_premium, 0)                                AS loss_ratio,
    COALESCE(c.total_claims_paid, 0)
        / NULLIF(COALESCE(c.claim_count, 0), 0)                    AS avg_severity,
    LAG(y.total_premium) OVER (ORDER BY y.year)                     AS prior_year_premium,
    CASE
        WHEN LAG(y.total_premium) OVER (ORDER BY y.year) > 0
        THEN (y.total_premium - LAG(y.total_premium) OVER (ORDER BY y.year))
             / LAG(y.total_premium) OVER (ORDER BY y.year) * 100
        ELSE NULL
    END                                                             AS premium_growth_pct,
    LAG(COALESCE(c.total_claims_paid, 0)) OVER (ORDER BY y.year)   AS prior_year_claims,
    CASE
        WHEN LAG(COALESCE(c.total_claims_paid, 0)) OVER (ORDER BY y.year) > 0
        THEN (COALESCE(c.total_claims_paid, 0)
              - LAG(COALESCE(c.total_claims_paid, 0)) OVER (ORDER BY y.year))
             / LAG(COALESCE(c.total_claims_paid, 0)) OVER (ORDER BY y.year) * 100
        ELSE NULL
    END                                                             AS claims_growth_pct
FROM yearly y
LEFT JOIN claims_yearly c ON y.year = c.year;
GO

PRINT 'View gold.vw_portfolio_summary created.';

-- ================================================================
-- Verification: test each view
-- ================================================================
PRINT '';
PRINT '--- Verifying Views ---';

PRINT 'vw_loss_ratio_by_state:';
SELECT TOP 5 * FROM gold.vw_loss_ratio_by_state ORDER BY year DESC, state_name;

PRINT 'vw_claims_frequency_severity:';
SELECT TOP 5 * FROM gold.vw_claims_frequency_severity ORDER BY year DESC, zone_category;

PRINT 'vw_large_loss_concentration:';
SELECT TOP 5 * FROM gold.vw_large_loss_concentration ORDER BY large_loss_total DESC;

PRINT 'vw_severity_by_flood_zone:';
SELECT TOP 5 * FROM gold.vw_severity_by_flood_zone ORDER BY avg_severity DESC;

PRINT 'vw_premium_adequacy:';
SELECT TOP 5 * FROM gold.vw_premium_adequacy ORDER BY total_claims DESC;

PRINT 'vw_claims_development:';
SELECT TOP 5 * FROM gold.vw_claims_development ORDER BY year_of_loss DESC;

PRINT 'vw_portfolio_summary:';
SELECT TOP 5 * FROM gold.vw_portfolio_summary ORDER BY year DESC;

PRINT '';
PRINT '================================================================';
PRINT 'ALL VIEWS CREATED AND VERIFIED';
PRINT '================================================================';
GO
