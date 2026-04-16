-- ================================================================
-- Script:    02_load_dim_date.sql
-- Layer:     Gold
-- Purpose:   Populate dim_date with dates from 1978-01-01 to
--            2026-12-31. date_key = YYYYMMDD as INT.
--            Fiscal year starts October (month >= 10 → year + 1).
-- Author:    Aayush Yagol
-- Execute:   ./scripts/run_sql.sh scripts/gold/02_load_dim_date.sql
-- ================================================================

USE NfipInsuranceWarehouse;
GO

DELETE FROM gold.dim_date;
PRINT 'Cleared gold.dim_date';

DECLARE @start_date DATE = '1978-01-01';
DECLARE @end_date   DATE = '2026-12-31';
DECLARE @current    DATE = @start_date;

WHILE @current <= @end_date
BEGIN
    INSERT INTO gold.dim_date (
        date_key,
        full_date,
        year,
        quarter,
        month,
        month_name,
        fiscal_year,
        fiscal_quarter,
        day_of_week,
        is_weekend
    )
    VALUES (
        CAST(FORMAT(@current, 'yyyyMMdd') AS INT),
        @current,
        YEAR(@current),
        DATEPART(QUARTER, @current),
        MONTH(@current),
        DATENAME(MONTH, @current),
        CASE WHEN MONTH(@current) >= 10 THEN YEAR(@current) + 1 ELSE YEAR(@current) END,
        CASE
            WHEN MONTH(@current) IN (10,11,12) THEN 1
            WHEN MONTH(@current) IN (1,2,3)    THEN 2
            WHEN MONTH(@current) IN (4,5,6)    THEN 3
            WHEN MONTH(@current) IN (7,8,9)    THEN 4
        END,
        DATENAME(WEEKDAY, @current),
        CASE WHEN DATEPART(WEEKDAY, @current) IN (1, 7) THEN 1 ELSE 0 END
    );

    SET @current = DATEADD(DAY, 1, @current);
END;

-- Validation
DECLARE @row_count INT;
SELECT @row_count = COUNT(*) FROM gold.dim_date;
PRINT 'gold.dim_date loaded: ' + CAST(@row_count AS VARCHAR) + ' rows';

-- Check no NULL keys
DECLARE @null_keys INT;
SELECT @null_keys = COUNT(*) FROM gold.dim_date WHERE date_key IS NULL;
PRINT 'NULL date_key count: ' + CAST(@null_keys AS VARCHAR) + ' (should be 0)';

-- Check no duplicate keys
DECLARE @dup_keys INT;
SELECT @dup_keys = COUNT(*) FROM (
    SELECT date_key FROM gold.dim_date GROUP BY date_key HAVING COUNT(*) > 1
) d;
PRINT 'Duplicate date_key count: ' + CAST(@dup_keys AS VARCHAR) + ' (should be 0)';
GO
