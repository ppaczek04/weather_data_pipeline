/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 

    Each view performs transformations and combines and agregates data from 
    the Silver layer to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/

-- =============================================================================
-- Create Aggregate:
-- =============================================================================

IF OBJECT_ID('gold.city_day_aggregate', 'V') IS NOT NULL
    DROP VIEW gold.city_day_aggregate;
    
GO

-- no surogate key as no fact-dim table sturcture
CREATE VIEW gold.city_day_aggregate AS
SELECT
    city,
    timestamp_day,
    AVG(temperature_c) AS avg_temp_c,
    MIN(temperature_c) AS min_temp_c,
    MAX(temperature_c) AS max_temp_c,
    AVG(humidity_pct) AS avg_humidity,
    SUM(precip_mm) AS total_precip_mm
FROM silver.weather_api_data
GROUP BY city, timestamp_day;

GO

IF OBJECT_ID('gold.city_latest_forecast', 'V') IS NOT NULL
    DROP VIEW gold.city_latest_forecast;

GO

-- No surrogate key as no fact-dim structure needed here
CREATE VIEW gold.city_latest_forecast AS
SELECT
    w.*
FROM silver.weather_api_data w
WHERE 
    w.timestamp_hour = 16   -- interesuje nas tylko odczyt z 16:00
    AND w.timestamp_day = (
        SELECT MAX(s.timestamp_day)
        FROM silver.weather_api_data s
        WHERE 
            s.city = w.city
            AND s.timestamp_hour = 16   -- tylko dni, w których mamy odczyt o 16:00
    );
GO




