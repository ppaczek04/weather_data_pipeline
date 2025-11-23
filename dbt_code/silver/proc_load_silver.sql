/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;
===============================================================================
*/

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
    DECLARE @batch_start_time DATETIME, @batch_end_time DATETIME; 
    BEGIN TRY
        SET @batch_start_time = GETDATE();
        PRINT '================================================';
        PRINT '            Loading Silver Layer';
        PRINT '================================================';

        -- we remove index, if it exists from previous procedure runs 
        -- to insert data faster
        DROP INDEX IF EXISTS IX_silver_weather_city_day_hour 
            ON silver.weather_api_data;

        PRINT '>> Truncating Table: silver.weather_api_data'
        TRUNCATE TABLE silver.weather_api_data;
        PRINT '>> Inserting Data Into: silver.weather_api_data'
        INSERT INTO silver.weather_api_data (
            timestamp_day,
            timestamp_hour,
            temperature_c,
            humidity_pct,
            precip_mm,
            city,
            latitude,
            longitude,
            load_dts_utc
        )
        SELECT
            TRY_CONVERT(date, REPLACE(timestamp, 'T', ' '))  AS timestamp_day,
            DATEPART(HOUR, TRY_CONVERT(datetime2, REPLACE(timestamp, 'T', ' '))) AS timestamp_hour,
            temperature_c,
            humidity_pct,
            precip_mm,
            city,
            latitude,
            longitude,
            load_dts_utc
        FROM bronze.weather_api_data;

        -- after silver layer is ready, we add index to optimise
        -- the performance of queries on silver and gold layer
        CREATE INDEX IX_silver_weather_city_day_hour
        ON silver.weather_api_data (city, timestamp_day, timestamp_hour);

        SET @batch_end_time = GETDATE();
		PRINT '=========================================='
		PRINT 'Loading Silver Layer is Completed';
        PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '=========================================='
    END TRY
    BEGIN CATCH
        PRINT '=============================='
        PRINT 'ERROR OCCURED DURING LOADING SILVER LAYER'
        PRINT 'Error Message' + ERROR_MESSAGE();
        PRINT 'Error Message' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT '=============================='
    END CATCH
END