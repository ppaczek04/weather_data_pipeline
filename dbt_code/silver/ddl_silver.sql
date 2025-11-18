/*
===============================================================================
DDL Script: Create Silver Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'silver' schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'silver' Tables
===============================================================================
*/

IF OBJECT_ID('silver.weather_api_data', 'U') IS NOT NULL
    DROP TABLE silver.weather_api_data;
GO

CREATE TABLE silver.weather_api_data (
    timestamp           DATETIME2,
    temperature_c       FLOAT,
    humidity_pct        INT,
    precip_mm           FLOAT,
    city                NVARCHAR(50),
    latitude            FLOAT,
    longitude           FLOAT,
    load_dts_utc        DATETIME2
);