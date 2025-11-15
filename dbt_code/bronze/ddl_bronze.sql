/*
===============================================================================
DDL Script: Create Bronze Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'bronze' schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'bronze' Tables
===============================================================================
*/

IF OBJECT_ID('bronze.weather_api_data', 'U') IS NOT NULL
    DROP TABLE bronze.weather_api_data;
GO

CREATE TABLE bronze.weather_api_data (
    timestamp NVARCHAR(50),
    temperature_c NVARCHAR(50),
    humidity_pct NVARCHAR(50),
    precip_mm,city NVARCHAR(50),
    latitude NVARCHAR(50),
    longitude NVARCHAR(50),
    load_dts_utc NVARCHAR(50)
);