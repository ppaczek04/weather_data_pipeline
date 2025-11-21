select * from bronze.weather_api_data
select count(*) from bronze.weather_api_data;
truncate table bronze.weather_api_data;

-- cities analized
select distinct(city) from bronze.weather_api_data;
-- lat,long pairs check 
select distinct latitude, longitude from bronze.weather_api_data;

select distinct load_dts_utc from bronze.weather_api_data;

-- check if we have any anomalies in numerical values columns
select min(temperature_c), max(temperature_c) from bronze.weather_api_data;
select min(humidity_pct), max(humidity_pct) from bronze.weather_api_data;
select min(precip_mm), max(precip_mm) from bronze.weather_api_data;

SELECT DISTINCT timestamp
FROM bronze.weather_api_data
WHERE TRY_CONVERT(date, timestamp) IS NULL;
