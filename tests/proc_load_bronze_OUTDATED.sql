```
For day: 17.11.2025 this proceudre is outdated as it was doing bulk insert from 
one big weather_data.csv file, which is prone to totally fail when only small part
of the csv file cannot be digested by script. 

Thus, i decided to save each api call in seperate csv file and load them one by one,
with the usage of:
	pypyodbc library
	(specifically, conection to databse and cursor)
```



CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @batch_start_time DATETIME, @batch_end_time DATETIME; 
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '================================================';
		PRINT 'Loading Bronze Layer';
		PRINT '================================================';

		PRINT '------------------------------------------------';
		PRINT 'Loading weather_api_data Table';
		PRINT '------------------------------------------------';

		PRINT '>> Truncating Table: bronze.weather_api_data';
		TRUNCATE TABLE bronze.weather_api_data;
		PRINT '>> Inserting Data Into: weather_api_data';
		BULK INSERT bronze.weather_api_data
		FROM 'C:\Users\DELL\Desktop\weather_data_pipeline\weather_data.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @batch_end_time = GETDATE();
		PRINT '=========================================='
		PRINT 'Loading Bronze Layer is Completed';
        PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '=========================================='
	END TRY
	BEGIN CATCH
		PRINT '=========================================='
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=========================================='
	END CATCH
END