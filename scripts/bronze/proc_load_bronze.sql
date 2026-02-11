/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/
CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time Datetime , @end_time Datetime , @batch_start_time Datetime , @batch_end_time Datetime
	BEGIN TRY
		SET @batch_start_time = Getdate();
		print '=================================================================';
		PRINT 'Loading the Bronze Layer';
		print '=================================================================';

		print '-----------------------------------------------------------------';
		print 'Loading CMR Tables ';
		print '-----------------------------------------------------------------';


		Set @start_time = Getdate();
		print '>> Truncating Table : bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info; --everytime that runs does not duplicate values

		print '>> Inserting Data into : bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\User\Downloads\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,  --start reading from the second row, first row is the head 
			FIELDTERMINATOR = ',', --data is seperated by ','
			TABLOCK 
		);
		Set @end_time = Getdate();
		print '>> Load Duartion : ' + CAST(Datediff(second, @start_time , @end_time) AS NVARCHAR) + ' seconds';

		print '-----------------------------------------------------------------------------'

		Set @start_time = Getdate();
		print '>> Truncating Table : bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info; 

		print '>> Inserting Data into : bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\User\Downloads\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_crm\prd_info.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		Set @end_time = Getdate();
		print '>> Load Duartion : ' + CAST(Datediff(second, @start_time , @end_time) AS NVARCHAR) + ' seconds';

		print '-----------------------------------------------------------------------------'

		Set @start_time = Getdate();
		print '>> Truncating Table : bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details; 

		print '>> Inserting Data into : bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\User\Downloads\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_crm\sales_details.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		Set @end_time = Getdate();
		print '>> Load Duartion : ' + CAST(Datediff(second, @start_time , @end_time) AS NVARCHAR) + ' seconds';

		print '-----------------------------------------------------------------';
		print 'Loading ERP Tables ';
		print '-----------------------------------------------------------------';

		Set @start_time = Getdate();
		print '>> Truncating Table : bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12; 

		print '>> Inserting Data into : bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\User\Downloads\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_erp\CUST_AZ12.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		Set @end_time = Getdate();
		print '>> Load Duartion : ' + CAST(Datediff(second, @start_time , @end_time) AS NVARCHAR) + ' seconds';

		print '-----------------------------------------------------------------------------'

		Set @start_time = Getdate();
		print '>> Truncating Table : bronze.erp_loc_a101';

		TRUNCATE TABLE bronze.erp_loc_a101; 

		print '>> Inserting Data into : bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\User\Downloads\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_erp\LOC_A101.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		Set @end_time = Getdate();
		print '>> Load Duartion : ' + CAST(Datediff(second, @start_time , @end_time) AS NVARCHAR) + ' seconds';

		print '-----------------------------------------------------------------------------'

		Set @start_time = Getdate();
		print '>> Truncating Table : bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2; 

		print '>> Inserting Data into : bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\User\Downloads\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		Set @end_time = Getdate();
		print '>> Load Duartion : ' + CAST(Datediff(second, @start_time , @end_time) AS NVARCHAR) + ' seconds';

		SET @batch_end_time = Getdate();
		print '=================================================================='
		print '>> Total Load Duartion was : ' + CAST(Datediff(second, @batch_start_time , @batch_end_time) AS NVARCHAR) + ' seconds'
		print '=================================================================='

	END TRY
	BEGIN CATCH
		PRINT '===========================================================';
		print 'ERROR OCCURED DURING LOADING BRONZE LAYER';
		PRINT 'ERROR MESSAGE' + ERROR_MESSAGE();
		PRINT 'ERROR MESSAGE' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'ERROR MESSAGE' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '===========================================================';
	END CATCH
END
