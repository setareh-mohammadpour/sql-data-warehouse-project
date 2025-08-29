use Datawarehouse;
EXEC bronze.load_bronze
Create or Alter Procedure bronze.load_bronze As 
Begin 
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		print '=======================================';
		print'Loading Bronze Layer';
		print '=======================================';
		PRINT'----------------------------------------';
		PRINT'LOADING CRM TABLES';
		PRINT'----------------------------------------';
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_cust_info';
		Truncate Table bronze.crm_cust_info;
		PRINT '>> Inserting Data Into: bronze.crm_cust_info';
		Bulk insert bronze.crm_cust_info
		From 'C:\Users\semoh\Downloads\cust_info.csv'
		with(
		 FIRSTROW = 2,
					FIELDTERMINATOR = ',',
					TABLOCK
 
		 );
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_prd_info';
		Truncate Table bronze.crm_prd_info;
		PRINT '>> Inserting Data Into: bronze.crm_prd_info';
		Bulk insert bronze.crm_prd_info
		From 'C:\Users\semoh\Downloads\prd_info.csv'
		with(
		 FIRSTROW = 2,
					FIELDTERMINATOR = ',',
					TABLOCK
 
		 );
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_sales_details';
		Truncate Table bronze.crm_sales_details;
		PRINT '>> Inserting Data Into: bronze.crm_sales_details';
		Bulk insert bronze.crm_sales_details
		 From 'C:\Users\semoh\Downloads\sales_details.csv'
		 with(
		 FIRSTROW = 2,
					FIELDTERMINATOR = ',',
					TABLOCK
 
		 );
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';
		PRINT'----------------------------------------';
		PRINT'LOADING ERP TABLES';
		PRINT'----------------------------------------';
		SET @start_time = GETDATE();
		Truncate Table bronze.erp_CUST_AZ12;
		Bulk insert bronze.erp_CUST_AZ12
		 From 'C:\Users\semoh\Downloads\CUST_AZ12.csv'
		 with(
		 FIRSTROW = 2,
					FIELDTERMINATOR = ',',
					TABLOCK
 
		 );
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		SET @start_time = GETDATE();
		Truncate Table bronze.erp_LOC_A101;
		Bulk insert bronze.erp_LOC_A101
		 From 'C:\Users\semoh\Downloads\LOC_A101.csv'
		 with(
		 FIRSTROW = 2,
					FIELDTERMINATOR = ',',
					TABLOCK
 
		 );
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		SET @start_time = GETDATE();
		Truncate Table bronze.erp_PX_CAT_G1V2;
		Bulk insert bronze.erp_PX_CAT_G1V2
		 From 'C:\Users\semoh\Downloads\PX_CAT_G1V2.csv'
		 with(
		 FIRSTROW = 2,
					FIELDTERMINATOR = ',',
					TABLOCK
 
		 );
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';
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

