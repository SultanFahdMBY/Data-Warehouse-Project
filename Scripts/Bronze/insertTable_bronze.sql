CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '=========================================';
		PRINT 'Loading Bronze Layer';
		PRINT '=========================================';

		PRINT '-----------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '-----------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_cust_info'
		--Menghapus seluruh row dari table, sehingga membuatnya menjadi empty table
		TRUNCATE TABLE bronze.crm_cust_info;

		PRINT '>> Inserting Data Into: bronze.crm_cust_info'
		--Insert data dari data lokal ke table
		BULK INSERT bronze.crm_cust_info
		FROM 'D:\File Sultan\SQL Course\SQL data warehouse\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' Seconds'
		PRINT '>> -------------';


		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_prd_info';
		--Menghapus seluruh row dari table, sehingga membuatnya menjadi empty table
		TRUNCATE TABLE bronze.crm_prd_info;

		PRINT '>> Inserting Data Into: bronze.crm_prd_info';
		--Insert data dari data lokal ke table
		BULK INSERT bronze.crm_prd_info
		FROM 'D:\File Sultan\SQL Course\SQL data warehouse\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		)
		SET @end_time = GETDATE();
		PRINT '>> Load Duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' Seconds'
		PRINT '>> -------------';



		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_sales_details';
		--Menghapus seluruh row dari table, sehingga membuatnya menjadi empty table
		TRUNCATE TABLE bronze.crm_sales_details;

		PRINT '>> Inserting Data Into: bronze.crm_sales_details';
		--Insert data dari data lokal ke table
		BULK INSERT bronze.crm_sales_details
		FROM 'D:\File Sultan\SQL Course\SQL data warehouse\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		)
		SET @end_time = GETDATE();
		PRINT '>> Load Duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' Seconds'
		PRINT '>> -------------'



		PRINT '-----------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '-----------------------------------------';


		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_cust_az12';
		--Menghapus seluruh row dari table, sehingga membuatnya menjadi empty table
		TRUNCATE TABLE bronze.erp_cust_az12;

		PRINT '>> Inserting Data Into: bronze.erp_cust_az12';
		--Insert data dari data lokal ke table
		BULK INSERT bronze.erp_cust_az12
		FROM 'D:\File Sultan\SQL Course\SQL data warehouse\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
		WITH  (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		)
		SET @end_time = GETDATE();
		PRINT '>> Load Duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' Seconds'
		PRINT '>> -------------'


		
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_loc_a101';
		--Menghapus seluruh row dari table, sehingga membuatnya menjadi empty table
		TRUNCATE TABLE bronze.erp_loc_a101;

		PRINT '>> Inserting Data Into: bronze.erp_loc_a101';
		--Insert data dari data lokal ke table
		BULK INSERT bronze.erp_loc_a101
		FROM 'D:\File Sultan\SQL Course\SQL data warehouse\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' Seconds'
		PRINT '>> -------------'


		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2';
		--Menghapus seluruh row dari table, sehingga membuatnya menjadi empty table
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;

		PRINT '>> Inserting Data Into: bronze.erp_px_cat_g1v2';
		--Insert data dari data lokal ke table
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM  'D:\File Sultan\SQL Course\SQL data warehouse\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' Seconds'
		PRINT '>> -------------'

		SET @batch_end_time = GETDATE();
		PRINT '=========================================';
		PRINT 'Loading Bronze Layer is completed';
		PRINT ' - Total Load Duration: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' Seconds'
		PRINT '=========================================';
	END TRY
	BEGIN CATCH
		PRINT '=========================================';
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
	END CATCH
END