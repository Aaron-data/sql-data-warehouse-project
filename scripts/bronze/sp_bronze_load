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
	DECLARE @start_time DATETIME, @end_time DATETIME;
	DECLARE @batch_start DATETIME, @batch_end DATETIME;
	BEGIN TRY
		PRINT '==========================';
		PRINT 'Loading the bronze layer';
		PRINT '==========================';

		PRINT '-------------------';
		PRINT 'Loading ERP Tables';
		PRINT '-------------------';

		--1--
		SET @batch_start = GETDATE();
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_cust_details';
		TRUNCATE TABLE bronze.erp_cust_details;

		PRINT '>> Loading into Table: bronze.erp_cust_details';
		BULK INSERT bronze.erp_cust_details
		FROM 'C:\sql_learning\data_warehouse_project\datasets\CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT'Load duration: '+CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds'; 

		--2--
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_loc_details';
		TRUNCATE TABLE bronze.erp_loc_details;

		PRINT '>> Loading into Table: bronze.erp_loc_details';
		BULK INSERT bronze.erp_loc_details
		FROM 'C:\sql_learning\data_warehouse_project\datasets\LOC_A101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT'Load duration: '+CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds'; 

		--3--
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_prd_cat_details';
		TRUNCATE TABLE bronze.erp_prd_cat_details;

		PRINT '>> Loading into Table: bronze.erp_prd_cat_details';
		BULK INSERT bronze.erp_prd_cat_details
		FROM 'C:\sql_learning\data_warehouse_project\datasets\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT'Load duration: '+CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds'; 


		PRINT '-------------------';
		PRINT 'Loading CRM Tables';
		PRINT '-------------------';

		--4--
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;

		PRINT '>> Loading into Table: bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\sql_learning\data_warehouse_project\datasets\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT'Load duration: '+CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds'; 

		--5--
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;

		PRINT '>> Loading into Table: bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\sql_learning\data_warehouse_project\datasets\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT'Load duration: '+CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds'; 

		--6--
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;

		PRINT '>> Loading into Table: bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\sql_learning\data_warehouse_project\datasets\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT'Load duration: '+CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds'; 
		SET @batch_end = GETDATE();
		PRINT'Total batch time: '+CAST(DATEDIFF(second,@batch_start,@batch_end) AS NVARCHAR)+ ' seconds';
	END TRY
	BEGIN CATCH
		PRINT'==================================';
		PRINT'ERROR OCCURED DURING LOADING BRONZE LAYER';
		PRINT'ERROR MESSAGE' + ERROR_MESSAGE();
		PRINT'ERROR MESSAGE' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT'ERROR MESSAGE' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT'==================================';
	END CATCH
END
