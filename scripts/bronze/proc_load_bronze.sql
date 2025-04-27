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
-- PostgreSQL PL/pgSQL Stored Procedure to load CSVs into bronze schema
-- Create or replace the procedure
CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
AS $$
DECLARE
    batch_start_time TIMESTAMP;
    batch_end_time   TIMESTAMP;
    start_time       TIMESTAMP;
    end_time         TIMESTAMP;
    csv_file TEXT;
BEGIN
    -- record batch start
    batch_start_time := clock_timestamp();
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Starting Bronze layer load...';
    RAISE NOTICE '========================================';

    -- List of tables and file paths
    FOR csv_file, start_time IN
        SELECT
            format('bronze.%s', t.tbl), clock_timestamp()
        FROM (VALUES
            ('crm_cust_info', 'C:/Users/Ryan Gabriel Magno/Documents/sql-data-warehouse-project/datasets/source_crm/cust_info.csv'),
            ('crm_prd_info', 'C:/Users/Ryan Gabriel Magno/Documents/sql-data-warehouse-project/datasets/source_crm/prd_info.csv'),
            ('crm_sales_details', 'C:/Users/Ryan Gabriel Magno/Documents/sql-data-warehouse-project/datasets/source_crm/sales_details.csv'),
            ('erp_loc_a101', 'C:/Users/Ryan Gabriel Magno/Documents/sql-data-warehouse-project/datasets/source_erp/LOC_A101.csv'),
            ('erp_cust_az12', 'C:/Users/Ryan Gabriel Magno/Documents/sql-data-warehouse-project/datasets/source_erp/CUST_AZ12.csv'),
            ('erp_px_cat_g1v2', 'C:/Users/Ryan Gabriel Magno/Documents/sql-data-warehouse-project/datasets/source_erp/PX_CAT_G1V2.csv')
        ) AS t(tbl, path)
    LOOP
        -- truncate
        RAISE NOTICE 'Truncating %', csv_file;
        EXECUTE format('TRUNCATE TABLE %s', csv_file);

        -- load via COPY
        RAISE NOTICE 'Copying data from %', csv_file;
        EXECUTE format(
            'COPY %s FROM %L WITH (FORMAT csv, HEADER true)',
            csv_file,
            (SELECT path FROM (VALUES
                ('crm_cust_info', 'C:/Users/Ryan Gabriel Magno/Documents/sql-data-warehouse-project/datasets/source_crm/cust_info.csv'),
                ('crm_prd_info',  'C:/Users/Ryan Gabriel Magno/Documents/sql-data-warehouse-project/datasets/source_crm/prd_info.csv'),
                ('crm_sales_details','C:/Users/Ryan Gabriel Magno/Documents/sql-data-warehouse-project/datasets/source_crm/sales_details.csv'),
                ('erp_loc_a101','C:/Users/Ryan Gabriel Magno/Documents/sql-data-warehouse-project/datasets/source_erp/LOC_A101.csv'),
                ('erp_cust_az12','C:/Users/Ryan Gabriel Magno/Documents/sql-data-warehouse-project/datasets/source_erp/CUST_AZ12.csv'),
                ('erp_px_cat_g1v2','C:/Users/Ryan Gabriel Magno/Documents/sql-data-warehouse-project/datasets/source_erp/PX_CAT_G1V2.csv')
            ) AS p(tbl,path) WHERE p.tbl = split_part(csv_file, '.', 2))
        );
        end_time := clock_timestamp();
        RAISE NOTICE 'Load duration for %: % seconds', csv_file, EXTRACT(epoch FROM end_time - start_time);
    END LOOP;

    batch_end_time := clock_timestamp();
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Finished Bronze layer load in % seconds', EXTRACT(epoch FROM batch_end_time - batch_start_time);
    RAISE NOTICE '========================================';
END;
$$;

/*how to execute the procedure in psql
===============================================================================
Script Purpose:
	This script provides instructions on how to execute the stored procedure 
	for loading data into the bronze layer of the data warehouse.
===============================================================================
1. Open psql and enter credentials

2. At the psql prompt, execute:
\copy bronze.crm_cust_info      FROM 'C:/Users/Ryan Gabriel Magno/Documents/sql-data-warehouse-project/datasets/source_crm/cust_info.csv'      CSV HEADER;
\copy bronze.crm_prd_info       FROM 'C:/Users/Ryan Gabriel Magno/Documents/sql-data-warehouse-project/datasets/source_crm/prd_info.csv'       CSV HEADER;
\copy bronze.crm_sales_details FROM 'C:/Users/Ryan Gabriel Magno/Documents/sql-data-warehouse-project/datasets/source_crm/sales_details.csv' CSV HEADER;
\copy bronze.erp_loc_a101      FROM 'C:/Users/Ryan Gabriel Magno/Documents/sql-data-warehouse-project/datasets/source_erp/LOC_A101.csv'      CSV HEADER;
\copy bronze.erp_cust_az12     FROM 'C:/Users/Ryan Gabriel Magno/Documents/sql-data-warehouse-project/datasets/source_erp/CUST_AZ12.csv'    CSV HEADER;
\copy bronze.erp_px_cat_g1v2   FROM 'C:/Users/Ryan Gabriel Magno/Documents/sql-data-warehouse-project/datasets/source_erp/PX_CAT_G1V2.csv' CSV HEADER;

*/