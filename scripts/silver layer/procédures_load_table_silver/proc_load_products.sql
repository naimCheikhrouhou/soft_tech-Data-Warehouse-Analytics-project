CREATE OR ALTER PROCEDURE silver.load_products AS
BEGIN
    DECLARE 
        @start_time DATETIME, 
        @end_time DATETIME; 

        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.products';
        TRUNCATE TABLE silver.products;
        PRINT '>> Inserting Data Into: silver.products';
        INSERT INTO silver.products (
            product_id,
            product_name,
            category,
            base_price,
            development_hours,
            [version],
            [source]
        )
        SELECT
            product_id,
            TRIM(product_name),
            TRIM(category),
            ISNULL(base_price,0),
            ISNULL(development_hours,0),
            TRIM(ISNULL([version],'n/a')),
            'bronze.products'
        FROM bronze.products;


        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';
    END
