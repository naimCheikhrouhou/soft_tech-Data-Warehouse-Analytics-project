CREATE OR ALTER PROCEDURE silver.load_customers AS
BEGIN
    DECLARE 
        @start_time DATETIME, 
        @end_time DATETIME ;

        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.customers';
        ---------------------------------------------------------------------------------------------------- Load silver.customers
        TRUNCATE TABLE silver.customers;
        PRINT '>> Inserting Data Into: silver.customers'; --handling spaces/nulls/phone number format/emails format
                                                           --/making country before create date
        INSERT INTO silver.customers (                           
            customer_id,
            company_name,
            contact_name,
            email,
            phone,
            country,
            created_at,
            [source]
        )
        SELECT
            customer_id,
            TRIM(IsNULL(company_name, 'n/a')) AS company_name,
            TRIM(IsNULL(contact_name, 'n/a')) AS contact_name,
        CASE 
        WHEN CHARINDEX('@', TRIM(email)) > 0 THEN LOWER(TRIM(email))
        ELSE 'n/a'
    END AS formatted_email,
           
    CASE 
        WHEN LOWER(TRIM(country)) = 'tunisie' THEN '+216' + RIGHT(TRIM(ISNULL(phone,'n/a')), 8)
        WHEN LOWER(TRIM(country)) = 'france' THEN '+33'  + RIGHT(TRIM(ISNULL(phone,'n/a')), 8)
        WHEN LOWER(TRIM(country)) = 'espagne' THEN '+34' + RIGHT(TRIM(ISNULL(phone,'n/a')), 8)
        WHEN LOWER(TRIM(country)) = 'allemagne' THEN '+49' + RIGHT(TRIM(ISNULL(phone,'n/a')), 8)
        WHEN LOWER(TRIM(country)) = 'italie' THEN '+39' + RIGHT(TRIM(ISNULL(phone,'n/a')), 8)
        WHEN LOWER(TRIM(country)) = 'japon' THEN '+81' + RIGHT(TRIM(ISNULL(phone,'n/a')), 8)
        WHEN LOWER(TRIM(country)) = 'canada' THEN '+1' + RIGHT(TRIM(ISNULL(phone,'n/a')), 8)
        WHEN LOWER(TRIM(country)) = 'usa' THEN '+1' + RIGHT(TRIM(ISNULL(phone,'n/a')), 8)
        WHEN LOWER(TRIM(country)) = 'maroc' THEN '+212' + RIGHT(TRIM(ISNULL(phone,'n/a')), 8)
       ELSE RIGHT(TRIM(phone), 8)                               -- default: just last 8 digits
                                           END AS phone,

           
            TRIM(IsNULL(country, 'n/a')) AS country,
            created_at,
            'bronze.customers_sfax'
        FROM bronze.customers_sfax;
        PRINT 'FROM  bronze.customers_sfax';
-----------------------------------------------------------------------(tunis_source)--------------------------------------
          INSERT INTO silver.customers (                           
            customer_id,
            company_name,
            contact_name,
            email,
            phone,
            country,
            created_at,
            [source]
        )
        SELECT
            customer_id,
            TRIM(IsNULL(company_name, 'n/a')) AS company_name,
            TRIM(IsNULL(contact_name, 'n/a')) AS contact_name,
            TRIM(IsNULL(email, 'n/a')) AS email,
           
    CASE 
        WHEN LOWER(TRIM(country)) = 'tunisie' THEN '+216' + RIGHT(TRIM(phone), 8)
        WHEN LOWER(TRIM(country)) = 'france' THEN '+33'  + RIGHT(TRIM(phone), 8)
        WHEN LOWER(TRIM(country)) = 'espagne' THEN '+34' + RIGHT(TRIM(phone), 8)
        WHEN LOWER(TRIM(country)) = 'allemagne' THEN '+49' + RIGHT(TRIM(phone), 8)
        WHEN LOWER(TRIM(country)) = 'italie' THEN '+39' + RIGHT(TRIM(phone), 8)
        WHEN LOWER(TRIM(country)) = 'japon' THEN '+81' + RIGHT(TRIM(phone), 8)
        WHEN LOWER(TRIM(country)) = 'canada' THEN '+1' + RIGHT(TRIM(phone), 8)
        WHEN LOWER(TRIM(country)) = 'usa' THEN '+1' + RIGHT(TRIM(phone), 8)
        WHEN LOWER(TRIM(country)) = 'maroc' THEN '+212' + RIGHT(TRIM(phone), 8)
       ELSE RIGHT(TRIM(phone), 8)                               -- default: just last 8 digits
                                           END AS phone,

           
            TRIM(IsNULL(country, 'n/a')) AS country,
            created_at,
            'bronze.customers_tunis'
        FROM bronze.customers_tunis;
        PRINT 'FROM bronze.customers_tunis'
        PRINT ' '
---------------------------------------------------------------------------------------------------------------------------------
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';
   END 
   exec silver.load_customers;
