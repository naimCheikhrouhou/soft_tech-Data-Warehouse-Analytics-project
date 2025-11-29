CREATE OR ALTER PROCEDURE silver.load_candidates AS
BEGIN
    DECLARE 
        @start_time DATETIME, 
        @end_time DATETIME;  

        SET @start_time = GETDATE();
        
        PRINT '>> Truncating Table: silver.candidates';
        TRUNCATE TABLE silver.candidates;
        PRINT '>> Inserting Data Into: silver.candidates';
        INSERT INTO silver.candidates (
            candidate_id,
            full_name,
            email,
            phone,
            degree,
            years_experience,
            tech_stack,
            certifications,
            application_date
            
        )
        SELECT
            candidate_id,
            TRIM(full_name),
            LOWER(TRIM(email)),
            TRIM(phone),
            TRIM(degree),
            ISNULL(years_experience,0),
            tech_stack,
            certifications,
            application_date
        FROM bronze.candidates; 

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';
          
    END;
    go
    CREATE OR ALTER PROCEDURE silver.load_candidates_cv_raw AS
BEGIN
    DECLARE 
        @start_time DATETIME, 
        @end_time DATETIME;  

        SET @start_time = GETDATE();
         PRINT '>> Truncating Table: silver.candidate_cv_raw';
        TRUNCATE TABLE silver.candidate_cv_raw;
        PRINT '>> Inserting Data Into: silver.candidate_cv_raw';
        INSERT INTO silver.candidate_cv_raw (
            cv_id,
            candidate_id,
            cv_text,
            parsed_keywords,
            language_detected,
            uploaded_date
        )
        SELECT
             -- Remove first char and convert to INT
    CAST(SUBSTRING(CAST(cv_id AS VARCHAR(MAX)), 2, LEN(CAST(cv_id AS VARCHAR(MAX)))) AS INT) AS cv_id,
    
    candidate_id,
    cv_text,
    parsed_keywords,
    
    -- Map language
    CASE TRIM(language_detected)
        WHEN 'EN' THEN 'anglais'
        WHEN 'FR' THEN 'francais'
        ELSE TRIM(language_detected)
    END AS language_detected,
    
   
    -- Remove last char and convert to DATE
CASE WHEN RIGHT(CAST(uploaded_date AS nvarchar(MAX)),1) ='"' THEN 

     CAST(LEFT(CAST(uploaded_date AS VARCHAR(MAX)), LEN(CAST(uploaded_date AS VARCHAR(MAX))) - 1) AS DATE)
     
     ELSE CAST(CAST(uploaded_date AS nvarchar(max)) AS DATE)
     END                 AS uploaded_date                                                                          --#######################################date 

        FROM bronze.candidate_cv_raw;
        
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';
          
    END;
    go
    CREATE OR ALTER PROCEDURE silver.load_candidates_interview AS
BEGIN
    DECLARE 
        @start_time DATETIME, 
        @end_time DATETIME;  

        SET @start_time = GETDATE();
         PRINT '>> Truncating Table: silver.candidate_interviews';
        TRUNCATE TABLE silver.candidate_interviews;
        PRINT '>> Inserting Data Into: silver.candidate_interviews';
        INSERT INTO silver.candidate_interviews (
            interview_id,
            candidate_id,
            recruiter_id,
            interview_score,
            notes,
            team_id,
            result
        )
        SELECT
            interview_id,
            candidate_id,
            recruiter_id,
            ISNULL(interview_score,0),
            notes,
            TRIM(team_id),
            TRIM(result)
        FROM bronze.candidate_interviews;
        
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';
          
    END;
    go
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
        WHEN LOWER(TRIM(country)) = 'tunisie' THEN '+216' +' '+ ' '+ RIGHT(TRIM(isnull(phone,'n/a')), 8)
        WHEN LOWER(TRIM(country)) = 'france' THEN '+33'  + ' '+ RIGHT(TRIM(isnull(phone,'n/a')), 8)
        WHEN LOWER(TRIM(country)) = 'espagne' THEN '+34' + ' '+ RIGHT(TRIM(isnull(phone,'n/a')), 8)
        WHEN LOWER(TRIM(country)) = 'allemagne' THEN '+49' + ' '+ RIGHT(TRIM(isnull(phone,'n/a')), 8)
        WHEN LOWER(TRIM(country)) = 'italie' THEN '+39' + ' '+ RIGHT(TRIM(isnull(phone,'n/a')), 8)
        WHEN LOWER(TRIM(country)) = 'japon' THEN '+81' + ' '+ RIGHT(TRIM(isnull(phone,'n/a')), 8)
        WHEN LOWER(TRIM(country)) = 'canada' THEN '+1' + ' '+ RIGHT(TRIM(isnull(phone,'n/a')), 8)
        WHEN LOWER(TRIM(country)) = 'usa' THEN '+1' + ' '+ RIGHT(TRIM(isnull(phone,'n/a')), 8)
        WHEN LOWER(TRIM(country)) = 'maroc' THEN '+212' + ' '+ RIGHT(TRIM(isnull(phone,'n/a')), 8)
       ELSE ' '+ RIGHT(TRIM(isnull(phone,'n/a')), 8)                               -- default: just last 8 digits
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
        WHEN LOWER(TRIM(country)) = 'tunisie' THEN '+216' + ' '+ RIGHT(TRIM(isnull(phone,'n/a')), 8)
        WHEN LOWER(TRIM(country)) = 'france' THEN '+33'  + ' '+ RIGHT(TRIM(isnull(phone,'n/a')), 8)
        WHEN LOWER(TRIM(country)) = 'espagne' THEN '+34' + ' '+ RIGHT(TRIM(isnull(phone,'n/a')), 8)
        WHEN LOWER(TRIM(country)) = 'allemagne' THEN '+49' + ' '+ RIGHT(TRIM(isnull(phone,'n/a')), 8)
        WHEN LOWER(TRIM(country)) = 'italie' THEN '+39' + ' '+ RIGHT(TRIM(isnull(phone,'n/a')), 8)
        WHEN LOWER(TRIM(country)) = 'japon' THEN '+81' + ' '+ RIGHT(TRIM(isnull(phone,'n/a')), 8)
        WHEN LOWER(TRIM(country)) = 'canada' THEN '+1' + ' '+ RIGHT(TRIM(isnull(phone,'n/a')), 8)
        WHEN LOWER(TRIM(country)) = 'usa' THEN '+1' + ' '+ RIGHT(TRIM(isnull(phone,'n/a')), 8)
        WHEN LOWER(TRIM(country)) = 'maroc' THEN '+212' + ' '+ RIGHT(TRIM(isnull(phone,'n/a')), 8)
       ELSE ' '+ RIGHT(TRIM(isnull(phone,'n/a')), 8)                               -- default: just last 8 digits
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
   END ;
   go
   
   CREATE OR ALTER PROCEDURE silver.load_employees AS
BEGIN
    DECLARE 
        @start_time DATETIME, 
        @end_time DATETIME; 
        
        PRINT '>> Truncating Table: silver.employees';
        TRUNCATE TABLE silver.employees;
        PRINT '>> Inserting Data Into: silver.employees';
        INSERT INTO silver.employees (
            employee_id,
            full_name,
            [role],
            team_id,
            hire_date,
            salary,
            seniority,
            email
        )
        SELECT
            employee_id as employee_id,
            TRIM(full_name) as full_name,
            TRIM([role]) as [role],
            team_id as team_id,
            hire_date AS hire_date,
            ISNULL(salary,0) as salary ,
            TRIM(seniority)  as seniority,
            LOWER(TRIM(email)) as email
        FROM bronze.employees;

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';
          
    END;
    go
    CREATE OR ALTER PROCEDURE silver.load_employees_performance AS
BEGIN
    DECLARE 
        @start_time DATETIME, 
        @end_time DATETIME;  
        
        SET @start_time = GETDATE();

         PRINT '>> Truncating Table: silver.employee_performance';
        TRUNCATE TABLE silver.employee_performance;
        PRINT '>> Inserting Data Into: silver.employee_performance';
       INSERT INTO silver.employee_performance (
     perf_id,
     employee_id,
     start_date,
     end_date,
     tasks_completed,
     overtime_hours,
     performance_score,
     project_success_rate
)
SELECT
    perf_id,
    employee_id,
    CAST(start_date AS DATE) AS start_date,

    -- Correct: use DATEADD instead of subtracting 1
    CAST(
        DATEADD(
            DAY, 
            -1, 
            LEAD(CAST(start_date AS DATE)) OVER (
                PARTITION BY employee_id 
                ORDER BY CAST(start_date AS DATE)
            )
        ) 
        AS DATE
    ) AS end_date,

    ISNULL(tasks_completed,0),
    ISNULL(overtime_hours,0),
    ISNULL(performance_score,0),
    ISNULL(project_success_rate,0)
FROM bronze.employee_performance;

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';
          
    END;
    go
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
    END;
    go
    CREATE OR ALTER PROCEDURE silver.load_projects AS
BEGIN
    DECLARE 
        @start_time DATETIME, 
        @end_time DATETIME; 
        
         SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.projects';
        TRUNCATE TABLE silver.projects;
        PRINT '>> Inserting Data Into: silver.projects';
        INSERT INTO silver.projects (
            project_id,
            customer_id,
            project_name,
            [start_date],
            end_date,
            delivered_date,
            project_manager_id,
            estimated_cost,
            actual_cost,
            [status],
            [source]
        )
        SELECT
            project_id,
            customer_id,
            TRIM(project_name),
            start_date,
            end_date,
            delivered_date,
            project_manager as project_manager_id,
            ISNULL(estimated_cost,0),
            ISNULL(actual_cost,0),
            TRIM([status]),
            'bronze.projects_sfax'
        FROM bronze.projects_sfax;  print'FROM bronze.projects_sfax';

        INSERT INTO silver.projects (
            project_id,
            customer_id,
            project_name,
            [start_date],
            end_date,
            delivered_date,
            project_manager_id,
            estimated_cost,
            actual_cost,
            [status],
            [source]
        )
        SELECT
            project_id,
            customer_id,
            TRIM(project_name),
            start_date,
            end_date,
            delivered_date,
            project_manager as project_manager_id,
            ISNULL(estimated_cost,0),
            ISNULL(actual_cost,0),
            TRIM([status]),
            'bronze.projects_tunis'

        FROM bronze.projects_tunis;print'FROM bronze.projects_tuniq';

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';
          
    END;
    go
    CREATE OR ALTER PROCEDURE silver.load_projects_bug_reports AS
BEGIN
    DECLARE 
        @start_time DATETIME, 
        @end_time DATETIME;  
        
        SET @start_time = GETDATE();

        TRUNCATE TABLE silver.project_bug_reports;
        PRINT '>> Inserting Data Into: silver.project_bug_reports';
        INSERT INTO silver.project_bug_reports (
            bug_id,
            project_id,
            severity,
            [description],
            reported_date,
            resolved,
            [source]
        )
        SELECT
            bug_id,
            project_id,
            TRIM(severity),
            [description],
            reported_date,
            resolved,
            'bronze.project_bug_reports_sfax'
        FROM bronze.project_bug_reports_sfax;print' FROM bronze.project_bug_reports_sfax'

        TRUNCATE TABLE silver.project_bug_reports;
        PRINT '>> Inserting Data Into: silver.project_bug_reports';
        INSERT INTO silver.project_bug_reports (
            bug_id,
            project_id,
            severity,
            [description],
            reported_date,
            resolved,
            [source]
        )
        SELECT
            bug_id,
            project_id,
            TRIM(severity),
            [description],
            reported_date,
            resolved,
            'bronze.project_bug_reports_tunis'
        FROM bronze.project_bug_reports_tunis;print' FROM bronze.project_bug_reports_tunis'

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';
          
    END;
    go
    CREATE OR ALTER PROCEDURE silver.load_sales AS
BEGIN
    DECLARE 
        @start_time DATETIME, 
        @end_time DATETIME; 

                    -- Load sales_orders
             SET @start_time = GETDATE();
             PRINT '>> Truncating Table: silver.sales_orders';
             TRUNCATE TABLE silver.sales_orders;
             PRINT '>> Inserting Data Into: silver.sales_orders';
             INSERT INTO silver.sales_orders (
                 sale_id,
                 customer_id,
                 product_id,
                 quantity,
                 total_price,
                 payment_method,
                 sale_date,
                 [source]
             )
             SELECT
                 sale_id,
                 customer_id,
                 product_id,
                 ISNULL(quantity, 0) AS quantity,
                 ISNULL(total_price, quantity * ISNULL(total_price, 0)) AS total_price,
                 TRIM(REPLACE(payment_method, '?', 'é')) AS payment_method,
                 sale_date as  sale_date ,
                 'bronze.sales_orders_sfax'
             FROM bronze.sales_orders_sfax;


        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';
    END;
    go
    CREATE OR ALTER PROCEDURE silver.load_teams AS
BEGIN
    DECLARE 
        @start_time DATETIME, 
        @end_time DATETIME; 
         PRINT '>> Truncating Table: silver.teams';
        TRUNCATE TABLE silver.teams;
        PRINT '>> Inserting Data Into: silver.teams';
        INSERT INTO silver.teams (
            team_id,
            team_name,
            department,
            manager_id
        )
        SELECT
            team_id,
            TRIM(team_name),
            TRIM(department),
            manager_id
        FROM bronze.teams;
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';
          
    END;
    go
    
