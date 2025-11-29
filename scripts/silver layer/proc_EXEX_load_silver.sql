CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
    DECLARE 
        @batch_start_time DATETIME, 
        @batch_end_time DATETIME; 

    BEGIN TRY
        SET @batch_start_time = GETDATE();
        PRINT '================================================';
        PRINT 'Loading Silver Layer';
        PRINT '================================================';

     
---------------------------------------------------------------------------------------------------------------------------------
        -- load customers
           EXEC silver.load_customers;
        -- Load products
           EXEC silver.load_products;
        --load sales 
           EXEC silver.load_sales ; 
        -- ========================================
        -- 2. Loading Teams & Employees & Projects
        -- ========================================

        -- Load teams
           EXEC silver.load_teams;

        -- Load employees
           EXEC silver.load_employees;

        -- Load projects
           EXEC silver.load_projects;


        -- Load project_bug_reports
           EXEC silver.load_projects_bug_reports;

        -- ========================================
        -- 3. Loading Employee Performance
        -- ========================================
           EXEC silver.load_employees_performance;
        -- ========================================
        -- 4. Loading Candidates
        -- ========================================
           EXEC silver.load_candidates;

        -- Load candidate_cv_raw
           EXEC silver.load_candidates_cv_raw;

        -- Load candidate_interviews
           EXEC silver.load_candidates_interview;

        -- ========================================
        -- End of Silver Load
        -- ========================================
        SET @batch_end_time = GETDATE();
        PRINT '========================================';
        PRINT 'Loading Silver Layer Completed';
        PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
        PRINT '========================================';

    END TRY
    BEGIN CATCH
        PRINT '========================================';
        PRINT 'ERROR OCCURRED DURING SILVER LOAD';
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error State: ' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT '========================================';
    END CATCH
END
