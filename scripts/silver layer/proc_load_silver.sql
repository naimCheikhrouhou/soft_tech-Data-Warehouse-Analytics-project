CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
    DECLARE 
        @start_time DATETIME, 
        @end_time DATETIME, 
        @batch_start_time DATETIME, 
        @batch_end_time DATETIME; 

    BEGIN TRY
        SET @batch_start_time = GETDATE();
        PRINT '================================================';
        PRINT 'Loading Silver Layer';
        PRINT '================================================';

        -- ========================================
        -- 1. Loading CRM Tables
        -- ========================================

        -- Load customers
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.customers';
        TRUNCATE TABLE silver.customers;
        PRINT '>> Inserting Data Into: silver.customers';
        INSERT INTO silver.customers (
            customer_id,
            company_name,
            contact_name,
            email,
            phone,
            country,
            created_at
        )
        SELECT
            customer_id,
            TRIM(company_name),
            TRIM(contact_name),
            LOWER(TRIM(email)),
            TRIM(phone),
            TRIM(country),
            created_at
        FROM bronze.customers;
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

        -- Load products
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
            version
        )
        SELECT
            product_id,
            TRIM(product_name),
            TRIM(category),
            ISNULL(base_price,0),
            ISNULL(development_hours,0),
            TRIM(version)
        FROM bronze.products;
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

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
            sale_date
        )
        SELECT
            sale_id,
            customer_id,
            product_id,
            ISNULL(quantity,0),
            ISNULL(total_price, quantity * ISNULL(price,0)),
            TRIM(payment_method),
            sale_date
        FROM bronze.sales_orders;
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

        -- ========================================
        -- 2. Loading Teams & Employees & Projects
        -- ========================================

        -- Load teams
        SET @start_time = GETDATE();
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

        -- Load employees
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.employees';
        TRUNCATE TABLE silver.employees;
        PRINT '>> Inserting Data Into: silver.employees';
        INSERT INTO silver.employees (
            employee_id,
            full_name,
            role,
            team_id,
            hire_date,
            salary,
            seniority,
            email
        )
        SELECT
            employee_id,
            TRIM(full_name),
            TRIM(role),
            team_id,
            hire_date,
            ISNULL(salary,0),
            TRIM(seniority),
            LOWER(TRIM(email))
        FROM bronze.employees;
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

        -- Load projects
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.projects';
        TRUNCATE TABLE silver.projects;
        PRINT '>> Inserting Data Into: silver.projects';
        INSERT INTO silver.projects (
            project_id,
            customer_id,
            project_name,
            start_date,
            end_date,
            delivered_date,
            project_manager_id,
            estimated_cost,
            actual_cost,
            status
        )
        SELECT
            project_id,
            customer_id,
            TRIM(project_name),
            start_date,
            end_date,
            delivered_date,
            project_manager_id,
            ISNULL(estimated_cost,0),
            ISNULL(actual_cost,0),
            TRIM(status)
        FROM bronze.projects;
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

        -- Load project_bug_reports
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.project_bug_reports';
        TRUNCATE TABLE silver.project_bug_reports;
        PRINT '>> Inserting Data Into: silver.project_bug_reports';
        INSERT INTO silver.project_bug_reports (
            bug_id,
            project_id,
            severity,
            description,
            reported_date,
            resolved
        )
        SELECT
            bug_id,
            project_id,
            TRIM(severity),
            description,
            reported_date,
            resolved
        FROM bronze.project_bug_reports;
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

        -- ========================================
        -- 3. Loading Employee Performance
        -- ========================================
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
            start_date,
            end_date,
            ISNULL(tasks_completed,0),
            ISNULL(overtime_hours,0),
            ISNULL(performance_score,0),
            ISNULL(project_success_rate,0)
        FROM bronze.employee_performance;
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

        -- ========================================
        -- 4. Loading Candidates
        -- ========================================
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

        -- Load candidate_cv_raw
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
            cv_id,
            candidate_id,
            cv_text,
            parsed_keywords,
            TRIM(language_detected),
            uploaded_date
        FROM bronze.candidate_cv_raw;
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

        -- Load candidate_interviews
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
            stage,
            result
        )
        SELECT
            interview_id,
            candidate_id,
            recruiter_id,
            ISNULL(interview_score,0),
            notes,
            TRIM(stage),
            TRIM(result)
        FROM bronze.candidate_interviews;
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

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
