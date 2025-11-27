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
          
    END
