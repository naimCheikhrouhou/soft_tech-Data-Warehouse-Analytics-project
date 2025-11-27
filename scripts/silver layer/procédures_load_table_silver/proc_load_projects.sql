CREATE OR ALTER PROCEDURE silver.load_employees AS
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
          
    END
