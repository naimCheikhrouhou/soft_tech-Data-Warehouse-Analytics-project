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
          
    END
