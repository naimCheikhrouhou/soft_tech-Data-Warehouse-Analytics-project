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
          
    END
