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
          
    END
