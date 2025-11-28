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
    CAST(SUBSTRING(CAST(cv_id AS VARCHAR(MAX)), 2, LEN(CAST(cv_id AS VARCHAR(MAX))) - 1) AS INT) AS cv_id,
    
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
    CAST(LEFT(CAST(uploaded_date AS VARCHAR(MAX)), LEN(CAST(uploaded_date AS VARCHAR(MAX))) - 1) AS DATE) AS uploaded_date --#######################################date 

        FROM bronze.candidate_cv_raw;
        
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';
          
    END
