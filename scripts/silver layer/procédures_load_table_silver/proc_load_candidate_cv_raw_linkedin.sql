CREATE OR ALTER PROCEDURE silver.load_candidates_cv_raw_linkedin AS
BEGIN
    DECLARE 
        @start_time DATETIME, 
        @end_time DATETIME;  

        SET @start_time = GETDATE();
         PRINT '>> Truncating Table: silver.candidate_cv_raw_linkedin';
        TRUNCATE TABLE silver.candidate_cv_raw_linkedin;
        PRINT '>> Inserting Data Into: silver.candidate_cv_raw_linkedin';
        INSERT INTO silver.candidate_cv_raw_linkedin (
            cv_id,
            candidate_id,
            cv_text,
            parsed_keywords,
            language_detected,
            uploaded_date,
            age,
            university
        )
        SELECT
             
     cast(cast(cv_id as nvarchar(50)) AS INT ) AS cv_id,
    
    candidate_id,
    cv_text,
    parsed_keywords,
    
    -- Map language
    CASE TRIM(language_detected)
        WHEN 'EN' THEN 'anglais'
        WHEN 'FR' THEN 'francais'
        ELSE TRIM(language_detected)
    END AS language_detected,
  
     
     --try_cast handels the  (dd/mm/yyyy /m/d/yyyy) diffrent date format  in your row data 
     
      TRY_CAST(REPLACE(CAST(uploaded_date as nvarchar(50)),'"','') as DATE )       AS uploaded_date     ,                                                                --#######################################date 
      
      age as age,
      university as university 
        FROM bronze.candidate_cv_raw_linkedin;
        
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';
          
    END
    exec silver.load_candidates_cv_raw_linkedin;
