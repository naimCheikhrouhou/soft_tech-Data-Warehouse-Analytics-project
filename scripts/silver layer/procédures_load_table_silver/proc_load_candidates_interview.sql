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
          
    END
