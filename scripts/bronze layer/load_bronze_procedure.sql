CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN 
DECLARE @start_time DATETIME ,@end_time DATETIME,@start DATETIME ,@end DATETIME ; 
   set @start = GETDATE();
  BEGIN TRY 
    print '==========================================================================';
    print 'loading the data from sources ' ;
    print '==========================================================================';


    -- ================= CUSTOMERS & PRODUCTS & SALES =================
    print 'loading  customers :' ;
     set @start_time = GETDATE();
    TRUNCATE TABLE bronze.customers_sfax ;
    BULK INSERT bronze.customers_sfax
    FROM 'C:\Users\cheik\Desktop\i1\entropot de donné\soft-tech project\datasets\business_data\branche_sfax\customers.csv'
    WITH(
    FIRSTROW =2 ,
    FIELDTERMINATOR =',' ,
    tablock 
    );
    ----------------------------
     TRUNCATE TABLE bronze.customers_tunis ;
    BULK INSERT bronze.customers_tunis
    FROM 'C:\Users\cheik\Desktop\i1\entropot de donné\soft-tech project\datasets\business_data\branche_tunis\customers.csv'
    WITH(
    FIRSTROW =2 ,
    FIELDTERMINATOR =',' ,
    tablock 
    );
    set @end_time = GETDATE();
    print 'load duration ' + cast(DATEDIFF(second,@start_time,@end_time ) AS NVARCHAR) +'seconds' ; 
    print '------------------------------------------------------------------------------------------'
    print 'loading products :' ;
     set @start_time = GETDATE();

    TRUNCATE TABLE bronze.products ;
    BULK INSERT bronze.products
    FROM 'C:\Users\cheik\Desktop\i1\entropot de donné\soft-tech project\datasets\business_data\products.csv'
    WITH(
    FIRSTROW =2 ,
    FIELDTERMINATOR =',' ,
    tablock 
    );
    set @end_time = GETDATE();
    print 'load duration ' + cast(DATEDIFF(second,@start_time,@end_time ) AS NVARCHAR) +'seconds' ; 
    print '------------------------------------------------------------------------------------------'
    print 'sales_orders :' ;
     set @start_time = GETDATE();
   
    TRUNCATE TABLE bronze.sales_orders_sfax ;
    BULK INSERT bronze.sales_orders_sfax
    FROM 'C:\Users\cheik\Desktop\i1\entropot de donné\soft-tech project\datasets\business_data\branche_sfax\sales.csv'
    WITH(
    FIRSTROW =2 ,
    FIELDTERMINATOR =',' ,
    tablock 
    );---------------------------------tunis source
      TRUNCATE TABLE bronze.sales_orders_tunis ;
    BULK INSERT bronze.sales_orders_tunis
    FROM 'C:\Users\cheik\Desktop\i1\entropot de donné\soft-tech project\datasets\business_data\branche_tunis\sales.csv'
    WITH(
    FIRSTROW =2 ,
    FIELDTERMINATOR =',' ,
    tablock 
    );
    set @end_time = GETDATE();
    print 'load duration ' + cast(DATEDIFF(second,@start_time,@end_time ) AS NVARCHAR) +'seconds' ; 
    print '------------------------------------------------------------------------------------------'

    -- ================= PROJECTS & BUG REPORTS =================
    print 'projects :' ;
     set @start_time = GETDATE();
     
    TRUNCATE TABLE bronze.projects_sfax ;
    BULK INSERT bronze.projects_sfax
    FROM 'C:\Users\cheik\Desktop\i1\entropot de donné\soft-tech project\datasets\business_data\branche_sfax\projects.csv'
    WITH(
    FIRSTROW =2 ,
    FIELDTERMINATOR =',' ,
    tablock 
    );---------------------------------------tunis source
    TRUNCATE TABLE bronze.projects_tunis ;
    BULK INSERT bronze.projects_tunis
    FROM 'C:\Users\cheik\Desktop\i1\entropot de donné\soft-tech project\datasets\business_data\branche_tunis\projects.csv'
    WITH(
    FIRSTROW =2 ,
    FIELDTERMINATOR =',' ,
    tablock 
    );
    set @end_time = GETDATE();
    print 'load duration ' + cast(DATEDIFF(second,@start_time,@end_time ) AS NVARCHAR) +'seconds' ; 
    print '------------------------------------------------------------------------------------------'
    print 'project_bug_reports :' ;
     set @start_time = GETDATE();

    TRUNCATE TABLE bronze.project_bug_reports_sfax ;
    BULK INSERT bronze.project_bug_reports_sfax
    FROM 'C:\Users\cheik\Desktop\i1\entropot de donné\soft-tech project\datasets\business_data\branche_sfax\project_bug_reports.csv'
    WITH(
    FIRSTROW =2 ,
    FIELDTERMINATOR =',' ,
    tablock 
    );------------------------------tunis source----------------
       TRUNCATE TABLE bronze.project_bug_reports_tunis ;
    BULK INSERT bronze.project_bug_reports_tunis
    FROM 'C:\Users\cheik\Desktop\i1\entropot de donné\soft-tech project\datasets\business_data\branche_tunis\project_bug_reports.csv'
    WITH(
    FIRSTROW =2 ,
    FIELDTERMINATOR =',' ,
    tablock 
    );
    set @end_time = GETDATE();
    print 'load duration ' + cast(DATEDIFF(second,@start_time,@end_time ) AS NVARCHAR) +'seconds' ; 
    print '------------------------------------------------------------------------------------------'

  



     -- ================================ EMPLOYEES & TEAMS ==========================================================================--
    print 'loading employees source :' ; 
    print '---------------------------------------------------------------------------';
    print 'employees :' ;
        set @start_time = GETDATE();
    TRUNCATE TABLE bronze.employees ;  
    BULK INSERT bronze.employees
    FROM 'C:\Users\cheik\Desktop\i1\entropot de donné\soft-tech project\datasets\condidate&employees\employees.csv'
    WITH(
    FIRSTROW =2 ,
    FIELDTERMINATOR =',' ,
    tablock 
    );  
    set @end_time = GETDATE();
    print'---------------------------------------------------------------------------------------------------';
    print 'load duration ' + cast(DATEDIFF(second,@start_time,@end_time ) AS NVARCHAR) +'seconds' ; 
    print '------------------------------------------------------------------------------------------';

    print 'teams :' ;
        set @start_time = GETDATE();

    TRUNCATE TABLE bronze.teams ;
    BULK INSERT bronze.teams
    FROM 'C:\Users\cheik\Desktop\i1\entropot de donné\soft-tech project\datasets\condidate&employees\teams.csv'
    WITH(
    FIRSTROW =2 ,
    FIELDTERMINATOR =',' ,
    tablock 
    );
    print'';
    set @end_time = GETDATE();
    print 'load duration ' + cast(DATEDIFF(second,@start_time,@end_time ) AS NVARCHAR) +'seconds' ; 
    print '------------------------------------------------------------------------------------------';

    print 'employee_performance :' ; 
        set @start_time = GETDATE();

    TRUNCATE TABLE bronze.employee_performance ;
    BULK INSERT bronze.employee_performance
    FROM 'C:\Users\cheik\Desktop\i1\entropot de donné\soft-tech project\datasets\condidate&employees\employee_performance.csv'
    WITH(
    FIRSTROW =2 ,
    FIELDTERMINATOR =',' ,
    tablock 
    );
    set @end_time = GETDATE();
    print '----------------------------------------------------------------------------------------------';
    print 'load duration ' + cast(DATEDIFF(second,@start_time,@end_time ) AS NVARCHAR) +'seconds' ; 
    print '------------------------------------------------------------------------------------------'

    -- ================= CANDIDATES =================
    print 'candidates :' ;
     set @start_time = GETDATE();
    TRUNCATE TABLE bronze.candidates ;
    BULK INSERT bronze.candidates
    FROM 'C:\Users\cheik\Desktop\i1\entropot de donné\soft-tech project\datasets\condidate&employees\candidates.csv'
    WITH(
    FIRSTROW =2 ,
    FIELDTERMINATOR =',' ,
    tablock 
    );
    set @end_time = GETDATE();
    print'';
    print 'load duration ' + cast(DATEDIFF(second,@start_time,@end_time ) AS NVARCHAR) +'seconds' ; 
    print '------------------------------------------------------------------------------------------'

    print 'candidate_cv_raw :' ;
     set @start_time = GETDATE();

    TRUNCATE TABLE bronze.candidate_cv_raw ;
    BULK INSERT bronze.candidate_cv_raw
    FROM 'C:\Users\cheik\Desktop\i1\entropot de donné\soft-tech project\datasets\condidate&employees\candidate_cv_raw.csv'
    WITH(
    FIRSTROW =2 ,
    FIELDTERMINATOR =',' ,
    tablock 
    );
    set @end_time = GETDATE();
    print 'load duration ' + cast(DATEDIFF(second,@start_time,@end_time ) AS NVARCHAR) +'seconds' ; 
    print '------------------------------------------------------------------------------------------'
    print 'candidate_interviews :' ;
     set @start_time = GETDATE();

  print 'candidate_cv_raw_linkedin :' ;
     set @start_time = GETDATE();

    TRUNCATE TABLE bronze.candidate_cv_raw_linkedin ;
    BULK INSERT bronze.candidate_cv_raw_linkedin
    FROM 'C:\Users\cheik\Desktop\i1\entropot de donné\soft-tech project\datasets\condidate&employees\candidate_cv_raw_linkedin.csv'
    WITH(
    FIRSTROW =2 ,
    FIELDTERMINATOR =',' ,
    tablock 
    );
    set @end_time = GETDATE();
    print 'load duration ' + cast(DATEDIFF(second,@start_time,@end_time ) AS NVARCHAR) +'seconds' ; 
    print '------------------------------------------------------------------------------------------'
    print 'candidate_interviews :' ;
     set @start_time = GETDATE();

    TRUNCATE TABLE bronze.candidate_interviews ;
    BULK INSERT bronze.candidate_interviews
    FROM 'C:\Users\cheik\Desktop\i1\entropot de donné\soft-tech project\datasets\condidate&employees\candidate_interviews.csv'
    WITH(
    FIRSTROW =2 ,
    FIELDTERMINATOR =',' ,
    tablock 
    );
    set @end_time = GETDATE();
    print 'load duration ' + cast(DATEDIFF(second,@start_time,@end_time ) AS NVARCHAR) +'seconds' ; 
    print '------------------------------------------------------------------------------------------'
      set @end = GETDATE();
  print 'full load duration ' + cast(DATEDIFF(second,@start,@end ) AS NVARCHAR) +'seconds' ;
    print'========================================================================'
  END TRY 
  BEGIN CATCH 
  PRINT '===================ERROR===============================================';
  print 'ERROR OCCURED IN LOADING BRONZE LAYER '; 
  print 'error message : '+ ERROR_MESSAGE() ; 
  print '===================ERROR===============================================';
  END CATCH 
END
