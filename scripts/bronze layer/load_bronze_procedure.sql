CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN 
DECLARE @start_time DATETIME ,@end_time DATETIME,@start DATETIME ,@end DATETIME ; 
   set @start = GETDATE();
  BEGIN TRY 
    print '==========================================================================';
    print 'loading the data from sources ' ;
    print '==========================================================================';


    -- ================= CUSTOMERS & PRODUCTS & SALES =================
    print 'customers :' ;
     set @start_time = GETDATE();
    TRUNCATE TABLE bronze.customers ;
    BULK INSERT bronze.customers
    FROM 'C:\path\customers.csv'
    WITH(
    FIRSTROW =2 ,
    FIELDTERMINATOR =',' ,
    tablock 
    );set @end_time = GETDATE();
    print 'load duration ' + cast(DATEDIFF(second,@start_time,@end_time ) AS NVARCHAR) +'seconds' ; 
    print '------------------------------------------------------------------------------------------'

    print 'products :' ;
     set @start_time = GETDATE();
    TRUNCATE TABLE bronze.products ;
    BULK INSERT bronze.products
    FROM 'C:\path\products.csv'
    WITH(
    FIRSTROW =2 ,
    FIELDTERMINATOR =',' ,
    tablock 
    );set @end_time = GETDATE();
    print 'load duration ' + cast(DATEDIFF(second,@start_time,@end_time ) AS NVARCHAR) +'seconds' ; 
    print '------------------------------------------------------------------------------------------'

    print 'sales_orders :' ;
     set @start_time = GETDATE();
    TRUNCATE TABLE bronze.sales_orders ;
    BULK INSERT bronze.sales_orders
    FROM 'C:\path\sales_orders.csv'
    WITH(
    FIRSTROW =2 ,
    FIELDTERMINATOR =',' ,
    tablock 
    );set @end_time = GETDATE();
    print 'load duration ' + cast(DATEDIFF(second,@start_time,@end_time ) AS NVARCHAR) +'seconds' ; 
    print '------------------------------------------------------------------------------------------'

    -- ================= PROJECTS & BUG REPORTS =================
    print 'projects :' ;
     set @start_time = GETDATE();
    TRUNCATE TABLE bronze.projects ;
    BULK INSERT bronze.projects
    FROM 'C:\path\projects.csv'
    WITH(
    FIRSTROW =2 ,
    FIELDTERMINATOR =',' ,
    tablock 
    );set @end_time = GETDATE();
    print 'load duration ' + cast(DATEDIFF(second,@start_time,@end_time ) AS NVARCHAR) +'seconds' ; 
    print '------------------------------------------------------------------------------------------'

    print 'project_bug_reports :' ;
     set @start_time = GETDATE();
    TRUNCATE TABLE bronze.project_bug_reports ;
    BULK INSERT bronze.project_bug_reports
    FROM 'C:\path\project_bug_reports.csv'
    WITH(
    FIRSTROW =2 ,
    FIELDTERMINATOR =',' ,
    tablock 
    );set @end_time = GETDATE();
    print 'load duration ' + cast(DATEDIFF(second,@start_time,@end_time ) AS NVARCHAR) +'seconds' ; 
    print '------------------------------------------------------------------------------------------'

  



     -- ================================ EMPLOYEES & TEAMS ==========================================================================--
    print 'loading employees source :' ; 
    print '---------------------------------------------------------------------------';
    print 'employees :' ;
        set @start_time = GETDATE();
    TRUNCATE TABLE bronze.employees ;  
    BULK INSERT bronze.employees
    FROM 'C:\path\employees.csv'
    WITH(
    FIRSTROW =2 ,
    FIELDTERMINATOR =',' ,
    tablock 
    );  
    set @end_time = GETDATE();
    print'';
    print 'load duration ' + cast(DATEDIFF(second,@start_time,@end_time ) AS NVARCHAR) +'seconds' ; 
    print '------------------------------------------------------------------------------------------'

    print 'teams :' ;
        set @start_time = GETDATE();
    TRUNCATE TABLE bronze.teams ;
    BULK INSERT bronze.teams
    FROM 'C:\path\teams.csv'
    WITH(
    FIRSTROW =2 ,
    FIELDTERMINATOR =',' ,
    tablock 
    );print'';
    set @end_time = GETDATE();
    print 'load duration ' + cast(DATEDIFF(second,@start_time,@end_time ) AS NVARCHAR) +'seconds' ; 
    print '------------------------------------------------------------------------------------------'

    print 'employee_performance :' ; 
        set @start_time = GETDATE();
    TRUNCATE TABLE bronze.employee_performance ;
    BULK INSERT bronze.employee_performance
    FROM 'C:\path\employee_performance.csv'
    WITH(
    FIRSTROW =2 ,
    FIELDTERMINATOR =',' ,
    tablock 
    );set @end_time = GETDATE();
    print '';
    print 'load duration ' + cast(DATEDIFF(second,@start_time,@end_time ) AS NVARCHAR) +'seconds' ; 
    print '------------------------------------------------------------------------------------------'

    -- ================= CANDIDATES =================
    print 'candidates :' ;
     set @start_time = GETDATE();
    TRUNCATE TABLE bronze.candidates ;
    BULK INSERT bronze.candidates
    FROM 'C:\path\candidates.csv'
    WITH(
    FIRSTROW =2 ,
    FIELDTERMINATOR =',' ,
    tablock 
    );print'';set @end_time = GETDATE();
    print 'load duration ' + cast(DATEDIFF(second,@start_time,@end_time ) AS NVARCHAR) +'seconds' ; 
    print '------------------------------------------------------------------------------------------'

    print 'candidate_cv_raw :' ;
     set @start_time = GETDATE();
    TRUNCATE TABLE bronze.candidate_cv_raw ;
    BULK INSERT bronze.candidate_cv_raw
    FROM 'C:\path\candidate_cv_raw.csv'
    WITH(
    FIRSTROW =2 ,
    FIELDTERMINATOR =',' ,
    tablock 
    );set @end_time = GETDATE();
    print 'load duration ' + cast(DATEDIFF(second,@start_time,@end_time ) AS NVARCHAR) +'seconds' ; 
    print '------------------------------------------------------------------------------------------'

    print 'candidate_interviews :' ;
     set @start_time = GETDATE();
    TRUNCATE TABLE bronze.candidate_interviews ;
    BULK INSERT bronze.candidate_interviews
    FROM 'C:\path\candidate_interviews.csv'
    WITH(
    FIRSTROW =2 ,
    FIELDTERMINATOR =',' ,
    tablock 
    );set @end_time = GETDATE();
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
