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
    FROM 'C:\Users\cheik\Desktop\i1\entropot de donné\soft-tech project\business_data\customers.csv'
    WITH(
    FIRSTROW =2 ,
    FIELDTERMINATOR =',' ,
@@ -26,7 +26,7 @@
     set @start_time = GETDATE();
    TRUNCATE TABLE bronze.products ;
    BULK INSERT bronze.products
    FROM 'C:\path\products.csv'
    FROM 'C:\Users\cheik\Desktop\i1\entropot de donné\soft-tech project\business_data\products.csv'
    WITH(
    FIRSTROW =2 ,
    FIELDTERMINATOR =',' ,
@@ -39,7 +39,7 @@
     set @start_time = GETDATE();
    TRUNCATE TABLE bronze.sales_orders ;
    BULK INSERT bronze.sales_orders
    FROM 'C:\path\sales_orders.csv'
    FROM 'C:\Users\cheik\Desktop\i1\entropot de donné\soft-tech project\business_data\sales.csv'
    WITH(
    FIRSTROW =2 ,
    FIELDTERMINATOR =',' ,
@@ -53,7 +53,7 @@
     set @start_time = GETDATE();
    TRUNCATE TABLE bronze.projects ;
    BULK INSERT bronze.projects
    FROM 'C:\path\projects.csv'
    FROM 'C:\Users\cheik\Desktop\i1\entropot de donné\soft-tech project\business_data\projects.csv'
    WITH(
    FIRSTROW =2 ,
    FIELDTERMINATOR =',' ,
@@ -66,7 +66,7 @@
     set @start_time = GETDATE();
    TRUNCATE TABLE bronze.project_bug_reports ;
    BULK INSERT bronze.project_bug_reports
    FROM 'C:\path\project_bug_reports.csv'
    FROM 'C:\Users\cheik\Desktop\i1\entropot de donné\soft-tech project\business_data\project_bug_reports.csv'
    WITH(
    FIRSTROW =2 ,
    FIELDTERMINATOR =',' ,
@@ -86,36 +86,36 @@
        set @start_time = GETDATE();
    TRUNCATE TABLE bronze.employees ;  
    BULK INSERT bronze.employees
    FROM 'C:\path\employees.csv'
    FROM 'C:\Users\cheik\Desktop\i1\entropot de donné\soft-tech project\employees&condidate\employees.csv'
    WITH(
    FIRSTROW =2 ,
    FIELDTERMINATOR =',' ,
    tablock 
    );  
    set @end_time = GETDATE();
    print'';
    print'---------------------------------------------------------------------------------------------------';
    print 'load duration ' + cast(DATEDIFF(second,@start_time,@end_time ) AS NVARCHAR) +'seconds' ; 
    print '------------------------------------------------------------------------------------------'
    print '------------------------------------------------------------------------------------------';

    print 'teams :' ;
        set @start_time = GETDATE();
    TRUNCATE TABLE bronze.teams ;
    BULK INSERT bronze.teams
    FROM 'C:\path\teams.csv'
    FROM 'C:\Users\cheik\Desktop\i1\entropot de donné\soft-tech project\employees&condidate\teams.csv'
    WITH(
    FIRSTROW =2 ,
    FIELDTERMINATOR =',' ,
    tablock 
    );print'';
    set @end_time = GETDATE();
    print 'load duration ' + cast(DATEDIFF(second,@start_time,@end_time ) AS NVARCHAR) +'seconds' ; 
    print '------------------------------------------------------------------------------------------'
    print '------------------------------------------------------------------------------------------';

    print 'employee_performance :' ; 
        set @start_time = GETDATE();
    TRUNCATE TABLE bronze.employee_performance ;
    BULK INSERT bronze.employee_performance
    FROM 'C:\path\employee_performance.csv'
    FROM 'C:\Users\cheik\Desktop\i1\entropot de donné\soft-tech project\employees&condidate\employee_performance.csv'
    WITH(
    FIRSTROW =2 ,
    FIELDTERMINATOR =',' ,
@@ -130,7 +130,7 @@
     set @start_time = GETDATE();
    TRUNCATE TABLE bronze.candidates ;
    BULK INSERT bronze.candidates
    FROM 'C:\path\candidates.csv'
    FROM 'C:\Users\cheik\Desktop\i1\entropot de donné\soft-tech project\employees&condidate\candidates.csv'
    WITH(
    FIRSTROW =2 ,
    FIELDTERMINATOR =',' ,
@@ -143,7 +143,7 @@
     set @start_time = GETDATE();
    TRUNCATE TABLE bronze.candidate_cv_raw ;
    BULK INSERT bronze.candidate_cv_raw
    FROM 'C:\path\candidate_cv_raw.csv'
    FROM 'C:\Users\cheik\Desktop\i1\entropot de donné\soft-tech project\employees&condidate\candidate_cv_raw.csv'
    WITH(
    FIRSTROW =2 ,
    FIELDTERMINATOR =',' ,
@@ -156,22 +156,22 @@
     set @start_time = GETDATE();
    TRUNCATE TABLE bronze.candidate_interviews ;
    BULK INSERT bronze.candidate_interviews
    FROM 'C:\path\candidate_interviews.csv'
    FROM 'C:\Users\cheik\Desktop\i1\entropot de donné\soft-tech project\employees&condidate\candidate_interviews.csv'
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
