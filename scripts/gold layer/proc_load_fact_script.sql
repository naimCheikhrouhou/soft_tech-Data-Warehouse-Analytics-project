CREATE OR ALTER PROCEDURE gold.load_fact_tables AS
BEGIN 
    delete from gold.fact_products_sales; PRINT 'DELETED';
    INSERT INTO gold.fact_products_sales(product_key, customer_key, sale_date_key, quantity, total_price)
    SELECT 
        dp.product_key,
        dc.customer_key,
        CONVERT(INT, FORMAT(so.sale_date, 'yyyyMMdd')),
        so.quantity,
        so.total_price
    FROM silver.sales_orders so
    JOIN gold.dim_product dp ON dp.product_id = so.product_id
    JOIN gold.dim_customers dc ON dc.customer_id = so.customer_id; PRINT' INSERTED IN FACT_PRODUCTS_SALES';


    DELETE FROM gold.fact_project_suivis; PRINT 'DELETED'
    INSERT INTO gold.fact_project_suivis
    (
        project_key,
        customer_key,
        manager_project_key,
        start_date_key,
        end_date_key,
        delivered_date_key,
        estimated_cost,
        actual_cost
    )
    SELECT 
        dp.project_key,               -- FK vers dim_project
        dc.customer_key,              -- FK vers dim_customers
        de.employee_key,              -- FK vers dim_employee
        CONVERT(INT, FORMAT(p.start_date, 'yyyyMMdd')),
        CONVERT(INT, FORMAT(p.end_date, 'yyyyMMdd')),
        CONVERT(INT, FORMAT(p.delivered_date, 'yyyyMMdd')),
        p.estimated_cost,
        p.actual_cost
    FROM silver.projects p
    JOIN gold.dim_project dp
        ON dp.project_id = p.project_id
    LEFT JOIN gold.dim_customers dc
        ON dc.customer_id = p.customer_id          -- correct FK
    LEFT JOIN gold.dim_employee de
        ON de.employee_id = p.project_manager_id;  -- correct FK  
        PRINT' INSERTED INTO fact_project_suivis'


    delete from gold.fact_employee_performance; PRINT 'DELETED'
    INSERT INTO gold.fact_employee_performance
    (
        employee_key, start_date_key, end_date_key,
        tasks_completed, overtime_hours, performance_score, project_success_rate
    )
    SELECT 
        de.employee_key,
        CONVERT(INT, FORMAT(ep.start_date, 'yyyyMMdd')),
        CONVERT(INT, FORMAT(ep.end_date, 'yyyyMMdd')),
        ep.tasks_completed,
        ep.overtime_hours,
        ep.performance_score,
        ep.project_success_rate
    FROM silver.employee_performance ep
    JOIN gold.dim_employee de ON de.employee_id = ep.employee_id; PRINT 'INSERTED IN employee_performance';
     PRINT ' INSERTED INTO fact_employee_performance'


    DELETE FROM gold.fact_interview;
    INSERT INTO gold.fact_interview
    (
        candidate_key,
        employee_key,
        team_key,
        interview_date_key,
        interview_score,
        result
    )
    SELECT 
        dc.candidate_key,                   -- FK dim_candidates
        de.employee_key,                    -- FK dim_employee
        dt.team_key,                        -- FK dim_team
       CONVERT(INT, FORMAT(CAST(GETDATE() AS DATE), 'yyyyMMdd')), -- date réelle
        ci.interview_score,
        ci.result
    FROM silver.candidate_interviews ci
    JOIN gold.dim_candidates dc
        ON dc.candidate_id = ci.candidate_id
    JOIN gold.dim_employee de
        ON de.employee_id = ci.recruiter_id
    LEFT JOIN gold.dim_team dt
        ON dt.team_id = ci.team_id; PRINT 'INSERTED INTO fact_interview' ; 
END 
