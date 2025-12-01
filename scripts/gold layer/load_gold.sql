

truncate table gold.dim_customers;
INSERT INTO gold.dim_customers(customer_id, company_name, country, created_at)
SELECT 
    customer_id,
    company_name,
    country,
    created_at
FROM silver.customers;
truncate table gold.dim_product;
INSERT INTO gold.dim_product(product_id, product_name, category, base_price, development_hours, version)
SELECT 
    product_id, product_name, category, base_price, development_hours, version
FROM silver.products;
truncate table gold.dim_project;
INSERT INTO gold.dim_project(project_id, project_name, status)
SELECT project_id, project_name, status
FROM silver.projects;
truncate table gold.dim_employee;
INSERT INTO gold.dim_employee(employee_id, full_name, role, hire_date, salary, seniority, email, team_id)
SELECT 
    employee_id, full_name, role, hire_date, salary, seniority, email, team_id
FROM silver.employees;
truncate table gold.dim_team;
INSERT INTO gold.dim_team(team_id, team_name, department, manager_team_id)
SELECT 
    team_id, team_name, department, manager_id
FROM silver.teams;
TRUNCATE TABLE gold.dim_candidates;

INSERT INTO gold.dim_candidates
(
    candidate_id, 
    full_name, 
    email, 
    phone, 
    degree, 
    years_experience, 
    tech_stack,
    certifications, 
    application_date, 
    cv_text, 
    parsed_keywords, 
    language_detected,
    uploaded_date_cv,
    age,
    university
)
SELECT 
    c.candidate_id,
    c.full_name,
    c.email,
    c.phone,
    c.degree,
    c.years_experience,
    c.tech_stack,
    c.certifications,
    c.application_date,

    /* ====================================================
       FUSION DÉDOUBLONNÉE DU CV TEXTE
       STRING_SPLIT ne fonctionne pas avec CRLF → on remplace par ' '
    ==================================================== */
    (
        SELECT STRING_AGG(value, CHAR(13) + CHAR(10))
        FROM (
            SELECT DISTINCT TRIM(value) AS value
            FROM STRING_SPLIT(
                REPLACE(
                    ISNULL(
                        CAST(
                            CONCAT(
                                NULLIF(CAST(cv1.cv_text AS NVARCHAR(MAX)), ''),
                                ' ',
                                NULLIF(CAST(cv2.cv_text AS NVARCHAR(MAX)), '')
                            ) AS NVARCHAR(MAX)
                        ), 
                    ''), 
                CHAR(13) + CHAR(10), ' '),       -- Remplace CRLF par espace
                ' '                              -- STRING_SPLIT separator (1 char OBLIGATOIRE)
            )
        ) x
    ) AS cv_text,

    /* ====================================================
       FUSION DES KEYWORDS SANS DOUBLONS
    ==================================================== */
    (
        SELECT STRING_AGG(value, ', ')
        FROM (
            SELECT DISTINCT TRIM(value) AS value
            FROM STRING_SPLIT(
                ISNULL(
                    CAST(
                        CONCAT(
                            NULLIF(CAST(cv1.parsed_keywords AS NVARCHAR(MAX)), ''),
                            ',',
                            NULLIF(CAST(cv2.parsed_keywords AS NVARCHAR(MAX)), '')
                        ) AS NVARCHAR(MAX)
                    ),
                ''), 
                ','   -- Séparateur valide (1 seul caractère)
            )
        ) y
    ) AS parsed_keywords,

    COALESCE(cv1.language_detected, cv2.language_detected) AS language_detected,
    COALESCE(cv1.uploaded_date, cv2.uploaded_date) AS uploaded_date_cv,
    cv2.age,
    cv2.university

FROM silver.candidates c
LEFT JOIN silver.candidate_cv_raw cv1 
    ON c.candidate_id = cv1.candidate_id
LEFT JOIN silver.candidate_cv_raw_linkedin cv2
    ON c.candidate_id = cv2.candidate_id;


    TRUNCATE TABLE gold.fact_products_sales;
INSERT INTO gold.fact_products_sales(product_key, customer_key, sale_date_key, quantity, total_price)
SELECT 
    dp.product_key,
    dc.customer_key,
    CONVERT(INT, FORMAT(so.sale_date, 'yyyyMMdd')),
    so.quantity,
    so.total_price
FROM silver.sales_orders so
JOIN gold.dim_product dp ON dp.product_id = so.product_id
JOIN gold.dim_customers dc ON dc.customer_id = so.customer_id;
TRUNCATE TABLE gold.fact_project_suivis;
INSERT INTO gold.fact_project_suivis
(
    project_key, customer_id, manager_project_id,
    start_date_key, end_date_key, delivered_date_key,
    estimated_cost, actual_cost
)
SELECT 
    dp.project_key,
    p.customer_id,
    p.project_manager_id,
    CONVERT(INT, FORMAT(p.start_date, 'yyyyMMdd')),
    CONVERT(INT, FORMAT(p.end_date, 'yyyyMMdd')),
    CONVERT(INT, FORMAT(p.delivered_date, 'yyyyMMdd')),
    p.estimated_cost,
    p.actual_cost
FROM silver.projects p
JOIN gold.dim_project dp ON dp.project_id = p.project_id;

TRUNCATE TABLE gold.fact_employee_performance;
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
JOIN gold.dim_employee de ON de.employee_id = ep.employee_id;

TRUNCATE TABLE gold.fact_interview;
INSERT INTO gold.fact_interview
(
    candidate_key, employee_key, team_id,
    interview_date_key, interview_score, result
)
SELECT 
    dc.candidate_key,
    de.employee_key,
    ci.team_id,
    CONVERT(INT, FORMAT(CAST(GETDATE() AS DATE), 'yyyyMMdd')),
    ci.interview_score,
    ci.result
FROM silver.candidate_interviews ci
JOIN gold.dim_candidates dc ON dc.candidate_id = ci.candidate_id
JOIN gold.dim_employee de ON de.employee_id = ci.recruiter_id;
