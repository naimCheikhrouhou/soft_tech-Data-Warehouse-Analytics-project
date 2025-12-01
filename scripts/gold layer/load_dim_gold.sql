delete from gold.dim_customers; print 'TRUNCATED';
INSERT INTO gold.dim_customers(customer_id, company_name, country, created_at)
SELECT 
    customer_id,
    company_name,
    country,
    created_at
FROM silver.customers; print 'INSERTED IN DIM_CUSTOMERS';
------------------------------------------------------------------------------------------------------------------------------------

delete from gold.dim_product;print 'TRUNCATED';

INSERT INTO gold.dim_product(product_id, product_name, category, base_price, development_hours, version)
SELECT 
    product_id, product_name, category, base_price, development_hours, [version]

FROM silver.products;print 'INSERTED IN DIM_PRODUCT';
--------------------------------------------------------------------------------------------------------------------------

delete from gold.dim_project;print 'TRUNCATED';

INSERT INTO gold.dim_project(project_id, project_name, status)
SELECT project_id, project_name, status

FROM silver.projects;print 'INSERTED IN DIM_PROJECT';


delete from gold.dim_employee;print 'TRUNCATED';

INSERT INTO gold.dim_employee(employee_id, full_name, role, hire_date, salary, seniority, email, team_id)
SELECT 
    employee_id, full_name, role, hire_date, salary, seniority, email, team_id

FROM silver.employees;print 'INSERTED IN DIM_EMPLOYEE';


delete from gold.dim_teams;print 'TRUNCATED';

INSERT INTO gold.dim_teams(team_id, team_name, department, manager_team_id)
SELECT 
    team_id, team_name, department, manager_id

FROM silver.team;print 'INSERTED IN DIM_TEAM';


delete from gold.dim_candidates;print 'TRUNCATED';
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
       STRING_SPLIT ne fonctionne pas avec CRLF ? on remplace par ' '
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
    ON c.candidate_id = cv2.candidate_id;    print 'INSERTED IN DIM_CANDIDATES';
