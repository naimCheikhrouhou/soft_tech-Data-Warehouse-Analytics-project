-- 1. TABLES CLIENTS / SALES
-- ========================================

DROP TABLE IF EXISTS bronze.customers;
CREATE TABLE bronze.customers (
    customer_id INT ,
    customer_id INT,
    company_name VARCHAR(100),
    contact_name VARCHAR(100),
    email VARCHAR(100),
    phone VARCHAR(20),
    created_at DATE,
    country VARCHAR(50)
    
);

DROP TABLE IF EXISTS bronze.products;
CREATE TABLE bronze.products (
    product_id INT ,
    product_name VARCHAR(100) ,
    product_id INT,
    product_name VARCHAR(100),
    category VARCHAR(50),
    base_price DECIMAL(10,2),
    development_hours INT,
    version VARCHAR(20)
);

DROP TABLE IF EXISTS bronze.sales_orders;
CREATE TABLE bronze.sales_orders (
    sale_id INT ,
    customer_id INT ,
    product_id INT ,
    sale_id INT,
    customer_id INT,
    product_id INT,
    quantity INT,
    total_price DECIMAL(10,2),
    payment_method VARCHAR(50),
--================================
-- 2. TABLES PROJETS / BUGS
-- ========================================

DROP TABLE IF EXISTS bronze.projects;
CREATE TABLE bronze.projects (
    project_id INT ,
    customer_id INT ,
    project_id INT,
    customer_id INT,
    project_name VARCHAR(100),
    start_date DATE,
    end_date DATE,
    status VARCHAR(20)
);

DROP TABLE IF EXISTS bronze.project_bug_reports;
CREATE TABLE bronze.project_bug_reports (
    bug_id INT,
    project_id INT ,
    project_id INT,
    severity VARCHAR(20),
    description TEXT,
    reported_date DATE,
    resolved BIT
);

-- ==========================================
-- 3. TABLES EMPLOYEES / TEAMS / PERFORMANCE_________________________________________________________________________________________________________________________________________________
-- ==========================================

DROP TABLE IF EXISTS bronze.teams;
CREATE TABLE bronze.teams (
    team_id INT,
    team_name VARCHAR(50),
    department VARCHAR(50),
    manager_id INT
);

DROP TABLE IF EXISTS bronze.employees;
CREATE TABLE bronze.employees (
    employee_id INT ,
    employee_id INT,
    full_name VARCHAR(100),
    role VARCHAR(50),
    team_id INT ,
    team_id INT,
    hire_date DATE,
    salary DECIMAL(10,2),
    seniority VARCHAR(20),
    email VARCHAR(100)
);

DROP TABLE IF EXISTS bronze.employee_performance;
CREATE TABLE bronze.employee_performance (
    perf_id INT ,
    perf_id INT,
    employee_id INT,
    month INT,
    year INT,
-- 4. TABLES RECRUTEMENT / CANDIDATS
-- ========================================

DROP TABLE IF EXISTS bronze.candidates;
CREATE TABLE bronze.candidates (
    candidate_id INT,
    full_name VARCHAR(100),
    application_date DATE
);

DROP TABLE IF EXISTS bronze.candidate_cv_raw;
CREATE TABLE bronze.candidate_cv_raw (
    cv_id INT,
    candidate_id INT ,
    candidate_id INT,
    cv_text TEXT,
    parsed_keywords TEXT,
    language_detected VARCHAR(20),
    uploaded_date DATE
);

DROP TABLE IF EXISTS bronze.candidate_interviews;
CREATE TABLE bronze.candidate_interviews (
    interview_id INT,
    candidate_id INT ,
    recruiter_id INT ,
    candidate_id INT,
    recruiter_id INT,
    interview_score INT,
    notes TEXT,
    stage VARCHAR(50),
    result VARCHAR(20)
);
