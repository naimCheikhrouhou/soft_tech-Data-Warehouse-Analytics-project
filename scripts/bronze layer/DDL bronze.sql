-- ========================================
-- 1. TABLES CLIENTS / SALES   ________________________________________________(tunis_source ) _______________________
-- ========================================

DROP TABLE IF EXISTS bronze.customers_tunis;
CREATE TABLE bronze.customers_tunis (
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
    product_id INT,
    product_name VARCHAR(100),
    category VARCHAR(50),
    base_price DECIMAL(10,2),
    development_hours INT,
    version VARCHAR(20)
);

DROP TABLE IF EXISTS bronze.sales_orders_tunis;
CREATE TABLE bronze.sales_orders_tunis(
    sale_id INT,
    customer_id INT,
    product_id INT,
    quantity INT,
    total_price DECIMAL(10,2),
    payment_method VARCHAR(50),
    sale_date DATE
);

-- ========================================
-- 2. TABLES PROJETS / BUGS
-- ========================================

DROP TABLE IF EXISTS bronze.projects_tunis;
CREATE TABLE bronze.projects_tunis (
    project_id INT,
    customer_id INT,
    project_name VARCHAR(100),
    start_date DATE,
    end_date DATE,
    delivered_date DATE,
    project_manager INT,
    estimated_cost DECIMAL(10,2),
    actual_cost DECIMAL(10,2),
    status VARCHAR(20)
);

DROP TABLE IF EXISTS bronze.project_bug_reports_tunis;
CREATE TABLE bronze.project_bug_reports_tunis (
    bug_id INT,
    project_id INT,
    severity VARCHAR(20),
    description TEXT,
    reported_date DATE,
    resolved BIT
);

-- ==========================================
-- 3. TABLES EMPLOYEES / TEAMS / PERFORMANCE_______________________________________________________________________________
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
    employee_id INT,
    full_name VARCHAR(100),
    role VARCHAR(50),
    team_id INT,
    hire_date DATE,
    salary DECIMAL(10,2),
    seniority VARCHAR(20),
    email VARCHAR(100)
);

DROP TABLE IF EXISTS bronze.employee_performance;
CREATE TABLE bronze.employee_performance(
    perf_id INT,
    employee_id INT,
    start_date date ,
    end_date date,
    tasks_completed INT,
    overtime_hours INT,
    performance_score INT,
    project_success_rate DECIMAL(5,2)
);

--                                         ========================================
--                                            4. TABLES RECRUTEMENT / CANDIDATS  
--                                            ========================================

DROP TABLE IF EXISTS bronze.candidates;
CREATE TABLE bronze.candidates (
    candidate_id INT,
    full_name VARCHAR(100),
    email VARCHAR(100),
    phone VARCHAR(20),
    degree VARCHAR(50),
    years_experience INT,
    tech_stack TEXT,
    certifications TEXT,
    application_date DATE
);

DROP TABLE IF EXISTS bronze.candidate_cv_raw;
CREATE TABLE bronze.candidate_cv_raw ( 
    cv_id TEXT,
    candidate_id INT,
    cv_text TEXT,
    parsed_keywords TEXT,
    language_detected VARCHAR(20),
    uploaded_date TEXT
);

DROP TABLE IF EXISTS bronze.candidate_cv_raw_linkedin;
CREATE TABLE bronze.candidate_cv_raw_linkedin ( 
    cv_id TEXT,
    candidate_id INT,
    cv_text TEXT,
    parsed_keywords TEXT,
    language_detected VARCHAR(20),
    uploaded_date TEXT ,
    age int ,
    university nvarchar(50)
);

DROP TABLE IF EXISTS bronze.candidate_interviews;
CREATE TABLE bronze.candidate_interviews (
    interview_id INT,
    candidate_id INT,
    recruiter_id INT,
    interview_score INT,
    notes TEXT,
    team_id VARCHAR(50),
    result VARCHAR(20)
);
-- ========================================
-- 1. TABLES CLIENTS / SALES_________________________________________________________________________( SFAX SOURCE )
-- ========================================

DROP TABLE IF EXISTS bronze.customers_sfax;
CREATE TABLE bronze.customers_sfax (
    customer_id INT,
    company_name VARCHAR(100),
    contact_name VARCHAR(100),
    email VARCHAR(100),
    phone VARCHAR(20),
    created_at DATE,
    country VARCHAR(50)
);

DROP TABLE IF EXISTS bronze.sales_orders_sfax;
CREATE TABLE bronze.sales_orders_sfax (
    sale_id INT,
    customer_id INT,
    product_id INT,
    quantity INT,
    total_price DECIMAL(10,2),
    payment_method VARCHAR(50),
    sale_date DATE
);

-- ========================================
-- 2. TABLES PROJETS / BUGS
-- ========================================

DROP TABLE IF EXISTS bronze.projects_sfax;
CREATE TABLE bronze.projects_sfax (
    project_id INT,
    customer_id INT,
    project_name VARCHAR(100),
    start_date DATE,
    end_date DATE,
    delivered_date DATE,
    project_manager INT,
    estimated_cost DECIMAL(10,2),
    actual_cost DECIMAL(10,2),
    status VARCHAR(20)
);

DROP TABLE IF EXISTS bronze.project_bug_reports_sfax;
CREATE TABLE bronze.project_bug_reports_sfax (
    bug_id INT,
    project_id INT,
    severity VARCHAR(20),
    description TEXT,
    reported_date DATE,
    resolved BIT
);


