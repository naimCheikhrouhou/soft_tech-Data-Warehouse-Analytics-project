select* from bronze.project_bug_reports;
-- ========================================
-- 1. TABLES CLIENTS / SALES
-- ========================================
drop table bronze.customers;
CREATE TABLE bronze.customers_branche_tunis (
    customer_id INT ,
    company_name VARCHAR(100),
    contact_name VARCHAR(100),
    email VARCHAR(100),
    phone VARCHAR(20),
    created_at DATE,
    country VARCHAR(50)
    
);
CREATE TABLE bronze.customers_branche_france (
    customer_id INT ,
    company_name VARCHAR(100),
    contact_name VARCHAR(100),
    email VARCHAR(100),
    phone VARCHAR(20),
    created_at DATE,
    country VARCHAR(50)
    
);

CREATE TABLE bronze.products (
    product_id INT ,
    product_name VARCHAR(100) ,
    category VARCHAR(50),
    base_price DECIMAL(10,2),
    development_hours INT,
    version VARCHAR(20)
);

CREATE TABLE bronze.sales_orders (
    sale_id INT ,
    customer_id INT ,
    product_id INT ,
    quantity INT,
    total_price DECIMAL(10,2),
    payment_method VARCHAR(50),
    sale_date DATE
);

-- ========================================
-- 2. TABLES PROJETS / BUGS
-- ========================================

CREATE TABLE bronze.projects (
    project_id INT ,
    customer_id INT ,
    project_name VARCHAR(100),
    start_date DATE,
    end_date DATE,
    delivered_date DATE,
    project_manager INT,
    estimated_cost DECIMAL(10,2),
    actual_cost DECIMAL(10,2),
    status VARCHAR(20)
);

CREATE TABLE bronze.project_bug_reports (
    bug_id INT,
    project_id INT ,
    severity VARCHAR(20),
    description TEXT,
    reported_date DATE,
    resolved BIT
);

-- ==========================================
-- 3. TABLES EMPLOYEES / TEAMS / PERFORMANCE_________________________________________________________________________________________________________________________________________________
-- ==========================================

CREATE TABLE bronze.teams (
    team_id INT,
    team_name VARCHAR(50),
    department VARCHAR(50),
    manager_id INT
);

CREATE TABLE bronze.employees (
    employee_id INT ,
    full_name VARCHAR(100),
    role VARCHAR(50),
    team_id INT ,
    hire_date DATE,
    salary DECIMAL(10,2),
    seniority VARCHAR(20),
    email VARCHAR(100)
);

CREATE TABLE bronze.employee_performance (
    perf_id INT ,
    employee_id INT,
    month INT,
    year INT,
    tasks_completed INT,
    overtime_hours INT,
    performance_score INT,
    project_success_rate DECIMAL(5,2)
);

-- ========================================
-- 4. TABLES RECRUTEMENT / CANDIDATS
-- ========================================
drop table bronze.candidates ;
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
drop table bronze.candidate_cv_raw;
CREATE TABLE bronze.candidate_cv_raw (
    cv_id INT,
    candidate_id INT ,
    cv_text TEXT,
    parsed_keywords TEXT,
    language_detected VARCHAR(50),
    uploaded_date DATE
);
drop table bronze.candidate_interviews;
CREATE TABLE bronze.candidate_interviews (
    interview_id INT,
    candidate_id INT ,
    recruiter_id INT ,
    interview_score INT,
    notes TEXT,
    team_id int,
    result VARCHAR(20)
);
CREATE TABLE silver.customers (
    customer_id INT PRIMARY KEY,
    company_name VARCHAR(100) NOT NULL,
    contact_name VARCHAR(100),
    email VARCHAR(100),
    phone int,
    
    country VARCHAR(50),
    created_at DATE,
                                                                    --meta data
    record_created_at DATETIME DEFAULT GETDATE(),
    record_created_by VARCHAR(50) DEFAULT SYSTEM_USER
    
);
select top 1000 * from silver.customers;
DELETE FROM silver.customers
WHERE contact_name IS NULL
  AND email IS NULL;

