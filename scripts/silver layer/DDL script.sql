-- ========================================
-- 1. TABLES CLIENTS / SALES
-- ========================================

DROP TABLE IF EXISTS silver.customers;
CREATE TABLE silver.customers (
    customer_id INT PRIMARY KEY,
    company_name VARCHAR(100) NOT NULL,
    contact_name VARCHAR(100),
    email VARCHAR(100),
    phone VARCHAR(20),
    country VARCHAR(50),
    created_at DATE,

    -- META
    record_created_at DATETIME DEFAULT GETDATE(),
    record_created_by VARCHAR(50) DEFAULT SYSTEM_USER,
    source VARCHAR(50) DEFAULT ''
);


DROP TABLE IF EXISTS silver.products;
CREATE TABLE silver.products (
    product_id INT PRIMARY KEY,
    product_name VARCHAR(100) NOT NULL,
    category VARCHAR(50),
    base_price DECIMAL(10,2),
    development_hours INT,
    version VARCHAR(20),

    -- META
    record_created_at DATETIME DEFAULT GETDATE(),
    record_created_by VARCHAR(50) DEFAULT SYSTEM_USER,
    source VARCHAR(225) DEFAULT 'dataset/buisiness_data/products.csv/bronze.products'
);


DROP TABLE IF EXISTS silver.sales_orders;
CREATE TABLE silver.sales_orders (
    sale_id INT PRIMARY KEY,
    customer_id INT,
    product_id INT,
    quantity INT,
    total_price DECIMAL(10,2),
    payment_method VARCHAR(50),
    sale_date DATE,

    -- META
    record_created_at DATETIME DEFAULT GETDATE(),
    record_created_by VARCHAR(50) DEFAULT SYSTEM_USER,
    source VARCHAR(225) DEFAULT 'dataset/buisiness_data/sales.csv/bronze.customers'
);



-- ========================================
-- 2. TABLES TEAMS / EMPLOYEES / PROJECTS / BUGS
-- ========================================

DROP TABLE IF EXISTS silver.teams;
CREATE TABLE silver.teams (
    team_id INT PRIMARY KEY,
    team_name VARCHAR(50),
    department VARCHAR(50),
    manager_id INT,

    -- META
    record_created_at DATETIME DEFAULT GETDATE(),
    record_created_by VARCHAR(50) DEFAULT SYSTEM_USER,
    source VARCHAR(225) DEFAULT 'dataset/employees&condidate/teams.csv/bronze.customers'
);


DROP TABLE IF EXISTS silver.employees;
CREATE TABLE silver.employees (
    employee_id INT PRIMARY KEY,
    full_name VARCHAR(100),
    role VARCHAR(50),
    team_id INT,
    hire_date DATE,
    salary DECIMAL(10,2),
    seniority VARCHAR(20),
    email VARCHAR(100),

    -- META
    record_created_at DATETIME DEFAULT GETDATE(),
    record_created_by VARCHAR(50) DEFAULT SYSTEM_USER,
    source VARCHAR(225) DEFAULT 'dataset/employees&condidate/employees.csv/bronze.customers'
);


DROP TABLE IF EXISTS silver.projects;
CREATE TABLE silver.projects (
    project_id INT PRIMARY KEY,
    customer_id INT,
    project_name VARCHAR(100),
    start_date DATE,
    end_date DATE,
    delivered_date DATE,
    project_manager_id INT,
    estimated_cost DECIMAL(10,2),
    actual_cost DECIMAL(10,2),
    status VARCHAR(20),

    -- META
    record_created_at DATETIME DEFAULT GETDATE(),
    record_created_by VARCHAR(50) DEFAULT SYSTEM_USER,
    source VARCHAR(225) DEFAULT 'dataset/buisiness_data/projects.csv/bronze.customers'
);


DROP TABLE IF EXISTS silver.project_bug_reports;
CREATE TABLE silver.project_bug_reports (
    bug_id INT PRIMARY KEY,
    project_id INT,
    severity VARCHAR(20),
    description TEXT,
    reported_date DATE,
    resolved BIT,

    -- META
    record_created_at DATETIME DEFAULT GETDATE(),
    record_created_by VARCHAR(50) DEFAULT SYSTEM_USER,
    source VARCHAR(225) DEFAULT 'dataset/buisiness_data/project_bug_reports.csv/bronze.customers'
);



-- ========================================
-- 3. TABLES PERFORMANCE
-- ========================================

DROP TABLE IF EXISTS silver.employee_performance;
CREATE TABLE silver.employee_performance (
    perf_id INT PRIMARY KEY,
    employee_id INT,
    start_date DATE,
    end_date DATE,
    tasks_completed INT,
    overtime_hours INT,
    performance_score INT,
    project_success_rate DECIMAL(5,2),

    -- META
    record_created_at DATETIME DEFAULT GETDATE(),
    record_created_by VARCHAR(50) DEFAULT SYSTEM_USER,
    source VARCHAR(225) DEFAULT 'dataset/employees&condidate/employee_performance.csv/bronze.customers'
);



-- ========================================
-- 4. TABLES RECRUITMENT / CANDIDATES
-- ========================================

DROP TABLE IF EXISTS silver.candidates;
CREATE TABLE silver.candidates (
    candidate_id INT PRIMARY KEY,
    full_name VARCHAR(100),
    email VARCHAR(100),
    phone VARCHAR(20),
    degree VARCHAR(50),
    years_experience INT,
    tech_stack TEXT,
    certifications TEXT,
    application_date DATE,

    -- META
    record_created_at DATETIME DEFAULT GETDATE(),
    record_created_by VARCHAR(50) DEFAULT SYSTEM_USER,
    source VARCHAR(225) DEFAULT 'dataset/employees&condidate/candidates.csv/bronze.customers'
);


DROP TABLE IF EXISTS silver.candidate_cv_raw;
CREATE TABLE silver.candidate_cv_raw (
    cv_id INT PRIMARY KEY,
    candidate_id INT,
    cv_text TEXT,
    parsed_keywords TEXT,
    language_detected VARCHAR(20),
    uploaded_date DATE,

    -- META
    record_created_at DATETIME DEFAULT GETDATE(),
    record_created_by VARCHAR(50) DEFAULT SYSTEM_USER,
    source VARCHAR(225) DEFAULT 'dataset/employees&condidate/candidate_cv_raw./bronze.customers'
);


DROP TABLE IF EXISTS silver.candidate_interviews;
CREATE TABLE silver.candidate_interviews (
    interview_id INT PRIMARY KEY,
    candidate_id INT,
    recruiter_id INT,
    interview_score INT,
    notes TEXT,
    stage VARCHAR(50),
    result VARCHAR(20),

    -- META
    record_created_at DATETIME DEFAULT GETDATE(),
    record_created_by VARCHAR(50) DEFAULT SYSTEM_USER,
    source VARCHAR(225) DEFAULT 'dataset/employees&condidate/candidate_interviews.csv/bronze.customers'
);
