-- ========================================
-- 1. TABLES CLIENTS / SALES
-- ========================================

CREATE TABLE customers (
    customer_id INT PRIMARY KEY,
    company_name VARCHAR(100) NOT NULL,
    contact_name VARCHAR(100),
    email VARCHAR(100),
    phone VARCHAR(20),
    sector VARCHAR(50),
    country VARCHAR(50),
    created_at DATE
);

CREATE TABLE products (
    product_id INT PRIMARY KEY,
    product_name VARCHAR(100) NOT NULL,
    category VARCHAR(50),
    base_price DECIMAL(10,2),
    development_hours INT,
    version VARCHAR(20)
);

CREATE TABLE sales_orders (
    sale_id INT PRIMARY KEY,
    customer_id INT REFERENCES customers(customer_id),
    product_id INT REFERENCES products(product_id),
    quantity INT,
    total_price DECIMAL(10,2),
    payment_method VARCHAR(50),
    sale_date DATE
);

-- ========================================
-- 2. TABLES PROJETS / BUGS
-- ========================================

CREATE TABLE projects (
    project_id INT PRIMARY KEY,
    customer_id INT REFERENCES customers(customer_id),
    project_name VARCHAR(100),
    start_date DATE,
    end_date DATE,
    delivered_date DATE,
    project_manager INT,
    estimated_cost DECIMAL(10,2),
    actual_cost DECIMAL(10,2),
    status VARCHAR(20)
);

CREATE TABLE project_bug_reports (
    bug_id INT PRIMARY KEY,
    project_id INT REFERENCES projects(project_id),
    severity VARCHAR(20),
    description TEXT,
    reported_date DATE,
    resolved BOOLEAN
);

-- ========================================
-- 3. TABLES EMPLOYEES / TEAMS / PERFORMANCE
-- ========================================

CREATE TABLE teams (
    team_id INT PRIMARY KEY,
    team_name VARCHAR(50),
    department VARCHAR(50),
    manager_id INT
);

CREATE TABLE employees (
    employee_id INT PRIMARY KEY,
    full_name VARCHAR(100),
    role VARCHAR(50),
    team_id INT REFERENCES teams(team_id),
    hire_date DATE,
    salary DECIMAL(10,2),
    seniority VARCHAR(20),
    email VARCHAR(100)
);

CREATE TABLE employee_performance (
    perf_id INT PRIMARY KEY,
    employee_id INT REFERENCES employees(employee_id),
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

CREATE TABLE candidates (
    candidate_id INT PRIMARY KEY,
    full_name VARCHAR(100),
    email VARCHAR(100),
    phone VARCHAR(20),
    degree VARCHAR(50),
    years_experience INT,
    tech_stack TEXT,
    certifications TEXT,
    application_date DATE
);

CREATE TABLE candidate_cv_raw (
    cv_id INT PRIMARY KEY,
    candidate_id INT REFERENCES candidates(candidate_id),
    cv_text TEXT,
    parsed_keywords TEXT,
    language_detected VARCHAR(20),
    uploaded_date DATE
);

CREATE TABLE candidate_interviews (
    interview_id INT PRIMARY KEY,
    candidate_id INT REFERENCES candidates(candidate_id),
    recruiter_id INT REFERENCES employees(employee_id),
    interview_score INT,
    notes TEXT,
    stage VARCHAR(50),
    result VARCHAR(20)
);

-- ========================================
-- 5. TABLE FINANCE (OPTIONNEL)
-- ========================================

CREATE TABLE invoices (
    invoice_id INT PRIMARY KEY,
    customer_id INT REFERENCES customers(customer_id),
    amount_due DECIMAL(10,2),
    amount_paid DECIMAL(10,2),
    invoice_date DATE,
    due_date DATE,
    status VARCHAR(20)
);
