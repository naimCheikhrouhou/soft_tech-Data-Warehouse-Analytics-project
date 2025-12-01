print('gold.dim_date');
print('====================================================================');

drop table if exists gold.dim_date;
CREATE TABLE gold.dim_date (
    date_key     INT PRIMARY KEY,
    full_date    DATE,
    year         INT,
    month        INT,
    day          INT
);

go 
delete from gold.dim_date
go 
-- Générateur sur 10 ans
WITH cte AS (
    SELECT CAST('1996-01-01' AS DATE) AS d
    UNION ALL
    SELECT DATEADD(day, 1, d)
    FROM cte
    WHERE d < '2027-12-31'
)
INSERT INTO gold.dim_date(date_key, full_date, year, month, day)
SELECT 
    CONVERT(INT, FORMAT(d, 'yyyyMMdd')),
    d,
    YEAR(d),
    MONTH(d),
    DAY(d)
FROM cte
OPTION (MAXRECURSION 32767);

drop table if exists gold.dim_customers;
CREATE TABLE gold.dim_customers (
    customer_key INT IDENTITY PRIMARY KEY,
    customer_id INT,
    company_name VARCHAR(200),
    country VARCHAR(100),
    created_at DATE
);

print('gold.dim_product');
print('====================================================================');
drop table if exists gold.dim_product;
CREATE TABLE gold.dim_product (
    product_key INT IDENTITY PRIMARY KEY,
    product_id INT,
    product_name VARCHAR(200),
    category VARCHAR(100),
    base_price DECIMAL(10,2),
    development_hours INT,
    version VARCHAR(50)
);



print('gold.dim_project');
print('====================================================================');
drop table if exists gold.dim_project;
CREATE TABLE gold.dim_project (
    project_key INT IDENTITY PRIMARY KEY,
    project_id INT,
    project_name VARCHAR(200),
    status VARCHAR(50)
);


print('gold.dim_employee');
print('====================================================================');

drop table if exists gold.dim_employee;
CREATE TABLE gold.dim_employee (
    employee_key INT IDENTITY PRIMARY KEY,
    employee_id INT,
    full_name VARCHAR(200),
    role VARCHAR(100),
    hire_date DATE,
    salary DECIMAL(10,2),
    seniority VARCHAR(50),
    email VARCHAR(200),
    team_id INT
);


print('gold.dim_team');
print('====================================================================');

drop table if exists gold.dim_team;
CREATE TABLE gold.dim_team (
    team_key INT IDENTITY PRIMARY KEY,
    team_id INT,
    team_name VARCHAR(100),
    department VARCHAR(100),
    manager_team_id INT
);


print('gold.dim_candidates');
print('====================================================================');

drop table if exists gold.dim_candidates;
CREATE TABLE gold.dim_candidates (
    candidate_key INT IDENTITY PRIMARY KEY,
    candidate_id INT,
    full_name VARCHAR(200),
    email VARCHAR(200),
    phone VARCHAR(50),
    degree VARCHAR(100),
    years_experience INT,
    tech_stack VARCHAR(200),
    certifications VARCHAR(200),
    application_date DATE,
    cv_text TEXT,
    parsed_keywords VARCHAR(500),
    language_detected VARCHAR(50),
    uploaded_date_cv DATE,
    age int,
    university nvarchar(50)
);


