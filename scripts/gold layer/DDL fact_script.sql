
print('gold.fact_products_sales');
print('====================================================================');
drop table if exists gold.fact_products_sales;
CREATE TABLE gold.fact_products_sales (
    product_key INT references gold.dim_product(product_key) on delete cascade,
    customer_key INT references gold.dim_customers(customer_key)on delete cascade,
    sale_date_key INT references gold.dim_date(date_key)on delete cascade,
    quantity INT ,
    total_price DECIMAL(10,2)
);

print('gold.fact_project_suivis');
print('====================================================================');

DROP TABLE IF EXISTS gold.fact_project_suivis;
CREATE TABLE gold.fact_project_suivis (
    project_key INT REFERENCES gold.dim_project(project_key) ON DELETE CASCADE,
    customer_key INT REFERENCES gold.dim_customers(customer_key) ON DELETE CASCADE,
    manager_project_key INT REFERENCES gold.dim_employee(employee_key) ON DELETE CASCADE,

    start_date_key INT REFERENCES gold.dim_date(date_key) ON DELETE CASCADE,
    end_date_key INT REFERENCES gold.dim_date(date_key) ON DELETE NO ACTION,
    delivered_date_key INT REFERENCES gold.dim_date(date_key) ON DELETE NO ACTION,

    estimated_cost DECIMAL(10,2),
    actual_cost DECIMAL(10,2)
);



print('gold.fact_employee_performance');
print('====================================================================');

drop table if exists gold.fact_employee_performance;

CREATE TABLE gold.fact_employee_performance (
    employee_key INT references gold.dim_employee(employee_key) on delete cascade,
    project_key  INT references gold.dim_project(project_key) on delete cascade,
    start_date_key INT references gold.dim_date(date_key) on delete cascade,
    end_date_key INT references gold.dim_date(date_key) on delete NO ACTION,
    tasks_completed INT,
    overtime_hours DECIMAL(10,2),
    performance_score INT,
    project_success_rate DECIMAL(5,2)
);




print('gold.fact_interview');
print('====================================================================');

drop table if exists gold.fact_interview;
CREATE TABLE gold.fact_interview (
    candidate_key INT references gold.dim_candidates(candidate_key) on delete cascade,
    employee_key INT references gold.dim_employee(employee_key) on delete cascade,
    team_key INT references gold.dim_team(team_key) on delete cascade,
    interview_date_key INT references gold.dim_date(date_key) on delete cascade,
    interview_score INT,
    result VARCHAR(50)
);
