-- Department x salary band cross-tabulation
{{ config(
    materialized='external',
    location='s3://lake-analytics/employee_department_salary_summary.parquet'
) }}

with bands as (
    select * from {{ ref('employee_salary_band') }}
)

select
    department,
    salary_band,
    count(*) as employee_count,
    sum(salary) as total_salary
from bands
group by department, salary_band
