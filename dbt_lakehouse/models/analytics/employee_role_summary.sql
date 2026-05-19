-- Role-level aggregation
{{ config(
    materialized='external',
    location='s3://lake-analytics/employee_role_summary.parquet'
) }}

with curated as (
    select * from {{ ref('dim_employee') }}
)

select
    role,
    count(*) as employee_count,
    avg(salary) as average_salary
from curated
group by role
