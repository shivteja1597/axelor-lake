-- Manager summary by department
{{ config(
    materialized='external',
    location='s3://lake-analytics/employee_manager_summary.parquet'
) }}

with curated as (
    select * from {{ ref('dim_employee') }}
)

select
    department,
    role,
    name as manager_name,
    count(*) over (partition by department) as dept_manager_count
from curated
where lower(role) like '%manager%'
