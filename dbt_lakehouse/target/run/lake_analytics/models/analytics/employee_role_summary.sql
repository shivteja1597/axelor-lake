create table "iceberg_lake"."lake_analytics"."employee_role_summary" as (
      -- Role-level aggregation
with curated as (
    select * from "iceberg_lake"."lake_curated"."dim_employee"
)

select
    role,
    count(*) as employee_count,
    avg(salary) as average_salary
from curated
group by role
    );