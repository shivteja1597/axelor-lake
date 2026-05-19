-- Role-level aggregation


with curated as (
    select * from "memory"."main"."dim_employee"
)

select
    role,
    count(*) as employee_count,
    avg(salary) as average_salary
from curated
group by role