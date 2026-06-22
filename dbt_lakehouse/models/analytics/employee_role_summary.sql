-- Role-level aggregation
with curated as (
    select * from {{ ref('dim_employee') }}
)

select
    role,
    count(*) as employee_count,
    avg(salary) as average_salary
from curated
group by role
