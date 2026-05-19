-- Manager summary by department


with curated as (
    select * from "memory"."main"."dim_employee"
)

select
    department,
    role,
    name as manager_name,
    count(*) over (partition by department) as dept_manager_count
from curated
where lower(role) like '%manager%'