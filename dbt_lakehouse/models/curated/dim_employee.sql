-- Curated employee master with nulls filtered
with staging as (
    select * from {{ ref('stg_employee') }}
)

select
    employee_id,
    name,
    department,
    age,
    salary,
    status,
    role
from staging
where employee_id is not null
