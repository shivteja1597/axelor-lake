-- Salary band classification
with curated as (
    select * from {{ ref('dim_employee') }}
)

select
    employee_id,
    name,
    department,
    role,
    salary,
    case
        when salary < 40000 then 'Low'
        when salary between 40000 and 80000 then 'Medium'
        else 'High'
    end as salary_band
from curated
