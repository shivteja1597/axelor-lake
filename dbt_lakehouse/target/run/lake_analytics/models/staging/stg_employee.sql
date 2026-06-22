
  
  create view "memory"."main"."stg_employee__dbt_tmp" as (
    -- Staging model: reads employee data from lake-raw CSV uploads
with raw_source as (
    select *
    from read_csv_auto('s3://lake-raw/*/*.csv', header=true, sample_size=-1)
)

select
    trim(employeeId) as employee_id,
    trim(name) as name,
    trim(department) as department,
    cast(age as integer) as age,
    cast(salary as double) as salary,
    trim(status) as status,
    trim(role) as role
from raw_source
  );
