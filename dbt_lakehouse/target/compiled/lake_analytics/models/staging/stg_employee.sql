-- Staging model: reads employee data from lake-staging
-- Uses read_parquet with glob pattern to avoid hardcoding UUID-based Iceberg paths
-- This reliably picks up all parquet data files regardless of table UUID


with raw_source as (
    select *
    from read_parquet('s3://lake-staging/my_data/data_*/data/*.parquet')
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