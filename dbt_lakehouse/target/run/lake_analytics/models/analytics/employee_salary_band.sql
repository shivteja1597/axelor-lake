
      create or replace view "memory"."main"."employee_salary_band__dbt_int" as (
        select * from read_parquet('s3://lake-analytics/employee_salary_band.parquet', union_by_name=False)
        -- if relation is empty, filter by all columns having null values
        
      );
    