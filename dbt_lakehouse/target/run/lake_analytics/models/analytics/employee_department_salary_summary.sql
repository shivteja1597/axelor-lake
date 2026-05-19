
      create or replace view "memory"."main"."employee_department_salary_summary__dbt_int" as (
        select * from read_parquet('s3://lake-analytics/employee_department_salary_summary.parquet', union_by_name=False)
        -- if relation is empty, filter by all columns having null values
        
      );
    