
      create or replace view "memory"."main"."employee_manager_summary__dbt_int" as (
        select * from read_parquet('s3://lake-analytics/employee_manager_summary.parquet', union_by_name=False)
        -- if relation is empty, filter by all columns having null values
        
      );
    