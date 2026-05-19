
      create or replace view "memory"."main"."dim_employee__dbt_int" as (
        select * from read_parquet('s3://lake-curated/dim_employee.parquet', union_by_name=False)
        -- if relation is empty, filter by all columns having null values
        
      );
    