
      create or replace view "memory"."main"."customer_profile_features__dbt_int" as (
        select * from read_parquet('s3://lake-curated/customer_profile_features.parquet', union_by_name=False)
        -- if relation is empty, filter by all columns having null values
        
      );
    