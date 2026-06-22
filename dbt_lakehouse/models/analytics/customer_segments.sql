with segment_file as (
    select *
    from read_parquet('{{ env_var("CUSTOMER_SEGMENTS_OBJECT", "s3://lake-analytics/customer_segments.parquet") }}')
)

select
    trim(cast(customer_segment_bucket as varchar)) as customer_segment_bucket,
    trim(cast(risk_segment as varchar)) as risk_segment,
    try_cast(customer_count as bigint) as customer_count
from segment_file
