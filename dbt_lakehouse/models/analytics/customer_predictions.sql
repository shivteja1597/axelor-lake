with prediction_file as (
    select *
    from read_parquet('{{ env_var("CUSTOMER_PREDICTIONS_OBJECT", "s3://lake-analytics/customer_predictions.parquet") }}')
)

select
    trim(cast(account_id as varchar)) as account_id,
    trim(cast(site_id as varchar)) as site_id,
    trim(cast(customer_name as varchar)) as customer_name,
    upper(trim(cast(state as varchar))) as state,
    trim(cast(contract_status as varchar)) as contract_status,
    trim(cast(plan_type as varchar)) as plan_type,
    try_cast(current_rmr as double) as current_rmr,
    trim(cast(customer_segment_bucket as varchar)) as customer_segment_bucket,
    try_cast(churn_risk_percentage as double) as churn_risk_percentage,
    trim(cast(risk_segment as varchar)) as risk_segment,
    try_cast(base_risk as double) as base_risk,
    trim(cast(positive_risk_drivers as varchar)) as positive_risk_drivers,
    trim(cast(negative_risk_drivers as varchar)) as negative_risk_drivers
from prediction_file
where account_id is not null
