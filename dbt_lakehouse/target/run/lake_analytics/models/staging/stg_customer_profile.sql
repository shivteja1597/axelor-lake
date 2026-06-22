
  
  create view "memory"."main"."stg_customer_profile__dbt_tmp" as (
    with raw_source as (
    select *
    from read_csv_auto('s3://lake-raw/customer_profile/*.csv', header=true, sample_size=-1)
)

select
    trim(cast(account_id as varchar)) as account_id,
    trim(cast(site_id as varchar)) as site_id,
    trim(cast(customer_first_name as varchar)) as customer_first_name,
    trim(cast(customer_last_name as varchar)) as customer_last_name,
    trim(cast(address_line1 as varchar)) as address_line1,
    trim(cast(address_line_2 as varchar)) as address_line_2,
    trim(cast(city as varchar)) as city,
    upper(trim(cast(state as varchar))) as state,
    trim(cast(country as varchar)) as country,
    trim(cast(contract_status as varchar)) as contract_status,
    trim(cast(plan_type as varchar)) as plan_type,
    trim(cast(billing_frequency as varchar)) as billing_frequency,
    trim(cast(payment_mode as varchar)) as payment_mode,
    trim(cast(property_type as varchar)) as property_type,
    trim(cast(acv_segment as varchar)) as acv_segment,
    try_cast(nullif(trim(cast(tenure_in_months as varchar)), '') as double) as tenure_in_months,
    try_cast(nullif(trim(cast(days_left_in_recent_contract as varchar)), '') as double) as days_left_in_recent_contract,
    try_cast(nullif(trim(cast(current_rmr as varchar)), '') as double) as current_rmr,
    try_cast(nullif(trim(cast(fico_score as varchar)), '') as double) as fico_score,
    try_cast(nullif(trim(cast(customer_initial_contract_term as varchar)), '') as double) as customer_initial_contract_term,
    try_cast(nullif(trim(cast(remaining_life_time as varchar)), '') as double) as remaining_life_time,
    try_cast(nullif(trim(cast(projected_rmr_churn as varchar)), '') as double) as projected_rmr_churn,
    try_cast(nullif(trim(cast(acv as varchar)), '') as double) as acv,
    try_cast(nullif(trim(cast(cost_to_serve as varchar)), '') as double) as cost_to_serve,
    try_cast(nullif(trim(cast(rmr_difference_from_company_avg as varchar)), '') as double) as rmr_difference_from_company_avg,
    try_cast(nullif(trim(cast(area_attrition as varchar)), '') as double) as area_attrition
from raw_source
  );
