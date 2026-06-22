create table "iceberg_lake"."lake_curated"."customer_profile_features" as (
      with staging as (
    select * from "memory"."main"."stg_customer_profile"
),
normalized as (
    select
        account_id,
        site_id,
        customer_first_name,
        customer_last_name,
        concat_ws(' ', customer_first_name, customer_last_name) as customer_name,
        address_line1,
        address_line_2,
        city,
        state,
        country,
        contract_status,
        tenure_in_months,
        days_left_in_recent_contract,
        plan_type,
        current_rmr,
        billing_frequency,
        payment_mode,
        property_type,
        fico_score,
        customer_initial_contract_term,
        remaining_life_time,
        acv_segment,
        projected_rmr_churn,
        acv,
        cost_to_serve,
        rmr_difference_from_company_avg,
        area_attrition,
        case
            when country is null or trim(country) = '' then 'Unknown'
            when upper(trim(country)) in ('UNITED STATES', 'USA', 'US') then 'Domestic'
            else 'International'
        end as country_bucket,
        case
            when state in ('CT','ME','MA','NH','RI','VT','NJ','NY','PA') then 'Northeast'
            when state in ('IL','IN','MI','OH','WI','IA','KS','MN','MO','NE','ND','SD') then 'Midwest'
            when state in ('DE','FL','GA','MD','NC','SC','VA','DC','WV','AL','KY','MS','TN','AR','LA','OK','TX') then 'South'
            when state in ('AZ','CO','ID','MT','NV','NM','UT','WY','AK','CA','HI','OR','WA') then 'West'
            when state is null or trim(state) = '' then 'Unknown'
            else 'Other'
        end as state_bucket,
        case
            when address_line1 is not null and trim(address_line1) <> ''
             and address_line_2 is not null and trim(address_line_2) <> ''
             and city is not null and trim(city) <> ''
             and state is not null and trim(state) <> ''
             and country is not null and trim(country) <> '' then 'Full Profile'
            when address_line1 is not null and trim(address_line1) <> ''
             and city is not null and trim(city) <> ''
             and state is not null and trim(state) <> ''
             and country is not null and trim(country) <> '' then 'Basic Profile'
            else 'Limited Profile'
        end as profile_completeness_bucket
    from staging
)

select
    account_id,
    site_id,
    customer_first_name,
    customer_last_name,
    customer_name,
    address_line1,
    address_line_2,
    city,
    state,
    country,
    contract_status,
    tenure_in_months,
    days_left_in_recent_contract,
    plan_type,
    current_rmr,
    billing_frequency,
    payment_mode,
    property_type,
    fico_score,
    customer_initial_contract_term,
    remaining_life_time,
    acv_segment,
    projected_rmr_churn,
    acv,
    cost_to_serve,
    rmr_difference_from_company_avg,
    area_attrition,
    country_bucket,
    state_bucket,
    profile_completeness_bucket,
    case
        when country_bucket = 'International' then 'International Customers'
        when property_type = 'Commercial' then 'Business Customers'
        when current_rmr >= 60 or acv >= 1000 then 'High-Value Customers'
        when profile_completeness_bucket = 'Full Profile' then 'Complete Domestic Profiles'
        when profile_completeness_bucket = 'Basic Profile' then 'Incomplete Domestic Profiles'
        else 'Review Required'
    end as customer_segment_bucket
from normalized
where account_id is not null
    );
    BEGIN TRANSACTION;