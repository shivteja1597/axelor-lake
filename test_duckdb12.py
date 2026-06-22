import duckdb

con = duckdb.connect()
con.execute("SET s3_endpoint='minio:9000';")
con.execute("SET s3_access_key_id='admin';")
con.execute("SET s3_secret_access_key='password123';")
con.execute("SET s3_region='us-east-1';")
con.execute("SET s3_use_ssl=false;")
con.execute("SET s3_url_style='path';")
try:
    con.execute("ATTACH 'warehouse' AS iceberg_lake (TYPE iceberg, ENDPOINT 'http://nessie:19120/iceberg', AUTHORIZATION_TYPE 'none');")
    con.execute("CREATE SCHEMA IF NOT EXISTS iceberg_lake.lake_curated;")
    con.execute("CREATE VIEW memory.main.stg_customer_profile AS SELECT 1 AS account_id, 'b' AS site_id, 'c' AS customer_first_name, 'd' AS customer_last_name, 'e' AS address_line1, 'f' AS address_line_2, 'g' AS city, 'h' AS state, 'i' AS country, 'j' AS contract_status, 1 AS tenure_in_months, 1 AS days_left_in_recent_contract, 'k' AS plan_type, 1 AS current_rmr, 'l' AS billing_frequency, 'm' AS payment_mode, 'n' AS property_type, 1 AS fico_score, 1 AS customer_initial_contract_term, 1 AS remaining_life_time, 'o' AS acv_segment, 1 AS projected_rmr_churn, 1 AS acv, 1 AS cost_to_serve, 1 AS rmr_difference_from_company_avg, 1 AS area_attrition;")
    
    sql = """
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
"""
    con.execute(sql)
    print("TABLE CREATED")
except Exception as e:
    print("ERROR:", e)
