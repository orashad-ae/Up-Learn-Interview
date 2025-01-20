WITH 

-- source models 

int_scd2_users AS (
SELECT
    user_id 
    ,school_id
    ,full_name 
    ,postcode 
    ,aquisition_channel
    ,school_year
    ,signed_up_at_ltz
    ,is_current_record
    ,valid_from_dt_ltz 
    ,valid_to_dt_ltz
    ,dwh_updated_at_ltz
    

FROM {{ ref('int_scd2_users') }}
)

-- main queries 

,transformed AS (
SELECT 
    {{ dbt_utils.generate_surrogate_key(['user_id', 'school_id', 'school_year', 'valid_from_dt_ltz']) }} AS user_scd_key
    ,{{ dbt_utils.generate_surrogate_key(['user_id']) }} AS user_key 
    ,user_id 
    ,{{ dbt_utils.generate_surrogate_key(['school_id']) }} AS school_key
    ,school_id 
    ,full_name 
    ,postcode 
    ,aquisition_channel
    ,school_year 
    ,signed_up_at_ltz
    ,is_current_record
    ,CASE 
        WHEN ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY valid_from_dt_ltz ASC) = 1 THEN TIMESTAMP '2000-01-01'
        ELSE valid_from_dt_ltz
    END AS valid_from_dt_ltz 
    ,valid_to_dt_ltz
    ,dwh_updated_at_ltz

FROM int_scd2_users
)

SELECT * 

FROM transformed


