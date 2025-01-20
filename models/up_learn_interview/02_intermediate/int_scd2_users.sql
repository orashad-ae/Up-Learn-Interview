WITH 

int_scd2_users AS (
SELECT
    user_id 
    ,school_id 
    ,full_name 
    ,postcode 
    ,aquisition_channel
    ,school_year 
    ,signed_up_at_ltz 

    -- snapshot columns
    ,dbt_valid_from
    ,dbt_valid_to
    ,dbt_updated_at


FROM {{ ref('snapshot_int_scd2_users') }}
)

,transformed AS (
SELECT
    user_id 
    ,school_id 
    ,full_name 
    ,postcode 
    ,aquisition_channel
    ,school_year 
    ,signed_up_at_ltz

    -- snapshot columns
    ,CASE
        WHEN dbt_valid_to IS NULL THEN 1 
        ELSE 0
    END AS is_current_record
        -- utc
    ,dbt_valid_from AS valid_from_dt_utc
    ,COALESCE(dbt_valid_to, TIMESTAMP '9999-12-31') AS valid_to_dt_utc
    ,dbt_updated_at AS dwh_updated_at_utc 
        -- ltz
    ,{{ dbt_date.convert_timezone('dbt_valid_from', 'Europe/London', 'UTC') }} AS valid_from_dt_ltz
    ,COALESCE({{ dbt_date.convert_timezone('dbt_valid_to', 'Europe/London', 'UTC') }}, TIMESTAMP '9999-12-31') AS valid_to_dt_ltz
    ,{{ dbt_date.convert_timezone('dbt_updated_at', 'Europe/London', 'UTC') }} AS dwh_updated_at_ltz

FROM int_scd2_users 
)

SELECT * 

FROM transformed