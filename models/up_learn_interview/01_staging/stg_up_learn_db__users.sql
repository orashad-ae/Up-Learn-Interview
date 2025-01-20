WITH 

source AS (
SELECT
    user_id 
    ,school_id 
    ,name 
    ,postcode 
    ,introduction_source 
    ,school_year 
    ,signed_up_at 
    ,last_logged_in_at

FROM {{ source('up_learn_db', 'users') }}
)

,transformed AS (
SELECT 
    -- ids 
    user_id AS user_id 
    ,school_id AS school_id 

    -- strings 
    ,name AS full_name
    ,postcode AS postcode 
    ,introduction_source AS aquisition_channel 

    -- numerics 
    ,school_year AS school_year

    -- timestamps 
    ,signed_up_at AS signed_up_at_utc
    ,last_logged_in_at AS last_logged_in_at_utc
    ,{{ dbt_date.convert_timezone('signed_up_at', 'Europe/London', 'UTC') }} AS signed_up_at_ltz
    ,{{ dbt_date.convert_timezone('last_logged_in_at', 'Europe/London', 'UTC') }} AS last_logged_in_at_ltz

    -- dates

FROM source
)

SELECT * 

FROM transformed