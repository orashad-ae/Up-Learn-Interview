WITH 

source AS (
SELECT
    trial_id
    ,user_id 
    ,subject_id 
    ,trial_started_at 
    ,trial_expired_at

FROM {{ source('up_learn_db', 'trials') }}
)

,transformed AS (
SELECT 
    -- ids 
    trial_id AS trial_id
    ,user_id AS user_id 
    ,subject_id AS subject_id

    -- strings 

    -- numerics 

    -- timestamps 
    ,trial_started_at AS started_at_utc
    ,trial_expired_at AS expired_at_utc 
    ,{{ dbt_date.convert_timezone('trial_started_at', 'Europe/London', 'UTC') }} AS started_at_ltz
    ,{{ dbt_date.convert_timezone('trial_expired_at', 'Europe/London', 'UTC') }} AS expired_at_ltz

    -- dates

FROM source
)

SELECT * 

FROM transformed