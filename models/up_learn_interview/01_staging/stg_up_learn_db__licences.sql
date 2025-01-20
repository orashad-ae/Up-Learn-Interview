WITH 

source AS (
SELECT
    licence_id 
    ,user_id
    ,subject_id 
    ,licence_type 
    ,licence_tier 
    ,licence_started_at 
    ,licence_expires_at

FROM {{ source('up_learn_db', 'licences') }}
)

,transformed AS (
SELECT 
    -- ids 
    licence_id AS licence_id
    ,user_id AS user_id
    ,subject_id AS subject_id

    -- strings 
    ,licence_type AS type
    ,licence_tier AS tier

    -- numerics 

    -- timestamps 
    ,licence_started_at AS started_at_utc
    ,licence_expires_at AS expired_at_utc
    ,{{ dbt_date.convert_timezone('licence_started_at', 'Europe/London', 'UTC') }} AS started_at_ltz
    ,{{ dbt_date.convert_timezone('licence_expires_at', 'Europe/London', 'UTC') }} AS expired_at_ltz

    -- dates

FROM source
)

SELECT * 

FROM transformed