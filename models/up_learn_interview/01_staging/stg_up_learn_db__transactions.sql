WITH 

source AS (
SELECT
    transaction_id
    ,licence_id 
    ,type 
    ,amount 
    ,transaction_occured_at

FROM {{ source('up_learn_db', 'transactions') }}
)

,transformed AS (
SELECT 
    -- ids 
    transaction_id AS transaction_id 
    ,licence_id AS licence_id 

    -- strings 
    ,type AS type 

    -- numerics 
    ,amount AS amount_lcr

    -- timestamps 
    ,transaction_occured_at AS occured_at_utc
    ,{{ dbt_date.convert_timezone('transaction_occured_at', 'Europe/London', 'UTC') }} AS occured_at_ltz

    -- dates

FROM source
)

SELECT * 

FROM transformed