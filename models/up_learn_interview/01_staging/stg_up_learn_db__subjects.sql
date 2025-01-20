WITH 

source AS (
SELECT
    subject_id
    ,subject_name

FROM {{ source('up_learn_db', 'subjects') }}
)

,transformed AS (
SELECT 
    -- ids 
    subject_id AS subject_id

    -- strings 
    ,subject_name AS subject

    -- numerics 

    -- timestamps 

    -- dates

FROM source
)

SELECT * 

FROM transformed