WITH 

source AS (
SELECT
    school_id 
    ,school_name 
    ,school_postcode 
    ,number_of_pupils

FROM {{ source('up_learn_db', 'schools') }}
)

,transformed AS (
SELECT 
    -- ids 
    school_id AS school_id

    -- strings 
    ,school_name AS name
    ,school_postcode AS postcode

    -- numerics 
    ,number_of_pupils AS enrolled_students_count

    -- timestamps 

    -- dates

FROM source
)

SELECT * 

FROM transformed