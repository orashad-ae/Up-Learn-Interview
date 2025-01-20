WITH 

-- source models 

subjects AS (
SELECT
    subject_id 
    ,subject

FROM {{ ref('stg_up_learn_db__subjects') }}
)

-- main queries
,transformed AS (
SELECT 
    {{ dbt_utils.generate_surrogate_key(['subject_id']) }} AS subject_key 
    ,subject_id
    ,subject AS name

FROM subjects
)

SELECT * 

FROM transformed


