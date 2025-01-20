WITH 

-- source models 

int_scd2_schools AS (
SELECT
    school_id 
    ,name 
    ,postcode 
    ,enrolled_students_count
    ,is_current_record
    ,valid_from_dt_ltz 
    ,valid_to_dt_ltz
    ,dwh_updated_at_ltz
    

FROM {{ ref('int_scd2_schools') }}
)

-- main queries 

,transformed AS (
SELECT 
    {{ dbt_utils.generate_surrogate_key(['school_id', 'enrolled_students_count', 'valid_from_dt_ltz']) }} AS school_scd_key
    ,{{ dbt_utils.generate_surrogate_key(['school_id']) }} AS school_key 
    ,school_id 
    ,name 
    ,postcode 
    ,enrolled_students_count
    ,is_current_record
    ,CASE 
        WHEN ROW_NUMBER() OVER(PARTITION BY school_id ORDER BY valid_from_dt_ltz ASC) = 1 THEN TIMESTAMP '2000-01-01'
        ELSE valid_from_dt_ltz
    END AS valid_from_dt_ltz  
    ,valid_to_dt_ltz
    ,dwh_updated_at_ltz

FROM int_scd2_schools
)

SELECT * 

FROM transformed