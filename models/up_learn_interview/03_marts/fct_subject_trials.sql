WITH

-- source models

trials AS (
SELECT
    trial_id 
    ,user_id 
    ,subject_id 
    ,started_at_ltz
    ,expired_at_ltz

FROM {{ ref('stg_up_learn_db__trials') }}
)

,dim_users AS (
SELECT
    user_id 
    ,user_scd_key 
    ,valid_from_dt_ltz
    ,valid_to_dt_ltz

FROM {{ ref('dim_users') }}
)

-- main queries 

,transformed AS (
SELECT 
    {{ dbt_utils.generate_surrogate_key(['trials.trial_id']) }} AS trial_key 
    ,dim_users.user_scd_key
    ,{{ dbt_utils.generate_surrogate_key(['trials.user_id']) }} AS user_key
    ,{{ dbt_utils.generate_surrogate_key(['trials.subject_id']) }} AS subject_key
    ,trials.started_at_ltz
    ,trials.expired_at_ltz

FROM trials 

LEFT JOIN dim_users
    ON trials.user_id = dim_users.user_id 
    AND trials.started_at_ltz >= dim_users.valid_from_dt_ltz
    AND trials.started_at_ltz < dim_users.valid_to_dt_ltz
)

SELECT * 

FROM transformed
