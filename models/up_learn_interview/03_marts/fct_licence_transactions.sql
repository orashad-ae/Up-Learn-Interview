WITH

-- source models

transactions AS (
SELECT 
    transaction_id
    ,licence_id
    ,type
    ,amount_lcr
    ,occured_at_ltz

FROM {{ ref('stg_up_learn_db__transactions') }}
)

,licences AS (
SELECT
    licence_id 
    ,user_id 
    ,subject_id 
    ,type 
    ,tier 
    ,started_at_ltz
    ,expired_at_ltz

FROM {{ ref('stg_up_learn_db__licences') }}
)

,trials AS (
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
    ,user_key 
    ,user_scd_key 
    ,school_key
    ,valid_from_dt_ltz
    ,valid_to_dt_ltz

FROM {{ ref('dim_users') }}
)

-- main queries 

,transformed AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['transactions.transaction_id']) }} AS transaction_key 
        ,dim_users.user_scd_key
        ,{{ dbt_utils.generate_surrogate_key(['licences.user_id']) }} AS user_key
        ,{{ dbt_utils.generate_surrogate_key(['licences.subject_id']) }} AS subject_key
        ,{{ dbt_utils.generate_surrogate_key(['transactions.licence_id']) }} AS licence_key 
        ,dim_users.school_key
        ,{{ dbt_utils.generate_surrogate_key(['trials.trial_id']) }} AS trial_key
        ,transactions.type
        ,licences.type AS licence_type 
        ,licences.tier AS licence_tier 
        ,transactions.amount_lcr 
        ,CASE
            WHEN trials.trial_id IS NOT NULL THEN 1
            ELSE 0
        END AS has_prior_subject_trial
        ,transactions.occured_at_ltz
        ,licences.started_at_ltz AS licence_started_at_ltz
        ,licences.expired_at_ltz AS licence_expired_at_ltz

    FROM transactions

    LEFT JOIN licences 
        ON transactions.licence_id = licences.licence_id

    LEFT JOIN trials 
        ON licences.user_id = trials.user_id 
        AND licences.subject_id = trials.subject_id
        AND trials.started_at_ltz < licences.started_at_ltz
    
    LEFT JOIN dim_users 
        ON licences.user_id = dim_users.user_id 
        AND transactions.occured_at_ltz >= dim_users.valid_from_dt_ltz
        AND transactions.occured_at_ltz < dim_users.valid_to_dt_ltz
)

SELECT * 

FROM transformed

ORDER BY
    occured_at_ltz ASC