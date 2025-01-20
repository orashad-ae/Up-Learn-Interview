WITH 

-- source models 

fct_licence_transactions AS (
SELECT
    transaction_key
    ,user_scd_key
    ,user_key
    ,subject_key
    ,licence_key
    ,school_key
    ,trial_key
    ,type 
    ,licence_type 
    ,licence_tier 
    ,amount_lcr 
    ,has_prior_subject_trial
    ,occured_at_ltz

FROM {{ ref('fct_licence_transactions') }}
)

,dim_users AS (
SELECT 
    user_scd_key
    ,full_name 
    ,aquisition_channel
    ,school_year

FROM {{ ref('dim_users') }}
)

,dim_schools AS (
SELECT 
    school_key 
    ,name 
    ,is_current_record

FROM {{ ref('dim_schools') }}
)

,dim_subjects AS (
SELECT
    subject_key
    ,name

FROM {{ ref('dim_subjects') }}
)

,fct_subject_trials AS (
SELECT
    trial_key 
    ,started_at_ltz
    ,expired_at_ltz

FROM {{ ref('fct_subject_trials') }}
)

-- main queries

,transformed AS (
SELECT 
    dim_users.full_name AS user_full_name 
    ,dim_users.aquisition_channel AS user_aquisition_channel
    ,dim_users.school_year AS user_school_year
    ,dim_schools.name AS user_school
    ,dim_subjects.name AS licence_subject
    ,fct_licence_transactions.licence_type 
    ,fct_licence_transactions.licence_tier
    ,fct_licence_transactions.type AS transaction_type 
    ,fct_licence_transactions.amount_lcr AS transaction_amount_lcr
    ,fct_licence_transactions.occured_at_ltz AS transaction_occured_at_ltz
    ,fct_licence_transactions.has_prior_subject_trial
    ,fct_subject_trials.started_at_ltz AS trial_started_at_ltz
    ,fct_subject_trials.expired_at_ltz AS trial_expired_at_ltz

    ,fct_licence_transactions.transaction_key
    ,fct_licence_transactions.user_scd_key
    ,fct_licence_transactions.user_key
    ,fct_licence_transactions.subject_key
    ,fct_licence_transactions.licence_key
    ,fct_licence_transactions.school_key
    ,fct_subject_trials.trial_key


FROM fct_licence_transactions

LEFT JOIN fct_subject_trials
    ON fct_licence_transactions.trial_key = fct_subject_trials.trial_key

LEFT JOIN dim_users
    ON fct_licence_transactions.user_scd_key = dim_users.user_scd_key 

LEFT JOIN dim_subjects
    ON fct_licence_transactions.subject_key = dim_subjects.subject_key

LEFT JOIN dim_schools
    ON fct_licence_transactions.school_key = dim_schools.school_key 
    AND dim_schools.is_current_record = 1
)

SELECT * 

FROM transformed