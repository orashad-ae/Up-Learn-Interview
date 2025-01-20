WITH 

-- source models 

int_scd2_user_logins AS (
SELECT
    user_id 
    ,last_logged_in_at_ltz

FROM {{ ref('int_scd2_user_logins') }}
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
    {{ dbt_utils.generate_surrogate_key(['int_scd2_user_logins.user_id', 'int_scd2_user_logins.last_logged_in_at_ltz']) }} AS login_key
    ,dim_users.user_scd_key
    ,{{ dbt_utils.generate_surrogate_key(['int_scd2_user_logins.user_id']) }} AS user_key
    ,int_scd2_user_logins.last_logged_in_at_ltz


FROM int_scd2_user_logins

LEFT JOIN dim_users
    ON int_scd2_user_logins.user_id = dim_users.user_id 
    AND int_scd2_user_logins.last_logged_in_at_ltz >= dim_users.valid_from_dt_ltz
    AND int_scd2_user_logins.last_logged_in_at_ltz < dim_users.valid_to_dt_ltz
)

SELECT * 

FROM transformed