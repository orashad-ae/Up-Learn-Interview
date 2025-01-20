WITH 

snapshot_int_scd2_user_logins AS (
SELECT
    user_id
    ,last_logged_in_at_utc 
    ,last_logged_in_at_ltz

FROM {{ ref('snapshot_int_scd2_user_logins') }}
)

,transformed AS (
SELECT
    user_id 
    ,last_logged_in_at_utc
    ,last_logged_in_at_ltz 

FROM snapshot_int_scd2_user_logins 
)

SELECT * 

FROM transformed