--======= CTE user_group_messages ======

WITH oldest_groups AS (
    SELECT hk_group_id
    FROM VT2511219940EA__DWH.h_groups
    ORDER BY registration_dt
    LIMIT 10
),

user_group_messages AS (
    SELECT
        lgd.hk_group_id,
        COUNT(DISTINCT lum.hk_user_id) AS cnt_users_in_group_with_messages
    FROM VT2511219940EA__DWH.l_groups_dialogs  AS lgd
        JOIN VT2511219940EA__DWH.l_user_message AS lum
        ON lgd.hk_message_id = lum.hk_message_id
    WHERE  lgd.hk_group_id IN (SELECT hk_group_id FROM oldest_groups)
    GROUP  BY lgd.hk_group_id
)

SELECT
    hk_group_id,
    cnt_users_in_group_with_messages
FROM   user_group_messages
ORDER  BY cnt_users_in_group_with_messages DESC
LIMIT  10;

--====== CTE user_group_log ======

WITH oldest_groups AS (
    SELECT hk_group_id
    FROM VT2511219940EA__DWH.h_groups
    ORDER BY registration_dt
    LIMIT 10
),

user_group_log AS (
    SELECT
        luga.hk_group_id,
        COUNT(DISTINCT luga.hk_user_id) AS cnt_added_users
    FROM VT2511219940EA__DWH.l_user_group_activity luga
        JOIN VT2511219940EA__DWH.s_auth_history sah ON sah.hk_l_user_group_activity = luga.hk_l_user_group_activity
    WHERE sah.event  = 'add'
        AND luga.hk_group_id IN (
            SELECT hk_group_id
            FROM oldest_groups
            )
    GROUP BY luga.hk_group_id
)

SELECT
    hk_group_id,
    cnt_added_users
FROM user_group_log
ORDER BY cnt_added_users
LIMIT 10
;

--====== Group conversion ======
WITH oldest_groups AS (
    SELECT hk_group_id
    FROM VT2511219940EA__DWH.h_groups
    ORDER BY registration_dt
    LIMIT 10
),

user_group_messages AS (
SELECT
    lgd.hk_group_id,
    COUNT(DISTINCT lum.hk_user_id) AS cnt_users_in_group_with_messages
    FROM VT2511219940EA__DWH.l_groups_dialogs  AS lgd
        JOIN VT2511219940EA__DWH.l_user_message AS lum
            ON lgd.hk_message_id = lum.hk_message_id
    WHERE  lgd.hk_group_id IN (SELECT hk_group_id FROM oldest_groups)
    GROUP  BY lgd.hk_group_id
),

cnt_users_in_group_with_messages AS (
SELECT
    hk_group_id,
    cnt_users_in_group_with_messages
FROM   user_group_messages
ORDER  BY cnt_users_in_group_with_messages DESC
LIMIT  10
),


user_group_log AS (
    SELECT
        luga.hk_group_id,
        COUNT(DISTINCT luga.hk_user_id) AS cnt_added_users
    FROM VT2511219940EA__DWH.l_user_group_activity luga
        JOIN VT2511219940EA__DWH.s_auth_history sah ON sah.hk_l_user_group_activity = luga.hk_l_user_group_activity
    WHERE sah.event  = 'add'
        AND luga.hk_group_id IN (
                SELECT hk_group_id
                FROM oldest_groups
                )
    GROUP BY luga.hk_group_id
),

cnt_added_users AS (
    SELECT
        hk_group_id,
        cnt_added_users
    FROM user_group_log
    ORDER BY cnt_added_users
)

SELECT
    a.hk_group_id,
    a.cnt_added_users,
    b.cnt_users_in_group_with_messages,
    (b.cnt_users_in_group_with_messages / a.cnt_added_users)::NUMERIC(5,3) * 100 AS group_conversion
FROM cnt_added_users a
    LEFT JOIN cnt_users_in_group_with_messages b ON a.hk_group_id = b.hk_group_id
ORDER BY group_conversion DESC
;