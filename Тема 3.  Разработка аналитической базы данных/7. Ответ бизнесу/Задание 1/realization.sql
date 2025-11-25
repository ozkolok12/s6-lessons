WITH oldest_groups AS (          
    SELECT hk_group_id
    FROM   VT2511219940EA__DWH.h_groups
    ORDER  BY registration_dt
    LIMIT  10
),
     group_messages AS (             
         SELECT lgd.hk_message_id
         FROM   VT2511219940EA__DWH.l_groups_dialogs lgd
                    JOIN   oldest_groups g
                           ON g.hk_group_id = lgd.hk_group_id
     ),
     group_users AS (                 
         SELECT DISTINCT lum.hk_user_id
         FROM   VT2511219940EA__DWH.l_user_message lum
                    JOIN   group_messages gm
                           ON gm.hk_message_id = lum.hk_message_id
     )
SELECT  sus.age,
        COUNT(DISTINCT gu.hk_user_id) AS user_cnt
FROM    group_users      gu
            JOIN    VT2511219940EA__DWH.s_user_socdem sus
                    ON sus.hk_user_id = gu.hk_user_id
GROUP BY sus.age
ORDER BY sus.age;