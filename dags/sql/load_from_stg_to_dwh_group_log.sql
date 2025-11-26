INSERT INTO VT2511219940EA__DWH.l_user_group_activity(
        hk_l_user_group_activity,
        hk_user_id,
        hk_group_id,
        load_dt,
        load_src
)

SELECT
    DISTINCT hash(hu.user_id, hg.group_id) AS hk_l_user_group_activity,
    hu.hk_user_id,
    hg.hk_group_id,
    now(),
    'group_log'

FROM VT2511219940EA__STAGING.group_log as gl
    LEFT JOIN  VT2511219940EA__DWH.h_users hu ON gl.user_id = hu.user_id
    LEFT JOIN  VT2511219940EA__DWH.h_groups hg ON gl.group_id = hg.group_id
;