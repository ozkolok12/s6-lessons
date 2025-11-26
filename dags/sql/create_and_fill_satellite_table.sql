--==========   CREATE AND INGEST SATELLITE  TABLE  ============--

CREATE TABLE IF NOT EXISTS VT2511219940EA__DWH.s_auth_history (
    hk_l_user_group_activity INT NOT NULL,
    user_id_from INT,
    event VARCHAR(6),
    event_dt TIMESTAMP,
    load_dt TIMESTAMP,
    load_src VARCHAR(20),

    CONSTRAINT f_l_user_group_activity_hub FOREIGN KEY (hk_l_user_group_activity)
        REFERENCES VT2511219940EA__DWH.l_user_group_activity(hk_l_user_group_activity)
)

ORDER BY hk_l_user_group_activity
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

--=========   INGEST SATELLITE  TABLE  ==========--

INSERT INTO VT2511219940EA__DWH.s_auth_history (
    hk_l_user_group_activity,
    user_id_from,
    event,
    event_dt,
    load_dt,
    load_src
)

SELECT
    luga.hk_l_user_group_activity,
    gl.user_id_from,
    gl.event,
    gl.datetime AS event_dt,
    now(),
    'group_log'

FROM VT2511219940EA__STAGING.group_log gl
         left join VT2511219940EA__DWH.h_groups as hg on gl.group_id = hg.group_id
         left join VT2511219940EA__DWH.h_users as hu on gl.user_id = hu.user_id
         left join VT2511219940EA__DWH.l_user_group_activity as luga on hg.hk_group_id = luga.hk_group_id and hu.hk_user_id = luga.hk_user_id
;