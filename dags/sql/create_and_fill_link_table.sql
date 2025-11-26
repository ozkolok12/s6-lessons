--============ CREATE LINK TABLE l_user_group_activity ============--

CREATE TABLE IF NOT EXISTS VT2511219940EA__DWH.l_user_group_activity (
        hk_l_user_group_activity INT NOT NULL,
        hk_user_id INT,
        hk_group_id INT,
        load_dt TIMESTAMP,
        load_src VARCHAR(20),

    PRIMARY KEY (hk_l_user_group_activity),

    CONSTRAINT f_user_id_hub FOREIGN KEY (hk_user_id)
        REFERENCES VT2511219940EA__DWH.h_users(hk_user_id),

    CONSTRAINT f_group_hub FOREIGN KEY (hk_group_id)
        REFERENCES VT2511219940EA__DWH.h_groups(hk_group_id)
)

ORDER BY hk_l_user_group_activity
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

--========== INSERT DATA INTO LINK TABLE ==========--

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

from VT2511219940EA__STAGING.group_log as gl
         left join VT2511219940EA__DWH.h_users hu ON gl.user_id = hu.user_id
    left join VT2511219940EA__DWH.h_groups hg ON gl.group_id = hg.group_id
;