COPY dwh.VT2511219940EA__STAGING.groups (
    id,
    admin_id,
    group_name,
    registration_dt,
    is_private
    )
FROM LOCAL '../../data/groups.csv'
DELIMITER ','
; 