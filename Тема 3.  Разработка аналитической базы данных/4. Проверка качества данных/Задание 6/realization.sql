SELECT
    COUNT (g.admin_id) as admin_qty
    FROM VT2511219940EA__STAGING.groups AS g
    LEFT JOIN VT2511219940EA__STAGING.users AS u ON g.admin_id = u.id
WHERE g.admin_id != u.id;