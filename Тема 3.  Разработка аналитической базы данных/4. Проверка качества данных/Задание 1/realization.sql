SELECT
    COUNT(*) as total_id_qty,
    COUNT(DISTINCT id) as unique_id_qty
FROM VT2511219940EA__STAGING.users;