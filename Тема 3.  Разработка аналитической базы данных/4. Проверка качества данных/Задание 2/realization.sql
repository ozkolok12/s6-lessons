SELECT
    COUNT(*) as total,
    COUNT(DISTINCT users.id) as unique_qty,
    'users' as dataset
FROM VT2511219940EA__STAGING.users

UNION ALL

SELECT
    COUNT(*) as total,
    COUNT(DISTINCT id) as unique_qty,
    'groups' as dataset
FROM VT2511219940EA__STAGING.groups

UNION ALL

SELECT
    COUNT(*) as total,
    COUNT(DISTINCT message_id) as unique_qty,
    'dialogs' as dataset
FROM VT2511219940EA__STAGING.dialogs
;