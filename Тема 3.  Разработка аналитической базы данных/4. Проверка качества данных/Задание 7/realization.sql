(SELECT count(1), 'missing group admin info' as info
    FROM VT2511219940EA__STAGING.groups g
    LEFT JOIN VT2511219940EA__STAGING.users AS u ON g.admin_id = u.id
    WHERE g.admin_id != u.id)

UNION ALL

(SELECT COUNT(1), 'missing sender info'
     FROM VT2511219940EA__STAGING.dialogs d
     LEFT JOIN VT2511219940EA__STAGING.users AS u ON d.message_from = u.id
     WHERE d.message_from != u.id)

UNION ALL

(SELECT COUNT(1), 'missing receiver info'
 FROM VT2511219940EA__STAGING.dialogs d
          LEFT JOIN VT2511219940EA__STAGING.users AS u ON d.message_to = u.id
 WHERE d.message_to != u.id)

UNION ALL

(SELECT COUNT(1), 'norm receiver info'
 FROM VT2511219940EA__STAGING.dialogs d
          LEFT JOIN VT2511219940EA__STAGING.users AS u ON d.message_to = u.id
 WHERE d.message_to = u.id)
;