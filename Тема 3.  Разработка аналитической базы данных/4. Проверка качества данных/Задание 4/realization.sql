(SELECT
     min(u.registration_dt) as datestamp,
     'earliest user registration' as info
 FROM VT2511219940EA__STAGING.users u)
UNION ALL
(SELECT
     max(u.registration_dt),
     'latest user registration'
 FROM VT2511219940EA__STAGING.users u)
UNION ALL
(SELECT
     min(registration_dt),
     'earliest group creation'
 FROM VT2511219940EA__STAGING.groups)
UNION ALL
(SELECT
     max(registration_dt),
     'latest group creation'
 FROM VT2511219940EA__STAGING.groups)
UNION ALL
(SELECT
     min(message_ts),
     'earliest dialog message'
 FROM VT2511219940EA__STAGING.dialogs)
UNION ALL
(SELECT
     max(message_ts),
     'latest dialog message'
 FROM VT2511219940EA__STAGING.dialogs);