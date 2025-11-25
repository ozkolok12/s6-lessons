(SELECT
     max(registration_dt) < now() as 'no future dates',
     min(registration_dt) > '2020-09-03'as 'no false-start dates',
    'users' as dataset
 FROM VT2511219940EA__STAGING.users)
UNION ALL
(SELECT
     max(registration_dt) < now() as 'no future dates',
     min(registration_dt) > '2020-09-03'as 'no false-start dates',
    'groups' as dataset
 FROM VT2511219940EA__STAGING.groups g)
UNION ALL
(SELECT
     max(message_ts) < now() as 'no future dates',
     min(message_ts) > '2020-09-03' as 'no false-start dates',
     'dialogs' as dataset
 FROM VT2511219940EA__STAGING.dialogs d); 