COPY dwh.VT2511219940EA__STAGING.dialogs (
    message_id,
    message_ts,
    message_from,
    message_to,
    message,
    message_group
    )
FROM LOCAL '../../data/dialogs.csv'
DELIMITER ','
; 