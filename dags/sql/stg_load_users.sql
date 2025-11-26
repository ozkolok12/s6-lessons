COPY dwh.VT2511219940EA__STAGING.users (id, chat_name, registration_dt, country, age)
FROM LOCAL '../../data/users.csv'
DELIMITER ','
; 