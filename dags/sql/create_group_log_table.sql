CREATE TABLE IF NOT EXISTS VT2511219940EA__STAGING.group_log (
    group_id INT ,
    user_id INT ,
    user_id_from INT,
    event VARCHAR(6),
    datetime TIMESTAMP,
    PRIMARY KEY (group_id)
);