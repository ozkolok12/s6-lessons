CREATE TABLE IF NOT EXISTS VT2511219940EA__STAGING.users (
    id INT NOT NULL,
    chat_name VARCHAR(200),
    registration_dt TIMESTAMP,
    country VARCHAR(200),
    age INT,
    PRIMARY KEY (id)
)
ORDER BY id;

CREATE TABLE IF NOT EXISTS VT2511219940EA__STAGING.dialogs (
    message_id INT NOT NULL,
    message_ts TIMESTAMP(9),
    message_from INT,
    message_to INT,
    message VARCHAR(1000),
    message_group INT,
    PRIMARY KEY (message_id),
    CONSTRAINT f_message_to FOREIGN KEY (message_to) 
        REFERENCES VT2511219940EA__STAGING.users(id),
    CONSTRAINT f_message_from FOREIGN KEY (message_from) 
        REFERENCES VT2511219940EA__STAGING.users(id)
)
ORDER BY message_id
PARTITION BY message_ts::date
GROUP BY calendar_hierarchy_day(message_ts::date, 3, 2);

CREATE TABLE IF NOT EXISTS VT2511219940EA__STAGING.groups (
    id INT NOT NULL,
    admin_id INT,
    group_name VARCHAR(100),
    registration_dt TIMESTAMP,
    is_private BOOLEAN,
    PRIMARY KEY (id),
    CONSTRAINT fk_groups_admin FOREIGN KEY (admin_id)
        REFERENCES VT2511219940EA__STAGING.users(id)
)
ORDER BY id, admin_id
PARTITION BY registration_dt::date
GROUP BY calendar_hierarchy_day(registration_dt::date, 3, 2);
