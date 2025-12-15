DROP TABLE IF EXISTS raw_events;

CREATE TABLE raw_events (
    event_time TIMESTAMP,
    processing_time TIMESTAMP,
    event_type TEXT,
    product_id INT,
    category_id BIGINT,
    category_code TEXT,
    brand TEXT,
    price FLOAT,
    user_id BIGINT,
    user_session TEXT,
    category_level1 TEXT,
    category_level2 TEXT,
    event_weekday INT
);
