DROP TABLE IF EXISTS raw_events;

CREATE TABLE raw_events (
    event_time TIMESTAMP,
    event_type TEXT,
    product_id INT,
    category_code TEXT,
    brand TEXT,
    price DOUBLE PRECISION,
    user_id BIGINT,
    user_session TEXT,
    category_level1 TEXT,
    category_level2 TEXT,
    event_weekday INT
);

CREATE INDEX idx_raw_event_time ON raw_events(event_time);
CREATE INDEX idx_raw_user_id ON raw_events(user_id);
CREATE INDEX idx_raw_event_type ON raw_events(event_type);
CREATE INDEX idx_raw_category ON raw_events(category_level1);

