-- Create schema for analytics / ML (safe)
CREATE SCHEMA IF NOT EXISTS analytics;

-- Drop table if it exists (for development)
DROP TABLE IF EXISTS analytics.customer_features;

-- Create feature table
CREATE TABLE analytics.customer_features (
    user_id BIGINT,
    user_session TEXT,

    -- Behavioral features
    total_events INT,
    view_count INT,
    cart_count INT,
    purchase_count INT,

    -- Monetary feature
    avg_price FLOAT,
    total_spent FLOAT,

    -- Temporal features
    active_days INT,
    most_active_weekday INT,

    -- Metadata
    feature_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
