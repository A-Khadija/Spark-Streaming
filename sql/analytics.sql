-- CREATE SCHEMA / TABLES ONLY
CREATE SCHEMA IF NOT EXISTS analytics;

DROP TABLE IF EXISTS analytics.customer_features;

CREATE TABLE analytics.customer_features (
    user_id BIGINT,
    user_session TEXT,
    total_events INT,
    view_count INT,
    cart_count INT,
    purchase_count INT,
    avg_price FLOAT,
    total_spent FLOAT,
    active_days INT,
    most_active_weekday INT,
    feature_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create aggregation tables
CREATE TABLE analytics.sales_by_category (
    category TEXT,
    total_sales DOUBLE PRECISION
);

CREATE TABLE analytics.sales_per_minute (
    minute TIMESTAMP PRIMARY KEY,
    total_sales DOUBLE PRECISION
);
