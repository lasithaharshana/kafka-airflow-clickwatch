-- Initialize analytics database for storing processed data
-- This script runs automatically when PostgreSQL container starts

-- Create analytics database if not exists
SELECT 'CREATE DATABASE analytics'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'analytics')\gexec

-- Connect to analytics database and create tables
\c analytics;

-- Daily summary table for batch processing results
CREATE TABLE IF NOT EXISTS daily_summary (
    id SERIAL PRIMARY KEY,
    report_date DATE NOT NULL UNIQUE,
    total_events INTEGER NOT NULL,
    buyer_count INTEGER NOT NULL,
    window_shopper_count INTEGER NOT NULL,
    buyer_rate DECIMAL(5,2),
    generated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- User segments table (buyers vs window shoppers)
CREATE TABLE IF NOT EXISTS user_segments (
    id SERIAL PRIMARY KEY,
    report_date DATE NOT NULL,
    user_id VARCHAR(50) NOT NULL,
    segment VARCHAR(20) NOT NULL CHECK (segment IN ('buyer', 'window_shopper')),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(report_date, user_id)
);

-- Product performance table (top viewed products)
CREATE TABLE IF NOT EXISTS product_performance (
    id SERIAL PRIMARY KEY,
    report_date DATE NOT NULL,
    product_id VARCHAR(50) NOT NULL,
    view_count INTEGER NOT NULL,
    rank INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(report_date, product_id)
);

-- Category conversion rates table
CREATE TABLE IF NOT EXISTS category_conversion (
    id SERIAL PRIMARY KEY,
    report_date DATE NOT NULL,
    category VARCHAR(50) NOT NULL,
    views INTEGER NOT NULL,
    purchases INTEGER NOT NULL,
    conversion_rate DECIMAL(5,2) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(report_date, category)
);

-- Flash sale alerts table (from streaming layer)
CREATE TABLE IF NOT EXISTS flash_sale_alerts (
    id SERIAL PRIMARY KEY,
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    product_id VARCHAR(50) NOT NULL,
    views INTEGER NOT NULL,
    purchases INTEGER NOT NULL,
    detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_daily_summary_date ON daily_summary(report_date);
CREATE INDEX IF NOT EXISTS idx_user_segments_date ON user_segments(report_date);
CREATE INDEX IF NOT EXISTS idx_user_segments_segment ON user_segments(segment);
CREATE INDEX IF NOT EXISTS idx_product_performance_date ON product_performance(report_date);
CREATE INDEX IF NOT EXISTS idx_category_conversion_date ON category_conversion(report_date);
CREATE INDEX IF NOT EXISTS idx_flash_sale_alerts_time ON flash_sale_alerts(window_start, window_end);

-- Grant permissions to airflow user
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO airflow;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO airflow;

-- Log initialization complete
DO $$
BEGIN
    RAISE NOTICE 'Analytics database initialized successfully';
END $$;
