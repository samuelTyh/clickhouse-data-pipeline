-- Create database if it doesn't exist
CREATE DATABASE IF NOT EXISTS analytics;

-- Use the analytics database
USE analytics;

-- Dimension tables
CREATE TABLE IF NOT EXISTS analytics.dim_advertiser
(
    advertiser_id UInt32,
    name String,
    updated_at DateTime,
    created_at DateTime
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (advertiser_id);

CREATE TABLE IF NOT EXISTS analytics.dim_campaign
(
    campaign_id UInt32,
    name String,
    bid Decimal(10, 2),
    budget Decimal(10, 2),
    start_date Date,
    end_date Date,
    advertiser_id UInt32,
    updated_at DateTime,
    created_at DateTime
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (campaign_id);

-- Fact tables
CREATE TABLE IF NOT EXISTS analytics.fact_impressions
(
    impression_id UInt32,
    campaign_id UInt32,
    event_date Date,
    event_time DateTime,
    created_at DateTime
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(event_date)
ORDER BY (campaign_id, event_date);

CREATE TABLE IF NOT EXISTS analytics.fact_clicks
(
    click_id UInt32,
    campaign_id UInt32,
    event_date Date,
    event_time DateTime,
    created_at DateTime
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(event_date)
ORDER BY (campaign_id, event_date);
