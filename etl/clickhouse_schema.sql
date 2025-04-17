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

-- Materialized view for daily metrics
CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.mv_daily_metrics
(
    event_date Date,
    campaign_id UInt32,
    advertiser_id UInt32,
    impressions UInt64,
    clicks UInt64,
    ctr Float64
)
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, campaign_id, advertiser_id)
POPULATE AS
SELECT
    imp.event_date,
    imp.campaign_id,
    c.advertiser_id,
    count(imp.impression_id) AS impressions,
    count(cl.click_id) AS clicks,
    (count(cl.click_id) / count(imp.impression_id)) AS ctr
FROM analytics.fact_impressions imp
LEFT JOIN analytics.fact_clicks cl ON imp.campaign_id = cl.campaign_id AND imp.event_date = cl.event_date
LEFT JOIN analytics.dim_campaign c ON imp.campaign_id = c.campaign_id
GROUP BY imp.event_date, imp.campaign_id, c.advertiser_id;