USE analytics;

-- Materialized view for campaign CTR
CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.mv_campaign_ctr
(
    campaign_id UInt32,
    campaign_name String,
    advertiser_name String,
    impressions UInt64,
    clicks UInt64,
    ctr Float64
)
ENGINE = SummingMergeTree()
ORDER BY (campaign_id)
POPULATE AS
SELECT
    c.campaign_id,
    c.name AS campaign_name,
    a.name AS advertiser_name,
    COUNT(DISTINCT i.impression_id) AS impressions,
    COUNT(DISTINCT cl.click_id) AS clicks,
    COUNT(DISTINCT cl.click_id) / COUNT(DISTINCT i.impression_id) AS ctr
FROM dim_campaign c
JOIN dim_advertiser a ON c.advertiser_id = a.advertiser_id
LEFT JOIN fact_impressions i ON c.campaign_id = i.campaign_id
LEFT JOIN fact_clicks cl ON c.campaign_id = cl.campaign_id
GROUP BY c.campaign_id, c.name, a.name;

-- Materialized view for daily metrics
CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.mv_daily_performance
(
    event_date Date,
    impressions UInt64,
    clicks UInt64,
    daily_ctr Float64
)
ENGINE = SummingMergeTree()
ORDER BY (event_date)
POPULATE AS
SELECT
    i.event_date,
    COUNT(DISTINCT i.impression_id) AS impressions,
    COUNT(DISTINCT cl.click_id) AS clicks,
    COUNT(DISTINCT cl.click_id) / COUNT(DISTINCT i.impression_id) AS daily_ctr
FROM fact_impressions i
LEFT JOIN fact_clicks cl ON i.event_date = cl.event_date
GROUP BY i.event_date;

-- Materialized view for campaign performance by date
CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.mv_campaign_daily_performance
(
    event_date Date,
    campaign_id UInt32,
    campaign_name String,
    impressions UInt64,
    clicks UInt64,
    daily_ctr Float64
)
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, campaign_id)
POPULATE AS
SELECT
    i.event_date,
    c.campaign_id,
    c.name AS campaign_name,
    COUNT(DISTINCT i.impression_id) AS impressions,
    COUNT(DISTINCT cl.click_id) AS clicks,
    COUNT(DISTINCT cl.click_id) / COUNT(DISTINCT i.impression_id) AS daily_ctr
FROM fact_impressions i
JOIN dim_campaign c ON i.campaign_id = c.campaign_id
LEFT JOIN fact_clicks cl ON 
    i.campaign_id = cl.campaign_id AND
    i.event_date = cl.event_date
GROUP BY i.event_date, c.campaign_id, c.name;

-- Materialized view for campaign cost efficiency
CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.mv_campaign_efficiency
(
    campaign_id UInt32,
    campaign_name String,
    bid_amount Decimal(10, 2),
    impressions UInt64,
    clicks UInt64,
    cost_per_click Decimal(10, 2)
)
ENGINE = SummingMergeTree()
ORDER BY (campaign_id)
POPULATE AS
SELECT
    c.campaign_id,
    c.name AS campaign_name,
    c.bid AS bid_amount,
    COUNT(DISTINCT i.impression_id) AS impressions,
    COUNT(DISTINCT cl.click_id) AS clicks,
    c.bid * COUNT(DISTINCT i.impression_id) / NULLIF(COUNT(DISTINCT cl.click_id), 0) AS cost_per_click
FROM dim_campaign c
LEFT JOIN fact_impressions i ON c.campaign_id = i.campaign_id
LEFT JOIN fact_clicks cl ON c.campaign_id = cl.campaign_id
GROUP BY c.campaign_id, c.name, c.bid;

-- Materialized view for advertiser performance
CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.mv_advertiser_performance
(
    advertiser_id UInt32,
    advertiser_name String,
    campaign_count UInt64,
    total_budget Decimal(10, 2),
    impressions UInt64,
    clicks UInt64,
    overall_ctr Float64
)
ENGINE = SummingMergeTree()
ORDER BY (advertiser_id)
POPULATE AS
SELECT
    a.advertiser_id,
    a.name AS advertiser_name,
    COUNT(DISTINCT c.campaign_id) AS campaign_count,
    SUM(c.budget) AS total_budget,
    COUNT(DISTINCT i.impression_id) AS impressions,
    COUNT(DISTINCT cl.click_id) AS clicks,
    COUNT(DISTINCT cl.click_id) / COUNT(DISTINCT i.impression_id) AS overall_ctr
FROM dim_advertiser a
LEFT JOIN dim_campaign c ON a.advertiser_id = c.advertiser_id
LEFT JOIN fact_impressions i ON c.campaign_id = i.campaign_id
LEFT JOIN fact_clicks cl ON c.campaign_id = cl.campaign_id
GROUP BY a.advertiser_id, a.name;
