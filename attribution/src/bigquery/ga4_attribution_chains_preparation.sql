------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- GOOGLE ANALYTICS CHAINS BASE PREPARATION

-- this script pre-filters sessions and events for the Google Analytics 4 (GA4) preprocessed digital events
--  - perimeter:
--    - filter all conversions occurred in the conversion period which by default consists in the last 30 days from the last month end date
--    - filter all GA4 events which occurred in the conversion and lookback period which by default is of 90 days
--      conversions that occurr in the lookback window before the start of the conversion period are considered as simple events
--  - data preparation:
--    - to each conversion in the conversion period attach all events that occurred in the lookback window from the conversion
--    - to each customer the latest visited store country is assigned 
--    - events are sorted by cookie id, session last event timestamp and event timestamp

------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

DECLARE conversion_period INT64 DEFAULT 30;
DECLARE lookback_window INT64 DEFAULT 90;
DECLARE last_day_of_previous_month DATE DEFAULT DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 1 DAY);

CREATE OR REPLACE TABLE `myproject.mydataset.attribution_chains_base` AS

-- select all conversion within conversion_period
WITH conversions AS (
    SELECT DISTINCT
        session_id,
        cookie_id,
        last_event_timestamp AS conversion_timestamp,
        total_transactions,
        total_item_quantity,
        session_revenue_usd
    FROM `myproject.mydataset.attribution_events`
    WHERE f_converted = 1
        AND CAST(first_event_timestamp AS DATE) BETWEEN DATE_SUB(last_day_of_previous_month, INTERVAL conversion_period DAY) AND last_day_of_previous_month
)

-- use conversions to select all events occurred within conversion_period + lookback_window
-- NOTE: if a conversion happened in the lookback window before the conversion period then I consider that event as only being a touchpoint and not a conversion
, events AS (
    SELECT DISTINCT
        session_id,
        cookie_id,
        country_web,
        is_engaged,
        event_timestamp,
        source,
        medium,
        campaign_name,
        source_medium,
        channel_group,
        CASE WHEN CAST(event_timestamp AS DATE) >= DATE_SUB(last_day_of_previous_month, INTERVAL conversion_period DAY) THEN f_converted
            ELSE 0
            END AS f_converted,
        CASE WHEN CAST(event_timestamp AS DATE) >= DATE_SUB(last_day_of_previous_month, INTERVAL conversion_period DAY) THEN total_transactions
            ELSE 0
            END AS total_transactions,
        CASE WHEN CAST(event_timestamp AS DATE) >= DATE_SUB(last_day_of_previous_month, INTERVAL conversion_period DAY) THEN total_item_quantity
            ELSE 0
            END AS total_item_quantity,
        CASE WHEN CAST(event_timestamp AS DATE) >= DATE_SUB(last_day_of_previous_month, INTERVAL conversion_period DAY) THEN session_revenue_usd
            ELSE 0
            END AS session_revenue_usd,
        first_event_timestamp,
        last_event_timestamp
    FROM `myproject.mydataset.attribution_events`
    WHERE CAST(first_event_timestamp AS DATE) BETWEEN DATE_SUB(last_day_of_previous_month, INTERVAL conversion_period + lookback_window DAY) AND last_day_of_previous_month
  )

-- now we have to identify country for each customer as the last visited store country
-- first identify for each client_id the last visited website country
, last_web_country AS (
    SELECT cookie_id, ARRAY_AGG(country_web IGNORE NULLS ORDER BY event_timestamp DESC)[SAFE_OFFSET(0)] AS country
    FROM events
    GROUP BY cookie_id
)

-- finally filter only events occurred in lookback window from a conversion
SELECT DISTINCT
    events.session_id,
    cookie_id,
    lwc.country,
    events.is_engaged,
    events.source,
    events.medium,
    events.campaign_name,
    events.source_medium,
    events.channel_group,
    events.event_timestamp,
    events.f_converted,
    events.total_transactions,
    events.total_item_quantity,
    events.session_revenue_usd,
    events.first_event_timestamp,
    events.last_event_timestamp,
FROM conversions
LEFT JOIN events USING (cookie_id)
LEFT JOIN last_web_country AS lwc USING (cookie_id)
WHERE events.first_event_timestamp <= conversions.conversion_timestamp
    AND events.first_event_timestamp > TIMESTAMP_SUB(conversions.conversion_timestamp, INTERVAL lookback_window DAY)
    AND cookie_id IS NOT NULL
    AND events.channel_group IS NOT NULL
ORDER BY cookie_id, last_event_timestamp, event_timestamp