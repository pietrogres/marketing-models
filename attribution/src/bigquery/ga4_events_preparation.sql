------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- GOOGLE ANALYTICS TOUCHPOINT IDENTIFICATION

-- this scripts identifies origin touchpoints from Google Analytics 4 (GA$) data
--  - notation:
--    - traffic_source.source/traffic_source.medium: they refer to the session's first touchpoint, the identified source/medium of the first visit in the given session and will be referred to as first_source/first_medium
--    - collected_traffic_source.manual_source/collected_traffic_source.manual_medium: they refer to a specific event's touchpoint and will be referred to as source/medium
--  - perimeter:
--    - all events with no consent mode or measurement protocol are dropped
--    - all Instant BigQuery ML events are removed from scope
--  - notes:
--    - first_source/first_medium is unique by session when excluding ('(direct)', '(none)') pairs
--  - data preparation:
--    - events with null gclid and source equal to google will have source/medium imputed as google/cpc
--    - for every session it's identified the first (first_source, first_medium) pair in time arrangement
--       this is to make sure that cases of multiple (first_source, first_medium) pairs occurring because of a ('(direct)', '(none)') pair are treated correctly
--       if first (first_source, first_medium) is null then it is imputed as ('(direct)', '(none)')
--       in this way every session has a valid (first_source, first_medium) pair
--    - for every session with no valid (source, value) pairs, the (first_source, first_medium) pair is used instead as source channel
--       in this way every session has at least a source/medium pair

------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

DECLARE start_date STRING DEFAULT '20230101';

CREATE OR REPLACE TABLE `myproject.mydataset.attribution_events` AS

-- first, read GA4 automatic touchpoint mapping rules
-- for each category create a list with all corresponding sources
WITH mapping_automatic AS (
  SELECT category, ARRAY_AGG(sources) AS sources_list
  FROM `myproject.mydataset.source_categories`
  GROUP BY category
)

-- now read all GA4 events in the analysis perimeter
, events_base AS (
    SELECT
        CONCAT(user_pseudo_id, '_', (SELECT ep.value.int_value FROM UNNEST(event_params) AS ep WHERE ep.key = 'ga_session_id')) AS session_id,
        user_pseudo_id AS cookie_id,
        event_name,
        (SELECT COALESCE(SAFE_CAST(ep.value.string_value AS INT64), ep.value.int_value, 0) FROM UNNEST(event_params) AS ep WHERE key = 'session_engaged') AS is_engaged,
        (SELECT CASE WHEN ep.value.string_value IN ('Europe', 'international') THEN NULL ELSE ep.value.string_value END FROM UNNEST(event_params) AS ep WHERE ep.key = 'country_long') AS country_web,  -- remove generic countries
        CASE
            WHEN collected_traffic_source.gclid IS NOT NULL AND LOWER(collected_traffic_source.manual_source) = 'google' THEN 'google'
            ELSE LOWER(collected_traffic_source.manual_source)
            END AS source,
        CASE
            WHEN collected_traffic_source.gclid IS NOT NULL AND LOWER(collected_traffic_source.manual_source) = 'google' THEN 'cpc'
            ELSE LOWER(collected_traffic_source.manual_medium)
            END AS medium,
         LOWER(collected_traffic_source.manual_medium) AS campaign_name,
        IFNULL(traffic_source.source, '(direct)') AS first_source,
        IFNULL(traffic_source.medium, '(none)') AS first_medium,
        IFNULL(traffic_source.name, '(none)') AS first_campaign_name,
        ecommerce,
        event_date,
        event_timestamp
    FROM `myproject.mydataset.events_*`
    WHERE _TABLE_SUFFIX >= start_date
        AND (privacy_info.analytics_storage IS NULL OR privacy_info.analytics_storage = 'Yes')  -- remove hit with no consent mode
        AND event_name != 'privacy_info.analytics_storage'  -- remove hit measurement protocol 
        AND event_name NOT LIKE ('%_iBQML')  -- remove instant bigquery ml events
)

-- then identify session level stats
, sessions_base AS (
    SELECT
        session_id,
        ARRAY_AGG(cookie_id ORDER BY event_timestamp DESC)[SAFE_OFFSET(0)] AS cookie_id,
        MAX(is_engaged) AS is_engaged,
        ARRAY_AGG(country_web IGNORE NULLS ORDER BY event_timestamp DESC)[SAFE_OFFSET(0)] AS country_web,  -- select last visited store country
        CASE WHEN SUM(IF(event_name = 'purchase', 1, 0)) > 0 THEN 1 ELSE 0 END AS f_converted,
        SUM(IF(event_name = 'purchase', 1, 0)) AS total_transactions,
        SUM(IF(event_name = 'purchase', ecommerce.total_item_quantity, 0)) AS total_item_quantity,
        SAFE_CAST(SUM(IF(event_name = 'purchase', ecommerce.purchase_revenue_in_usd, 0.0)) AS NUMERIC) AS session_revenue_usd,
        MIN(TIMESTAMP_MICROS(event_timestamp)) AS first_event_timestamp,
        MAX(TIMESTAMP_MICROS(event_timestamp)) AS last_event_timestamp
    FROM events_base
    GROUP BY session_id
)

-- now to correctly identify sessions' events' origin, start from all sessions having at least a non null (source, medium) pair
-- for such sessions, keep the first timestamp related to every valid non null (source, medium) pair and fill source and medium if missing with '(none)'
, sessions_tp AS (
  SELECT
    session_id,
    COALESCE(source, '(none)') AS source,
    COALESCE(medium, '(none)') AS medium,
    COALESCE(campaign_name, '(none)') AS campaign_name,
    MIN(event_timestamp) AS event_timestamp,
    1 AS f_tp
  FROM events_base
  WHERE source IS NOT NULL OR medium IS NOT NULL
  GROUP BY session_id, source, medium, campaign_name
)

-- on the other side, for all events with non populated origin, only keep session_id with the timestamp of the first event and take the (first_source, first_medium) pair as origin
, sessions_no_tp AS (
  SELECT
    session_id,
    ARRAY_AGG(first_source ORDER BY event_timestamp)[OFFSET(0)] AS source,
    ARRAY_AGG(first_medium ORDER BY event_timestamp)[OFFSET(0)] AS medium,
    ARRAY_AGG(first_campaign_name ORDER BY event_timestamp)[OFFSET(0)] AS campaign_name,
    MIN(event_timestamp) AS event_timestamp,
    0 AS f_tp
  FROM events_base
  WHERE source IS NULL AND medium IS NULL
  GROUP BY session_id
)

-- and combine events in a single table
-- here keep all data that is present in events_tp table that reports all identified sessions' origins and report (first_source, first_medium) pair only for sessions without any valid (source, medium) pairs
-- in this way, at least an origin point is kept for all sessions  
, sessions_origins AS (
  SELECT
    session_id,
    COALESCE(tp.f_tp, no_tp.f_tp) AS f_tp,
    COALESCE(tp.source, no_tp.source) AS source,
    COALESCE(tp.medium, no_tp.medium) AS medium,
    COALESCE(tp.campaign_name, no_tp.campaign_name) AS campaign_name,
    CONCAT(COALESCE(tp.source, no_tp.source), '/', COALESCE(tp.medium, no_tp.medium)) AS source_medium,
    TIMESTAMP_MICROS(COALESCE(tp.event_timestamp, no_tp.event_timestamp)) AS event_timestamp
  FROM sessions_tp AS tp
  FULL JOIN sessions_no_tp AS no_tp USING (session_id)
  WHERE session_id IS NOT NULL
)

-- here map (source, medium) pairs in corresponding channels
, sessions_channels_base AS (
    SELECT
        *,
        CASE WHEN source = '(direct)'
            AND medium IN ('(not set)', '(none)')
        THEN 'Direct'
        
        WHEN REGEXP_CONTAINS(campaign_name, 'cross-network')
            AND medium != 'referral'
        THEN 'Cross Network'

        WHEN REGEXP_CONTAINS(medium, r'^(.*cp.*|ppc|retargeting|paid.*)$')
            AND (
                REGEXP_CONTAINS(campaign_name, r'^(.*(([^a-df-z]|^)shop|shopping).*)$')
                OR (
                    SELECT category
                    FROM mapping_automatic
                    WHERE category = 'SOURCE_CATEGORY_SHOPPING' 
                        AND source IN UNNEST(mapping_automatic.sources_list)
                ) IS NOT NULL
            )
        THEN 'Paid Shopping'

        WHEN REGEXP_CONTAINS(medium, r'^(.*cp.*|ppc|retargeting|paid.*)$')
            AND (
                SELECT category
                FROM mapping_automatic
                WHERE category = 'SOURCE_CATEGORY_SEARCH' 
                AND source IN UNNEST(mapping_automatic.sources_list)
            ) IS NOT NULL
        THEN 'Paid Search'

        WHEN REGEXP_CONTAINS(medium, r'^(.*cp.*|ppc|retargeting|paid.*)$')
            AND (
                SELECT category
                FROM mapping_automatic
                WHERE category = 'SOURCE_CATEGORY_SOCIAL'
                    AND source IN UNNEST(mapping_automatic.sources_list)
            ) IS NOT NULL
        THEN 'Paid Social'

        WHEN REGEXP_CONTAINS(medium, r'^(.*cp.*|ppc|retargeting|paid.*)$')
            AND (
                SELECT category
                FROM mapping_automatic
                WHERE category = 'SOURCE_CATEGORY_VIDEO'
                    AND source IN UNNEST(mapping_automatic.sources_list)
            ) IS NOT NULL
        THEN 'Paid Video'

        WHEN medium IN ('display', 'banner', 'expandable', 'interstitial', 'cpm')
        THEN 'Display'

        WHEN REGEXP_CONTAINS(medium, r'^(.*cp.*|ppc|retargeting|paid.*)$')
        THEN 'Paid Other'
        
        WHEN REGEXP_CONTAINS(campaign_name, r'^(.*(([^a-df-z]|^)shop|shopping).*)$')
            OR (
                SELECT category
                FROM mapping_automatic
                WHERE category = 'SOURCE_CATEGORY_SHOPPING' 
                    AND source IN UNNEST(mapping_automatic.sources_list)
            ) IS NOT NULL
        THEN 'Organic Shopping'

        WHEN medium IN ('social', 'social-network', 'social-media', 'sm', 'social network', 'social media')
            OR (
                SELECT category
                FROM mapping_automatic
                WHERE category = 'SOURCE_CATEGORY_SOCIAL'
                    AND source IN UNNEST(mapping_automatic.sources_list)
            ) IS NOT NULL
        THEN 'Organic Social'

        WHEN REGEXP_CONTAINS(medium, r'^(.*video.*)$')
            OR (
                SELECT category
                FROM mapping_automatic
                WHERE category = 'SOURCE_CATEGORY_VIDEO'
                    AND source IN UNNEST(mapping_automatic.sources_list)
            ) IS NOT NULL
        THEN 'Organic Video'

        WHEN medium = 'organic'
            OR (
                SELECT category
                FROM mapping_automatic
                WHERE category = 'SOURCE_CATEGORY_SEARCH' 
                AND source IN UNNEST(mapping_automatic.sources_list)
            ) IS NOT NULL
        THEN 'Organic Search'

        WHEN medium IN ('referral', 'app', 'link')
        THEN 'Referral'

        WHEN REGEXP_CONTAINS(source_medium, 'e[-_\s]?mail')
        THEN 'Email'

        WHEN medium = 'affiliate'
        THEN 'Affiliates'

        WHEN medium = 'audio'
        THEN 'Audio'

        WHEN REGEXP_CONTAINS(source_medium, 'sms')
        THEN 'SMS'

        WHEN source = 'firebase'
            OR REGEXP_CONTAINS(medium, 'push$|mobile|notification')
        THEN 'Mobile Push Notifications'

        ELSE 'Unassigned'
        END AS channel_group

    FROM sessions_origins
)

-- now for each session and for each channel, only keep its first occurrence
-- this is not to have duplicated channels for the same given session
, sessions_channels AS (
    SELECT
        session_id,
        ARRAY_AGG(source ORDER BY event_timestamp)[OFFSET(0)] AS source,
        ARRAY_AGG(medium ORDER BY event_timestamp)[OFFSET(0)] AS medium,
        ARRAY_AGG(campaign_name ORDER BY event_timestamp)[OFFSET(0)] AS campaign_name,
        ARRAY_AGG(source_medium ORDER BY event_timestamp)[OFFSET(0)] AS source_medium,
        channel_group,
        MAX(f_tp) AS f_tp,
        MIN(event_timestamp) AS event_timestamp
    FROM sessions_channels_base
    GROUP BY session_id, channel_group
)

-- finally combine mapped events with session-level data and customer mapping
SELECT
    s.session_id,
    s.cookie_id,
    s.is_engaged,
    s.country_web,
    e.f_tp,
    e.event_timestamp,
    e.source,
    e.medium,
    e.campaign_name,
    e.source_medium,
    e.channel_group,
    s.f_converted,
    s.total_transactions,
    s.total_item_quantity,
    s.session_revenue_usd,
    s.first_event_timestamp,
    s.last_event_timestamp
FROM sessions_base AS s
LEFT JOIN sessions_channels AS e ON s.session_id = e.session_id
WHERE e.session_id IS NOT NULL
