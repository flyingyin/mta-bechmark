SELECT
    11152200942590 AS action_id,
    attr_all."ad_id",
    attr_all."advertiser_id",
    attr_all."placement_id",
    SUM(attr_all."conversions_click_based_last_touch") AS "conversions_click_based_last_touch",
    SUM(attr_all."conversions_full_path") AS "conversions_full_path",
    SUM(attr_all."conversions_last_touch") AS "conversions_last_touch",
    SUM(attr_all."conversions_view_based_last_touch") AS "conversions_view_based_last_touch",
    NULL AS dummy
FROM (
    SELECT
        attr.action_id,
        attr.ad_id,
        attr.advertiser_id,
        attr.placement_id,
        SUM(attr.conversions_click_based_last_touch) AS conversions_click_based_last_touch,
        SUM(attr.conversions_full_path) AS conversions_full_path,
        SUM(attr.conversions_last_touch) AS conversions_last_touch,
        SUM(attr.conversions_view_based_last_touch) AS conversions_view_based_last_touch,
        NULL AS dummy
    FROM (
        SELECT *,
            atlas_attribution_last_click_click_based_new(internal_timestamp,internal_category,255,internal_action_tag_fbid,'{"query_params_map":{"11152200942590":{"click_window_seconds":2592000,"view_window_seconds":2592000,"repeated_conversion_widow_seconds":60,"order_id_window_seconds":0,"is_revenue_enabled":false,"advertiser_fbids":[11022200790603],"deleted_offline_action_batch_fbids":[0]}}}',placement_id,ad_id,creative_id,audience_id,ad_event_id) OVER (PARTITION BY internal_hashed_fbid ORDER BY internal_timestamp,internal_action_tag_fbid) AS conversions_click_based_last_touch,
            atlas_attribution_even_credit_new(internal_timestamp,internal_category,255,internal_action_tag_fbid,'{"query_params_map":{"11152200942590":{"click_window_seconds":2592000,"view_window_seconds":2592000,"repeated_conversion_widow_seconds":60,"order_id_window_seconds":0,"is_revenue_enabled":false,"advertiser_fbids":[11022200790603],"deleted_offline_action_batch_fbids":[0]}}}',placement_id,ad_id,creative_id,audience_id,ad_event_id) OVER (PARTITION BY internal_hashed_fbid ORDER BY internal_timestamp,internal_action_tag_fbid) AS conversions_full_path,
            atlas_attribution_last_click_new(internal_timestamp,internal_category,255,internal_action_tag_fbid,'{"query_params_map":{"11152200942590":{"click_window_seconds":2592000,"view_window_seconds":2592000,"repeated_conversion_widow_seconds":60,"order_id_window_seconds":0,"is_revenue_enabled":false,"advertiser_fbids":[11022200790603],"deleted_offline_action_batch_fbids":[0]}}}',placement_id,ad_id,creative_id,audience_id,ad_event_id) OVER (PARTITION BY internal_hashed_fbid ORDER BY internal_timestamp,internal_action_tag_fbid) AS conversions_last_touch,
            atlas_attribution_last_click_view_based_new(internal_timestamp,internal_category,255,internal_action_tag_fbid,'{"query_params_map":{"11152200942590":{"click_window_seconds":2592000,"view_window_seconds":2592000,"repeated_conversion_widow_seconds":60,"order_id_window_seconds":0,"is_revenue_enabled":false,"advertiser_fbids":[11022200790603],"deleted_offline_action_batch_fbids":[0]}}}',placement_id,ad_id,creative_id,audience_id,ad_event_id) OVER (PARTITION BY internal_hashed_fbid ORDER BY internal_timestamp,internal_action_tag_fbid) AS conversions_view_based_last_touch,
            NULL AS dummy
        FROM (
            SELECT *,
                atlas_is_repeated_conversion_new(internal_timestamp,60,120,internal_category) OVER (PARTITION BY internal_hashed_fbid ORDER BY internal_timestamp) AS internal_is_repeated,
                NULL AS dummy
            FROM (
                SELECT
                    cce.audience_fbid AS audience_id,
                    cce.device_type AS device_type,
                    (CASE WHEN category IN (0, 3, 5) THEN event_ts ELSE 0 END) AS internal_contributor_event_ts,
                    cce.ad_event_fbid AS ad_event_id,
                    cce.creative_fbid AS creative_id,
                    cce.hashed_fbid AS internal_hashed_fbid,
                    cce.advertiser_fbid AS advertiser_id,
                    cce.placement_fbid AS placement_id,
                    (CASE WHEN category IN (2) THEN event_ts ELSE 0 END) AS internal_conversion_event_ts,
                    cce.action_tag_fbid AS internal_action_tag_fbid,
                    cce.ad_fbid AS ad_id,
                    cce.action_tag_fbid AS action_id,
                    cce.event_ts AS internal_timestamp,
                    cce.category AS internal_category,
                    NULL AS dummy
                FROM conversion_contributor_event cce
                WHERE advertiser_fbid IN ( 11152200768503 )
                    AND action_tag_fbid IN (0, 11152200942590)
                        AND ((cce.category IN (2) AND cce.event_ts between 1462086000 and 1464764400) 
                          OR (cce.category IN (3, 5) AND cce.event_ts between 1459494000 and 1464764400) 
                          OR (cce.category IN (0) AND cce.event_ts between 1459494000 and 1464764400))
                    AND cce.hashed_fbid <> 0
                    AND NOT (cce.hashed_fbid = 1097334974130218754)
                    AND NOT (cce.hashed_fbid = 1223170964065720141)
                    AND NOT (cce.hashed_fbid = 2376733277266567740)
                    AND NOT (cce.hashed_fbid = 2677340769064894231)
                    AND NOT (cce.hashed_fbid = 267746005371472066)
                    AND NOT (cce.hashed_fbid = 2952667145608029622)
                    AND NOT (cce.hashed_fbid = 5588423229099024838)
                    AND NOT (cce.hashed_fbid = 5652273542304714584)
                    AND NOT (cce.hashed_fbid = 5805681970821068061)
                    AND NOT (cce.hashed_fbid = 6505821964018948130)
                    AND NOT (cce.hashed_fbid = 8432366750960467942)
                    AND NOT (cce.hashed_fbid = 8915407375144459195)
                    AND NOT (cce.native_identity_provider = 2 AND cce.native_identity_value = '3282997242609647394')
                    AND NOT (cce.native_identity_provider = 2 AND cce.native_identity_value = '-232831633068248153')
                    AND NOT (cce.hashed_fbid = 8462601398986346667 AND cce.event_ts >= 1459209600 AND cce.event_ts < 1893456000)
                    AND NOT (cce.hashed_fbid = 9050672120357634712 AND cce.event_ts >= 1459209600 AND cce.event_ts < 1893456000)
                ) attr_with_dupes
            ) cc
            WHERE NOT internal_is_repeated
        ) attr
        INNER JOIN placement ON placement.fbid = attr.placement_id
        INNER JOIN site ON site.fbid = placement.site_fbid
        WHERE (attr.conversions_click_based_last_touch <> 0.0
            OR attr.conversions_full_path <> 0.0
            OR attr.conversions_last_touch <> 0.0
            OR attr.conversions_view_based_last_touch <> 0.0)
            AND attr.advertiser_id IN (11152200768503)
        GROUP BY 1, 2, 3, 4

        UNION ALL

        SELECT
            attr.action_id,
            attr.ad_id,
            attr.advertiser_id,
            attr.placement_id,
            SUM(attr.conversions_click_based_last_touch) AS conversions_click_based_last_touch,
            SUM(attr.conversions_full_path) AS conversions_full_path,
            SUM(attr.conversions_last_touch) AS conversions_last_touch,
            SUM(attr.conversions_view_based_last_touch) AS conversions_view_based_last_touch,
            NULL AS dummy
        FROM (
            SELECT *,
                atlas_attribution_last_click_click_based_new(internal_timestamp,internal_category,255,internal_action_tag_fbid,'{"query_params_map":{"11152200942590":{"click_window_seconds":2592000,"view_window_seconds":2592000,"repeated_conversion_widow_seconds":60,"order_id_window_seconds":0,"is_revenue_enabled":false,"advertiser_fbids":[11022200790603],"deleted_offline_action_batch_fbids":[0]}}}',placement_id,ad_id,creative_id,audience_id,ad_event_id) OVER (PARTITION BY internal_native_identity_provider,internal_native_identity_value ORDER BY internal_timestamp,internal_action_tag_fbid) AS conversions_click_based_last_touch,
                atlas_attribution_even_credit_new(internal_timestamp,internal_category,255,internal_action_tag_fbid,'{"query_params_map":{"11152200942590":{"click_window_seconds":2592000,"view_window_seconds":2592000,"repeated_conversion_widow_seconds":60,"order_id_window_seconds":0,"is_revenue_enabled":false,"advertiser_fbids":[11022200790603],"deleted_offline_action_batch_fbids":[0]}}}',placement_id,ad_id,creative_id,audience_id,ad_event_id) OVER (PARTITION BY internal_native_identity_provider,internal_native_identity_value ORDER BY internal_timestamp,internal_action_tag_fbid) AS conversions_full_path,
                atlas_attribution_last_click_new(internal_timestamp,internal_category,255,internal_action_tag_fbid,'{"query_params_map":{"11152200942590":{"click_window_seconds":2592000,"view_window_seconds":2592000,"repeated_conversion_widow_seconds":60,"order_id_window_seconds":0,"is_revenue_enabled":false,"advertiser_fbids":[11022200790603],"deleted_offline_action_batch_fbids":[0]}}}',placement_id,ad_id,creative_id,audience_id,ad_event_id) OVER (PARTITION BY internal_native_identity_provider,internal_native_identity_value ORDER BY internal_timestamp,internal_action_tag_fbid) AS conversions_last_touch,
                atlas_attribution_last_click_view_based_new(internal_timestamp,internal_category,255,internal_action_tag_fbid,'{"query_params_map":{"11152200942590":{"click_window_seconds":2592000,"view_window_seconds":2592000,"repeated_conversion_widow_seconds":60,"order_id_window_seconds":0,"is_revenue_enabled":false,"advertiser_fbids":[11022200790603],"deleted_offline_action_batch_fbids":[0]}}}',placement_id,ad_id,creative_id,audience_id,ad_event_id) OVER (PARTITION BY internal_native_identity_provider,internal_native_identity_value ORDER BY internal_timestamp,internal_action_tag_fbid) AS conversions_view_based_last_touch,
                NULL AS dummy
            FROM (
                SELECT *,
                    atlas_is_repeated_conversion_new(internal_timestamp,60,120,internal_category) OVER (PARTITION BY internal_native_identity_provider,internal_native_identity_value ORDER BY internal_timestamp) AS internal_is_repeated,
                    NULL AS dummy
                FROM (
                    SELECT
                        cce.native_identity_provider AS internal_native_identity_provider,
                        to_unixtime(date_trunc('day', from_unixtime(cce.event_ts) at time zone 'America/New_York')) AS statistics_date,
                        cce.native_identity_value AS internal_native_identity_value,
                        cce.audience_fbid AS audience_id,
                        cce.device_type AS device_type,
                        (CASE WHEN category IN (0, 3, 5) THEN event_ts ELSE 0 END) AS internal_contributor_event_ts,
                        cce.ad_event_fbid AS ad_event_id,
                        cce.creative_fbid AS creative_id,
                        cce.advertiser_fbid AS advertiser_id,
                        cce.placement_fbid AS placement_id,
                        (CASE WHEN category IN (2) THEN event_ts ELSE 0 END) AS internal_conversion_event_ts,
                        cce.action_tag_fbid AS internal_action_tag_fbid,
                        cce.ad_fbid AS ad_id,
                        cce.action_tag_fbid AS action_id,
                        cce.event_ts AS internal_timestamp,
                        cce.category AS internal_category,
                        NULL AS dummy
                    FROM conversion_contributor_event cce
                    WHERE advertiser_fbid IN ( 11152200768503 )
                        AND action_tag_fbid IN (0, 11152200942590)
                    AND ((cce.category IN (2) AND cce.event_ts between 1462086000 and 1464764400) 
                      OR (cce.category IN (3, 5) AND cce.event_ts between 1459494000 and 1464764400) 
                      OR (cce.category IN (0) AND cce.event_ts between 1459494000 and 1464764400))
                        AND cce.hashed_fbid = 0 AND cce.native_identity_value <> ''
                        AND NOT (cce.hashed_fbid = 1097334974130218754)
                        AND NOT (cce.hashed_fbid = 1223170964065720141)
                        AND NOT (cce.hashed_fbid = 2376733277266567740)
                        AND NOT (cce.hashed_fbid = 2677340769064894231)
                        AND NOT (cce.hashed_fbid = 267746005371472066)
                        AND NOT (cce.hashed_fbid = 2952667145608029622)
                        AND NOT (cce.hashed_fbid = 5588423229099024838)
                        AND NOT (cce.hashed_fbid = 5652273542304714584)
                        AND NOT (cce.hashed_fbid = 5805681970821068061)
                        AND NOT (cce.hashed_fbid = 6505821964018948130)
                        AND NOT (cce.hashed_fbid = 8432366750960467942)
                        AND NOT (cce.hashed_fbid = 8915407375144459195)
                        AND NOT (cce.native_identity_provider = 2 AND cce.native_identity_value = '3282997242609647394')
                        AND NOT (cce.native_identity_provider = 2 AND cce.native_identity_value = '-232831633068248153')
                        AND NOT (cce.hashed_fbid = 8462601398986346667 AND cce.event_ts >= 1459209600 AND cce.event_ts < 1893456000)
                        AND NOT (cce.hashed_fbid = 9050672120357634712 AND cce.event_ts >= 1459209600 AND cce.event_ts < 1893456000)
                    ) attr_with_dupes
                ) cc
                WHERE NOT internal_is_repeated
            ) attr
            INNER JOIN placement ON placement.fbid = attr.placement_id
            INNER JOIN site ON site.fbid = placement.site_fbid
            WHERE (attr.conversions_click_based_last_touch <> 0.0
                OR attr.conversions_full_path <> 0.0
                OR attr.conversions_last_touch <> 0.0
                OR attr.conversions_view_based_last_touch <> 0.0)
                AND attr.advertiser_id IN (11152200768503)
            GROUP BY 1, 2, 3, 4
        ) attr_all
        GROUP BY 1, 2, 3, 4
