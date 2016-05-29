SELECT
    11022200782971 AS action_id,
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
            atlas_attribution_last_click_click_based(internal_contributor_event_ts,internal_category,255,placement_id,ad_id,creative_id,audience_id,ad_event_id) OVER (PARTITION BY internal_conversion_event_ts,internal_action_tag_fbid,internal_hashed_fbid) AS conversions_click_based_last_touch,
            atlas_attribution_even_credit(internal_contributor_event_ts,internal_category,255) OVER (PARTITION BY internal_conversion_event_ts,internal_action_tag_fbid,internal_hashed_fbid) AS conversions_full_path,
            atlas_attribution_last_click(internal_contributor_event_ts,internal_category,255,placement_id,ad_id,creative_id,audience_id,ad_event_id) OVER (PARTITION BY internal_conversion_event_ts,internal_action_tag_fbid,internal_hashed_fbid) AS conversions_last_touch,
            atlas_attribution_last_click_view_based(internal_contributor_event_ts,internal_category,255,placement_id,ad_id,creative_id,audience_id,ad_event_id) OVER (PARTITION BY internal_conversion_event_ts,internal_action_tag_fbid,internal_hashed_fbid) AS conversions_view_based_last_touch,
            NULL AS dummy
        FROM (
            SELECT
                ce.*,
                c.ad_fbid AS ad_id,
                c.audience_fbid AS audience_id,
                c.device_type AS device_type,
                c.event_ts AS internal_contributor_event_ts,
                c.ad_event_fbid AS ad_event_id,
                c.creative_fbid AS creative_id,
                c.advertiser_fbid AS advertiser_id,
                c.placement_fbid AS placement_id,
                c.category AS internal_category,
                NULL AS dummy
            FROM (
                SELECT *,
                    atlas_is_repeated_conversion(internal_conversion_event_ts,60,120) OVER (PARTITION BY internal_hashed_fbid ORDER BY internal_conversion_event_ts) AS internal_is_repeated,
                        NULL AS dummy
                FROM (
                    SELECT
                        ce.action_tag_fbid AS action_id,
                        ce.hashed_fbid AS internal_hashed_fbid,
                        ce.event_ts AS internal_conversion_event_ts,
                        ce.action_tag_fbid AS internal_action_tag_fbid,
                        NULL AS dummy
                    FROM conversion_events ce
                    WHERE action_tag_fbid IN ( 11152200942590 )
                        AND ( ce.offline_action_batch_fbid = 0
                        OR ce.offline_action_batch_fbid NOT IN ( 0 ) )
                        AND ce.hashed_fbid <> 0
                        AND ce.event_ts between 1460444400 and 1463036400
                    AND NOT (ce.hashed_fbid = 1097334974130218754)
                    AND NOT (ce.hashed_fbid = 1223170964065720141)
                    AND NOT (ce.hashed_fbid = 2376733277266567740)
                    AND NOT (ce.hashed_fbid = 2677340769064894231)
                    AND NOT (ce.hashed_fbid = 267746005371472066)
                    AND NOT (ce.hashed_fbid = 2952667145608029622)
                    AND NOT (ce.hashed_fbid = 5588423229099024838)
                    AND NOT (ce.hashed_fbid = 5652273542304714584)
                    AND NOT (ce.hashed_fbid = 5805681970821068061)
                    AND NOT (ce.hashed_fbid = 6505821964018948130)
                    AND NOT (ce.hashed_fbid = 8432366750960467942)
                    AND NOT (ce.hashed_fbid = 8915407375144459195)
                    AND NOT (ce.native_identity_provider = 2 AND ce.native_identity_value = '3282997242609647394')
                    AND NOT (ce.native_identity_provider = 2 AND ce.native_identity_value = '-232831633068248153')
                    AND NOT (ce.hashed_fbid = 8462601398986346667 AND ce.event_ts >= 1459209600 AND ce.event_ts < 1893456000)
                    AND NOT (ce.hashed_fbid = 9050672120357634712 AND ce.event_ts >= 1459209600 AND ce.event_ts < 1893456000)
                ) attr_with_dupes
            ) ce
            INNER JOIN contributors c
                ON c.hashed_fbid = ce.internal_hashed_fbid
            WHERE c.advertiser_fbid IN ( 11152200768503 )
                AND c.event_ts < ce.internal_conversion_event_ts + 20
                AND ((c.category IN (3,5) AND c.event_ts >= ce.internal_conversion_event_ts - 2592000)
                    OR (c.category = 0 AND c.event_ts >= ce.internal_conversion_event_ts - 432000))
                AND c.event_ts between 1452585600 and 1463036400
            AND NOT (c.hashed_fbid = 1097334974130218754)
            AND NOT (c.hashed_fbid = 1223170964065720141)
            AND NOT (c.hashed_fbid = 2376733277266567740)
            AND NOT (c.hashed_fbid = 2677340769064894231)
            AND NOT (c.hashed_fbid = 267746005371472066)
            AND NOT (c.hashed_fbid = 2952667145608029622)
            AND NOT (c.hashed_fbid = 5588423229099024838)
            AND NOT (c.hashed_fbid = 5652273542304714584)
            AND NOT (c.hashed_fbid = 5805681970821068061)
            AND NOT (c.hashed_fbid = 6505821964018948130)
            AND NOT (c.hashed_fbid = 8432366750960467942)
            AND NOT (c.hashed_fbid = 8915407375144459195)
            AND NOT (c.native_identity_provider = 2 AND c.native_identity_value = '3282997242609647394')
            AND NOT (c.native_identity_provider = 2 AND c.native_identity_value = '-232831633068248153')
            AND NOT (c.hashed_fbid = 8462601398986346667 AND c.event_ts >= 1459209600 AND c.event_ts < 1893456000)
            AND NOT (c.hashed_fbid = 9050672120357634712 AND c.event_ts >= 1459209600 AND c.event_ts < 1893456000)
        ) cc
        WHERE NOT internal_is_repeated
    ) attr
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
            atlas_attribution_last_click_click_based(internal_contributor_event_ts,internal_category,255,placement_id,ad_id,creative_id,audience_id,ad_event_id) OVER (PARTITION BY internal_conversion_event_ts,internal_action_tag_fbid,internal_native_identity_provider,internal_native_identity_value) AS conversions_click_based_last_touch,
            atlas_attribution_even_credit(internal_contributor_event_ts,internal_category,255) OVER (PARTITION BY internal_conversion_event_ts,internal_action_tag_fbid,internal_native_identity_provider,internal_native_identity_value) AS conversions_full_path,
            atlas_attribution_last_click(internal_contributor_event_ts,internal_category,255,placement_id,ad_id,creative_id,audience_id,ad_event_id) OVER (PARTITION BY internal_conversion_event_ts,internal_action_tag_fbid,internal_native_identity_provider,internal_native_identity_value) AS conversions_last_touch,
            atlas_attribution_last_click_view_based(internal_contributor_event_ts,internal_category,255,placement_id,ad_id,creative_id,audience_id,ad_event_id) OVER (PARTITION BY internal_conversion_event_ts,internal_action_tag_fbid,internal_native_identity_provider,internal_native_identity_value) AS conversions_view_based_last_touch,
            NULL AS dummy
        FROM (
            SELECT
                ce.*,
                c.ad_fbid AS ad_id,
                c.audience_fbid AS audience_id,
                c.device_type AS device_type,
                c.event_ts AS internal_contributor_event_ts,
                c.ad_event_fbid AS ad_event_id,
                c.creative_fbid AS creative_id,
                c.advertiser_fbid AS advertiser_id,
                c.placement_fbid AS placement_id,
                c.category AS internal_category,
                NULL AS dummy
            FROM (
                SELECT *,
                    atlas_is_repeated_conversion(internal_conversion_event_ts,60,120) OVER (PARTITION BY internal_native_identity_provider,internal_native_identity_value ORDER BY internal_conversion_event_ts) AS internal_is_repeated,
                    NULL AS dummy
                FROM (
                    SELECT
                        ce.action_tag_fbid AS action_id,
                        ce.native_identity_provider AS internal_native_identity_provider,
                        ce.native_identity_value AS internal_native_identity_value,
                        ce.event_ts AS internal_conversion_event_ts,
                        ce.action_tag_fbid AS internal_action_tag_fbid,
                        NULL AS dummy
                    FROM conversion_events ce
                    WHERE action_tag_fbid IN ( 11152200942590 )
                        AND ( ce.offline_action_batch_fbid = 0
                        OR ce.offline_action_batch_fbid NOT IN ( 0 ) )
                        AND ce.hashed_fbid = 0 AND ce.native_identity_value <> ''''
                        AND ce.event_ts between 1460444400 and 1463036400
                    AND NOT (ce.hashed_fbid = 1097334974130218754)
                    AND NOT (ce.hashed_fbid = 1223170964065720141)
                    AND NOT (ce.hashed_fbid = 2376733277266567740)
                    AND NOT (ce.hashed_fbid = 2677340769064894231)
                    AND NOT (ce.hashed_fbid = 267746005371472066)
                    AND NOT (ce.hashed_fbid = 2952667145608029622)
                    AND NOT (ce.hashed_fbid = 5588423229099024838)
                    AND NOT (ce.hashed_fbid = 5652273542304714584)
                    AND NOT (ce.hashed_fbid = 5805681970821068061)
                    AND NOT (ce.hashed_fbid = 6505821964018948130)
                    AND NOT (ce.hashed_fbid = 8432366750960467942)
                    AND NOT (ce.hashed_fbid = 8915407375144459195)
                    AND NOT (ce.native_identity_provider = 2 AND ce.native_identity_value = '3282997242609647394')
                    AND NOT (ce.native_identity_provider = 2 AND ce.native_identity_value = '-232831633068248153')
                    AND NOT (ce.hashed_fbid = 8462601398986346667 AND ce.event_ts >= 1459209600 AND ce.event_ts < 1893456000)
                    AND NOT (ce.hashed_fbid = 9050672120357634712 AND ce.event_ts >= 1459209600 AND ce.event_ts < 1893456000)
                ) attr_with_dupes
            ) ce
            INNER JOIN contributors c
                ON c.native_identity_provider = ce.internal_native_identity_provider
                AND c.native_identity_value = ce.internal_native_identity_value
            WHERE c.advertiser_fbid IN ( 11152200768503 )
                AND c.event_ts < ce.internal_conversion_event_ts + 20
                AND ((c.category IN (3,5) AND c.event_ts >= ce.internal_conversion_event_ts - 2592000)
                    OR (c.category = 0 AND c.event_ts >= ce.internal_conversion_event_ts - 432000))
                AND c.event_ts between 1452585600 and 1463036400
            AND NOT (c.hashed_fbid = 1097334974130218754)
            AND NOT (c.hashed_fbid = 1223170964065720141)
            AND NOT (c.hashed_fbid = 2376733277266567740)
            AND NOT (c.hashed_fbid = 2677340769064894231)
            AND NOT (c.hashed_fbid = 267746005371472066)
            AND NOT (c.hashed_fbid = 2952667145608029622)
            AND NOT (c.hashed_fbid = 5588423229099024838)
            AND NOT (c.hashed_fbid = 5652273542304714584)
            AND NOT (c.hashed_fbid = 5805681970821068061)
            AND NOT (c.hashed_fbid = 6505821964018948130)
            AND NOT (c.hashed_fbid = 8432366750960467942)
            AND NOT (c.hashed_fbid = 8915407375144459195)
            AND NOT (c.native_identity_provider = 2 AND c.native_identity_value = '3282997242609647394')
            AND NOT (c.native_identity_provider = 2 AND c.native_identity_value = '-232831633068248153')
            AND NOT (c.hashed_fbid = 8462601398986346667 AND c.event_ts >= 1459209600 AND c.event_ts < 1893456000)
            AND NOT (c.hashed_fbid = 9050672120357634712 AND c.event_ts >= 1459209600 AND c.event_ts < 1893456000)
        ) cc
        WHERE NOT internal_is_repeated
    ) attr
    WHERE (attr.conversions_click_based_last_touch <> 0.0
        OR attr.conversions_full_path <> 0.0
        OR attr.conversions_last_touch <> 0.0
        OR attr.conversions_view_based_last_touch <> 0.0)
        AND attr.advertiser_id IN (11152200768503)
    GROUP BY 1, 2, 3, 4
) attr_all
GROUP BY 1, 2, 3, 4 ORDER BY 1,2,3,4