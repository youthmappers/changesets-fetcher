--BEGIN h3_daily_aggregation
SELECT
    h3,
    uid,
    CAST(created_at AS date) as created_at,
    CAST(
        ROW(
            COALESCE(SUM(buildings.new),0), 
            COALESCE(SUM(buildings.edited),0)
        ) AS ROW(new INT, edited INT)
    ) AS buildings,
    CAST(
        ROW(
            COALESCE(SUM(highways.new),0), 
            COALESCE(SUM(highways.edited),0)
        ) AS ROW(new INT, edited INT)
    ) AS highways,
    CAST(
        ROW(
            COALESCE(SUM(amenities.new),0), 
            COALESCE(SUM(amenities.edited),0)
        ) AS ROW(new INT, edited INT)
    ) AS amenities,
    CAST(
        ROW(
            COALESCE(SUM(elements.new),0), 
            COALESCE(SUM(elements.edited), 0),
            COALESCE(SUM(elements.deleted), 0),
            COALESCE(SUM(elements.num_changes), 0)
        ) AS ROW( new INT, edited INT, deleted INT, num_changes INT)
    ) AS elements,
    CAST(
        ROW(
            COALESCE(SUM(features.new), 0),
            COALESCE(SUM(features.edited), 0),
            COALESCE(SUM(features.new_vertices), 0),
            COALESCE(SUM(features.edited_vertices), 0)
        ) AS ROW(new INT, edited INT, new_vertices INT, edited_vertices INT)
    ) AS features,

    CAST(
        ROW(
            COALESCE(SUM(nodes.new), 0),
            COALESCE(SUM(nodes.edited), 0),
            COALESCE(SUM(nodes.deleted), 0)
        )AS ROW(new INT, edited INT, deleted INT)
    ) AS nodes,
    CAST(
        ROW(
            COALESCE(SUM(ways.new), 0),
            COALESCE(SUM(ways.edited), 0),
            COALESCE(SUM(ways.deleted), 0)
        ) AS ROW(new INT, edited INT, deleted INT)
    ) AS ways,
    CAST(
        ROW(
            COALESCE(SUM(relations.new), 0),
            COALESCE(SUM(relations.edited), 0),
            COALESCE(SUM(relations.deleted), 0)
        ) AS ROW(new INT, edited INT, deleted INT)
    ) AS relations,
    ARRAY_AGG(bbox) AS bboxes,
    -- Geometry Collection of the centroids that exist in this h3 cell
    ST_Union_Agg(geometry) AS geometry,
FROM ym_changesets
WHERE geometry IS NOT NULL
GROUP BY 1,2,3
--END 

--BEGIN weekly-rollup
SELECT 
    chapter_id,
    date_trunc('week', created_at) as week,
    CAST( sum(features.new + features.edited) AS INT) AS all_feats,
    CAST( sum(buildings.new + buildings.edited) AS INT) AS buildings,
    CAST( sum(highways.new + highways.edited) AS INT) AS highways,
    CAST( sum(amenities.new + amenities.edited) AS INT) AS amenities,
    CAST( sum(features.new + features.edited - buildings.new - buildings.edited - highways.new - highways.edited - amenities.new - amenities.edited) AS INT) AS other,
    count(distinct(uid)) AS mappers,
FROM changesets_gb_h3_day
GROUP BY 1,2
--END

-- BEGIN monthly-activity
SELECT
    date_trunc('month', created_at) + INTERVAL 14 DAY AS month,
    sum(buildings.new) as new_buildings,
    sum(highways.new) as new_highways,
    sum(amenities.new) as new_amenities,
    sum(buildings.edited) as edited_buildings,
    sum(highways.edited) as edited_highways,
    sum(amenities.edited) as edited_amenities,
    count(distinct(chapter_id)) as chapters,
    count(distinct(uid)) as users
FROM changesets_gb_h3_day
WHERE created_at < date_trunc('month', today())
GROUP BY 1
ORDER BY 1 ASC
--END

-- BEGIN most-edited-countries
SELECT 
    country,
    date_trunc('month', created_at) + INTERVAL 14 DAY AS month,
    sum(features.edited + features.new) AS all_feats
FROM ym_changesets
WHERE created_at > date_trunc('month', (today() - INTERVAL 90 day)) AND country IS NOT NULL
GROUP BY 1,2
ORDER BY all_feats DESC
-- END

-- BEGIN daily-level-tile-summaries
SELECT
    h3,
    CAST(epoch(created_at) AS INT) AS timestamp,
    chapter_id,
    CAST( features.new + features.edited AS INT) AS all_feats,
    CAST( buildings.new + buildings.edited AS INT) AS buildings,
    CAST( highways.new + highways.edited AS INT) AS highways,
    CAST( amenities.new + amenities.edited AS INT) AS amenities,
    ST_CENTROID(geometry) AS geometry
FROM changesets_gb_h3_day
-- END

-- BEGIN daily-level-bboxes
SELECT
    h3,
    CAST(epoch(created_at) AS INT) AS timestamp,
    chapter_id,
    features.new + features.edited AS all_feats,
    buildings.new + buildings.edited AS buildings,
    highways.new + highways.edited AS highways,
    amenities.new + amenities.edited AS amenities,
    ST_ENVELOPE(
        ST_COLLECT(ARRAY[
            ST_POINT(
                list_sort(list_transform(bboxes, bbox -> bbox.xmin))[1],
                list_sort(list_transform(bboxes, bbox -> bbox.ymin))[1]
            ),
            ST_POINT(
                list_sort(list_transform(bboxes, bbox -> bbox.xmax))[-1],
                list_sort(list_transform(bboxes, bbox -> bbox.ymax))[-1]
            )
        ])
    ) AS geometry                
FROM changesets_gb_h3_day