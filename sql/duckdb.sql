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