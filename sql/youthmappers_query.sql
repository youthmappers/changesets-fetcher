-- Latest UIDS of interest from YouthMappers table
WITH uids AS (
    SELECT DISTINCT 
        uid,
        ds AS youthmappers_ds
    FROM 
        youthmappers
    WHERE 
        ds = (SELECT max(ds) FROM youthmappers)
),
-- Changesets table with stats from planet-rollup joined in
changesets_plus AS (
    SELECT changesets.id,
        changesets.uid,
        changesets.user AS username,
        changesets.created_at,
        changesets.closed_at,
        element_at(changesets.tags, 'comment') AS comment,
        element_at(changesets.tags, 'created_by') as created_by,
        element_at(changesets.tags, 'host') AS host,
        element_at(changesets.tags, 'imagery_used') as imagery_used,
        element_at(changesets.tags, 'source') AS source,
        split(element_at(changesets.tags, 'hashtags'), ';') AS hashtags,
        MAP_FILTER(
            changesets.tags,
            (k, v)->k NOT IN (
                'comment',
                'created_by',
                'hashtags',
                'host',
                'imagery_used',
                'source'
            )
        ) AS tags,
        changesets.num_changes,
        CAST(
            ROW(
                min_lon,
                min_lat,
                max_lon,
                max_lat,
                TRY(
                    ROUND(
                        great_circle_distance(min_lat, min_lon, max_lat, max_lon),
                        3
                    )
                )
            ) AS ROW(
                xmin decimal(10, 7),
                ymin decimal(9, 7),
                xmax decimal(10, 7),
                ymax decimal(9, 7),
                diameter double
            )
        ) AS bbox,
        ST_ASBINARY(
            IF(
                min_lon = max_lon
                AND min_lat = max_lat,
                TRY(ST_POINT(min_lon, max_lat)),
                TRY(
                    ST_CENTROID(
                        ST_ENVELOPE(
                            ST_LINESTRING(
                                ARRAY [ ST_POINT(min_lon, min_lat),
                                ST_POINT(max_lon, max_lat) ]
                            )
                        )
                    )
                )
            )
        ) AS geometry,
        uids.youthmappers_ds
    FROM changesets changesets
    -- This is where we limit to YouthMappers UIDs only
    JOIN uids ON uids.uid = changesets.uid
    -- Date filter for all changesets (YouthMappers founded in 2015)
    WHERE changesets.created_at >= DATE '2015-01-01'
),
-- Query the planet_history table
planet_rollup AS (
    SELECT planet_history.changeset,
        -- Buildings
        count_if(
            element_at(planet_history.tags, 'building') IS NOT NULL
            AND (version = 1)
        ) new_buildings,
        count_if(
            element_at(planet_history.tags, 'building') IS NOT NULL
            AND (version > 1)
        ) AS edited_buildings,
        -- Highways
        count_if(
            element_at(planet_history.tags, 'highway') IS NOT NULL
            AND (version = 1)
        ) new_highways,
        count_if(
            element_at(planet_history.tags, 'highway') IS NOT NULL
            AND (version > 1)
        ) AS edited_highways,
        -- Amenities
        count_if(element_at(planet_history.tags, 'amenity') IS NOT NULL
            AND (version = 1)
        ) AS new_amenities,
        count_if(
            element_at(planet_history.tags, 'amenity') IS NOT NULL
            AND (version > 1)
        ) AS edited_amenities,

        -- OSM elements
        count_if(type = 'node' AND version = 1) AS new_nodes,
        count_if(type = 'node' AND version > 1) AS edited_nodes,
        count_if(type = 'node' AND NOT visible) AS deleted_nodes,
        count_if(type = 'way' AND version = 1) AS new_ways,
        count_if(type = 'way' AND version > 1) AS edited_ways,
        count_if(type = 'way' AND NOT visible ) AS deleted_ways,
        count_if(type = 'relation' AND version = 1) AS new_relations,
        count_if(type = 'relation' AND version > 1) AS edited_relations,
        count_if(type = 'relation' AND NOT visible ) AS deleted_relations,
        count_if(NOT visible) AS deleted_elements,
        count_if(visible AND (version = 1) ) AS new_elements,
        count_if(visible AND (version > 1) ) AS edited_elements,


        -- Features & Vertices
        count_if(visible AND version = 1 AND type = 'node' AND CARDINALITY(planet_history.tags)=0) AS new_vertices,
        count_if(visible AND version > 1 AND type = 'node' AND CARDINALITY(planet_history.tags)=0) AS edited_vertices,
        
        count_if(
            visible
            AND (version > 1) AND (cardinality(planet_history.tags) > 0)
        ) AS edited_features,
        count_if(
            (version = 1)
            AND (cardinality(planet_history.tags) > 0)
        ) AS new_features
    FROM planet_history planet_history
        JOIN changesets_plus changesets_plus ON changesets_plus.id = planet_history.changeset
    GROUP BY planet_history.changeset
)
-- Final selection & spatial join to country boundaries
SELECT id,
    uid,
    username,
    created_at,
    closed_at,
    comment,
    created_by,
    imagery_used,
    host,
    source,
    hashtags,
    NULLIF(tags, MAP()) AS tags,
    IF(
        (planet_rollup.new_buildings > 0) OR (planet_rollup.edited_buildings > 0),
        CAST(
            ROW(
                planet_rollup.new_buildings,
                planet_rollup.edited_buildings
            ) AS ROW(new int, edited int)
        ),
        NULL
    ) AS buildings,
    IF(
        (planet_rollup.new_highways > 0) OR (planet_rollup.edited_highways > 0),
        CAST(
            ROW(
                planet_rollup.new_highways,
                planet_rollup.edited_highways
            ) AS ROW(new int, edited int)
        ),
        NULL
    ) AS highways,
    IF(
        (planet_rollup.new_amenities > 0) OR (planet_rollup.edited_amenities > 0),
        CAST(
            ROW(
                planet_rollup.new_amenities,
                planet_rollup.edited_amenities
            ) AS ROW(new int, edited int)
        ),
        NULL
    ) AS amenities,

    -- Individual OSM Elements
    IF(
        (planet_rollup.new_nodes > 0) OR (planet_rollup.edited_nodes > 0) OR planet_rollup.deleted_nodes > 0,
        CAST(
            ROW(
                planet_rollup.new_nodes, 
                planet_rollup.edited_nodes,
                planet_rollup.deleted_nodes
            )
            AS ROW(
                new int, edited int, deleted int
            )
        ),
        NULL
    ) AS nodes,
    IF(
        (planet_rollup.new_ways > 0) OR (planet_rollup.edited_ways > 0) OR (planet_rollup.deleted_ways > 0),
        CAST(
            ROW(
                planet_rollup.new_ways,
                planet_rollup.edited_ways,
                planet_rollup.deleted_ways
            )
            AS ROW(
                new int, 
                edited int, 
                deleted int
            )
        ),
        NULL
    ) AS ways,
    IF(
        (planet_rollup.new_relations > 0) OR (planet_rollup.edited_relations > 0) OR (planet_rollup.deleted_relations > 0),
        CAST(
            ROW(
                planet_rollup.new_relations,
                planet_rollup.edited_relations,
                planet_rollup.deleted_relations
            )
            AS ROW(
                new int, 
                edited int, 
                deleted int
            )
        ),
        NULL
    ) AS relations,
    CAST(
        ROW(
            planet_rollup.new_elements,
            planet_rollup.edited_elements,
            planet_rollup.deleted_elements,
            num_changes
        )
        AS ROW(
            new int, 
            edited int, 
            deleted int, 
            num_changes int
        )
    ) AS elements,

    -- Features and Vertices
    CAST(
        ROW(
            planet_rollup.new_features, 
            planet_rollup.edited_features,
            planet_rollup.new_vertices,
            planet_rollup.edited_vertices
        ) AS ROW(
            new int,
            edited int,
            new_vertices int,
            edited_vertices int
        )
    ) AS features,

    -- Geometry functions
    bbox,
    changesets_plus.geometry,
    natural_earth_admin0.a3 AS a3,
    changesets_plus.youthmappers_ds,
    date_format(max(created_at), '%Y-%m-%d') AS ds
FROM changesets_plus changesets_plus
    JOIN planet_rollup planet_rollup ON changesets_plus.id = planet_rollup.changeset
    LEFT JOIN natural_earth_admin0 natural_earth_admin0 ON ST_Contains(
        ST_GeomFromBinary(natural_earth_admin0.geometry),
        ST_GeomFromBinary(changesets_plus.geometry)
    )