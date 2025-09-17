WITH intersections (id, intersection_name_api) AS (
    VALUES
    ('3a6c4b1b-91cc-4600-8b9e-f3416839397b', 'The Queensway and The East Mall'),
    ('ae52ca3c-9a25-4c2c-a726-24388616fe54', 'Lawrence Ave E and Fortune Gate'),
    ('c85d49f2-bd4a-4649-b967-ce24e39ac102', 'Lawrence Ave E and Galloway Rd'),
    ('0f37f5c0-9eb0-45d8-80cf-bd3d3718c2d4', 'Lawrence Ave E and Mossbank Dr'),
    ('0b4e1e39-a9e9-46a3-895f-0f21924c0e54', 'Lawrence Ave E and Scarborough Golf Club Road'),
    ('01150688-deb1-4565-a0da-236ed77cad12', 'Orton Park Rd and Lawrence Ave E'),
    ('0726384f-e753-4826-9ef2-72a817776ca5', 'Overture Rd and Lawrence Ave E'),
    ('843d1d1f-5350-4aaf-a764-ec8ebf847673', 'Greenholm Circuit and Lawrence Ave E'),
    ('5de86391-7559-48e3-aa46-7cfe0b28bd05', 'The Queensway and The West Mall')
),

enriched_data AS (
    SELECT
        i.id,
        SPLIT_PART(i.intersection_name_api, ' and ', 1) AS street_main,
        SPLIT_PART(i.intersection_name_api, ' and ', 2) AS street_cross,
        g.int_result[3] AS int_id,
        ts.px::int AS px,
        ts.geom
    FROM intersections AS i
    JOIN gis._get_intersection_id(
        SPLIT_PART(i.intersection_name_api, ' and ', 1),
        SPLIT_PART(i.intersection_name_api, ' and ', 2),
        0
    ) AS g (int_result) ON TRUE
    LEFT JOIN gis.traffic_signal AS ts ON ts.node_id = g.int_result[3]
)

UPDATE miovision_api.intersections AS target
SET
    px = enriched_data.px,
    geom = enriched_data.geom,
    int_id = enriched_data.int_id,
    street_main = enriched_data.street_main,
    street_cross = enriched_data.street_cross
FROM enriched_data
WHERE target.id = enriched_data.id;
