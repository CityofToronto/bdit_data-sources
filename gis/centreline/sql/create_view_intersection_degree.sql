CREATE OR REPLACE VIEW gis_core.intersection_degree AS
WITH all_intersections AS (
    SELECT
        intersections.intersection_id,
        intersections.classification_desc,
        cent.feature_code_desc,
        intersections.number_of_elevations,
        cent.centreline_id,
        cent.linear_name_full,
        cent.geom AS centreline_geom,
        intersections.geom AS intersection_geom,
        cent.feature_code
    FROM gis_core.centreline_latest AS cent
    JOIN gis_core.centreline_intersection_point_latest AS intersections
        ON cent.from_intersection_id = intersections.intersection_id
    UNION
    SELECT
        intersections.intersection_id,
        intersections.classification_desc,
        cent.feature_code_desc,
        intersections.number_of_elevations,
        cent.centreline_id,
        cent.linear_name_full,
        cent.geom AS centreline_geom,
        intersections.geom AS intersection_geom,
        cent.feature_code
    FROM gis_core.centreline_latest AS cent
    JOIN gis_core.centreline_intersection_point_latest AS intersections
        ON cent.to_intersection_id = intersections.intersection_id
),

centreline_agg AS (
    SELECT
        intersection_id,
        array_agg(centreline_id) AS centreline_list
    FROM all_intersections
    GROUP BY intersection_id
)

SELECT
    dist_feature.intersection_id,
    array_agg(
        dist_feature.feature_code_desc
        ORDER BY dist_feature.feature_code
    ) AS feature_desc_list,
    (array_agg(
        dist_feature.feature_code_desc
        ORDER BY dist_feature.feature_code
    ))[
        1
    ] AS highest_order_feature,
    dist_feature.number_of_elevations,
    array_length(centreline_agg.centreline_list, 1) AS degree,
    centreline_agg.centreline_list,
    dist_feature.geom
FROM (
    SELECT DISTINCT
        all_intersections.intersection_id,
        all_intersections.feature_code_desc,
        all_intersections.number_of_elevations,
        all_intersections.geom,
        all_intersections.feature_code
    FROM all_intersections
) AS dist_feature
LEFT JOIN centreline_agg USING (intersection_id)
GROUP BY
    dist_feature.intersection_id,
    dist_feature.number_of_elevations,
    dist_feature.geom,
    centreline_agg.centreline_list;

COMMENT ON VIEW gis_core.intersection_degree IS
'This view provides a summary of intersections, including road class involved, highest degree of 
road class, number of elevations, degree (number of connected centreline segments).';
