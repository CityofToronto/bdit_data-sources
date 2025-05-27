-- View: gis_core.intersection_classification

-- DROP VIEW gis_core.intersection_classification;

CREATE OR REPLACE VIEW gis_core.intersection_classification AS
WITH all_intersections AS (
    SELECT
        intersections.intersection_id,
        intersections.intersection_desc,
        intersections.classification_desc,
        cent.feature_code_desc,
        intersections.number_of_elevations,
        cent.centreline_id,
        cent.linear_name_full,
        cent.geom AS centreline_geom,
        intersections.geom,
        cent.feature_code
    FROM gis_core.centreline_latest AS cent
    JOIN
        gis_core.centreline_intersection_point_latest AS intersections
        ON cent.from_intersection_id = intersections.intersection_id
    WHERE
        intersections.classification_desc
        <> ALL(
            ARRAY[
                'Pseudo Intersection-Overpass/Underpass'::text,
                'Cul de Sac-Single Level'::text,
                'Pseudo Intersection-Single Level'::text
            ]
        )
    UNION
    SELECT
        intersections.intersection_id,
        intersections.intersection_desc,
        intersections.classification_desc,
        cent.feature_code_desc,
        intersections.number_of_elevations,
        cent.centreline_id,
        cent.linear_name_full,
        cent.geom AS centreline_geom,
        intersections.geom,
        cent.feature_code
    FROM gis_core.centreline_latest AS cent
    JOIN
        gis_core.centreline_intersection_point_latest AS intersections
        ON cent.to_intersection_id = intersections.intersection_id
    WHERE
        intersections.classification_desc
        <> ALL(
            ARRAY[
                'Pseudo Intersection-Overpass/Underpass'::text,
                'Cul de Sac-Single Level'::text,
                'Pseudo Intersection-Single Level'::text
            ]
        )
),

centreline_agg AS (
    SELECT
        all_intersections.intersection_id,
        array_agg(DISTINCT all_intersections.centreline_id) AS centreline_list,
        count(DISTINCT all_intersections.linear_name_full) AS count_name,
        array_agg(DISTINCT all_intersections.linear_name_full) AS linear_list,
        array_agg(all_intersections.feature_code_desc) AS all_feature_code_list,
        st_union(all_intersections.centreline_geom) AS cent_geom
    FROM all_intersections
    GROUP BY all_intersections.intersection_id
),

staging AS (
    SELECT
        dist_feature.intersection_id,
        dist_feature.intersection_desc,
        array_agg(
            dist_feature.feature_code_desc
            ORDER BY dist_feature.feature_code
        ) AS distinct_feature_desc_list,
        (array_agg(
            dist_feature.feature_code_desc
            ORDER BY dist_feature.feature_code
        ))[
            1
        ] AS highest_order_feature,
        centreline_agg.all_feature_code_list,
        dist_feature.number_of_elevations,
        centreline_agg.count_name,
        centreline_agg.linear_list AS road_names,
        array_length(centreline_agg.centreline_list, 1) AS degree,
        dist_feature.classification_desc,
        centreline_agg.centreline_list AS centreline_ids,
        dist_feature.geom,
        centreline_agg.cent_geom
    FROM (SELECT DISTINCT
        all_intersections.intersection_id,
        all_intersections.intersection_desc,
        all_intersections.feature_code_desc,
        all_intersections.classification_desc,
        all_intersections.number_of_elevations,
        all_intersections.geom,
        all_intersections.feature_code
    FROM all_intersections) AS dist_feature
    LEFT JOIN centreline_agg USING (intersection_id)
    GROUP BY
        dist_feature.intersection_id,
        dist_feature.intersection_desc,
        dist_feature.classification_desc,
        dist_feature.number_of_elevations,
        dist_feature.geom,
        centreline_agg.linear_list,
        centreline_agg.count_name,
        centreline_agg.cent_geom,
        centreline_agg.centreline_list,
        centreline_agg.all_feature_code_list
),

elev AS (SELECT DISTINCT
    intersection_id,
    elevation_feature_code_desc
FROM gis_core.intersection_latest)

SELECT
    staging.intersection_id,
    staging.intersection_desc,
    staging.distinct_feature_desc_list,
    staging.highest_order_feature,
    staging.all_feature_code_list,
    staging.classification_desc,
    elev.elevation_feature_code_desc,
    staging.road_names,
    staging.degree,
    staging.centreline_ids,
    staging.geom,
    staging.cent_geom
FROM staging
LEFT JOIN elev USING (intersection_id)
WHERE staging.count_name > 1 OR staging.degree > 2;

ALTER TABLE gis_core.intersection_classification
OWNER TO gis_admins;
COMMENT ON VIEW gis_core.intersection_classification
IS 'A view that provides information on intersection''s related road classes, road names,and connectivity degree.';

GRANT SELECT ON TABLE gis_core.intersection_classification TO bdit_humans;
GRANT ALL ON TABLE gis_core.intersection_classification TO gis_admins;

