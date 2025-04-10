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
    JOIN gis_core.centreline_intersection_point_latest AS intersections
        ON cent.from_intersection_id = intersections.intersection_id
		WHERE classification_desc != 'Pseudo Intersection-Overpass/Underpass'
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
    JOIN gis_core.centreline_intersection_point_latest AS intersections
        ON cent.to_intersection_id = intersections.intersection_id
		WHERE classification_desc != 'Pseudo Intersection-Overpass/Underpass'
),

centreline_agg AS (
    SELECT
        intersection_id,
        array_agg(DISTINCT centreline_id) AS centreline_list
    FROM all_intersections
    GROUP BY intersection_id
)

, staging AS (
	SELECT
	    dist_feature.intersection_id,
		dist_feature.intersection_desc,
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
			all_intersections.intersection_desc,
	        all_intersections.feature_code_desc,
	        all_intersections.number_of_elevations,
	        all_intersections.geom,
	        all_intersections.feature_code
	    FROM all_intersections
	) AS dist_feature
	LEFT JOIN centreline_agg USING (intersection_id)
	GROUP BY
	    dist_feature.intersection_id,
		dist_feature.intersection_desc,
	    dist_feature.number_of_elevations,
	    dist_feature.geom,
	    centreline_agg.centreline_list)

, non_road_info AS (
-- this is to find distinct pairs of feature classes 
	select  DISTINCT intersection_id, 
	    	LEAST(feature_class_from, feature_class_to) AS feature_class_1,
	    	GREATEST(feature_class_from, feature_class_to) AS feature_class_2,
			case when 
				feature_class_from not in ('ROADWAY', 'RAMP')
				OR feature_class_to not in ('ROADWAY', 'RAMP') THEN 1 else 0
			end as ex
	from gis_core.intersection_latest
	)

-- select only the intersection_id, where there is only 1 pair of feature class matches
-- and one of them is NOT a roadway or ramp, 
-- aka geostatistical line, river, walkway and what not
, to_exclude AS (
SELECT intersection_id, count(1), max(ex) as ex
from non_road_info
GROUP BY intersection_id)

select staging.* 
from staging
left join (SELECT * FROM to_exclude WHERE count = 1 AND ex = 1) dumps USING (intersection_id)
WHERE dumps.intersection_id IS NULL


COMMENT ON VIEW gis_core.intersection_classification IS
'This view provides a summary of intersections, including road class involved, highest degree of 
road class, number of elevations, degree (number of connected centreline segments).';
