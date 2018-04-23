DROP VIEW IF EXISTS open_data.ksp_atr_2015;
CREATE VIEW open_data.ksp_atr_2015 AS(
SELECT	centreline_id, 
	lf_name,
--Concatenates FROM intersection to TO intersection and removes mentions of King St using regular expressions (the | indicates OR), 'g' flag specifies replace all
regexp_REPLACE(fromstreet.intersec5, '(King St W / )|(King St E / )|( / King St W)', '', 'g') ||' to '|| regexp_replace(tostreet.intersec5, '(King St W / )|(King St E / )|( / King St W)', '', 'g') from_to, 
	CASE dir_bin WHEN -1 THEN 'WB' ELSE 'EB' END as dir, 
	count_bin AS datetime_bin, 
	volume
FROM prj_volume.centreline_volumes_2015
INNER JOIN gis.street_centreline s ON centreline_id = geo_id
INNER JOIN rdumas.king_pilot_polygon k ON ST_within(s.geom, k.geom)
INNER JOIN gis.centreline_intersection tostreet ON tnode = tostreet.int_id
INNER JOIN gis.centreline_intersection fromstreet ON fnode = fromstreet.int_id
WHERE lf_name LIKE 'King St %' AND count_type = 1
);
COMMENT ON VIEW open_data.ksp_atr_2015 IS 'ATR counts for volumes on King in 2015';