UPDATE miovision.intersections
SET geom = ST_GeomFromText('POINT('||lng||' '||lat||')', 4326);

ALTER TABLE miovision.intersections 
ADD COLUMN int_id bigint; 

COMMENT ON COLUMN miovision.intersections.int_id IS 'City''s centreline intersection id';

UPDATE miovision.intersections i
SET int_id = cntr.int_id
FROM (

SELECT intersection_uid, xsecs.int_id
  FROM miovision.intersections mv
  CROSS JOIN LATERAL(SELECT int_id, elev_id, intersec5, classifi7
  FROM gis.centreline_intersection cntr
  ORDER BY ST_Distance(mv.geom, cntr.geom)
  LIMIT 1) xsecs)cntr
  WHERE i.intersection_uid = cntr.intersection_uid;




SELECT intersection_name, intersec5  FROM gis.centreline_intersection 
INNER JOIN miovision.intersections USING (int_id)