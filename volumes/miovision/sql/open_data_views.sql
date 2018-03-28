-- View: open_data.miovision_2017

DROP VIEW open_data.miovision_2017;

CREATE OR REPLACE VIEW open_data.miovision_2017 AS 
 SELECT int_id,
    intersec5 AS intersection_name,
    volumes_15min.datetime_bin,
    CASE WHEN classification ='Buses' THEN 'Buses and streetcars'
    ELSE classification END AS classification,
    volumes_15min.leg,
    volumes_15min.dir,
    SUM(volumes_15min.volume)
   FROM miovision.volumes_15min
   INNER JOIN miovision.intersections USING (intersection_uid)
   INNER JOIN gis.centreline_intersection USING (int_id)
   INNER JOIN miovision.classifications USING(classification_uid)
      
  WHERE date_part('year'::text, volumes_15min.datetime_bin) = 2017::double precision
GROUP BY int_id, intersec5, datetime_bin, classification, leg, dir;
ALTER TABLE open_data.miovision_2017
  OWNER TO rdumas;
GRANT ALL ON TABLE open_data.miovision_2017 TO rdumas;
GRANT SELECT ON TABLE open_data.miovision_2017 TO od_extract_svc;

DROP VIEW open_data.miovision_2018;

CREATE OR REPLACE VIEW open_data.miovision_2018 AS 
 SELECT int_id,
    intersec5 AS intersection_name,
    volumes_15min.datetime_bin,
    CASE WHEN classification ='Buses' THEN 'Buses and streetcars'
    ELSE classification END AS classification,
    volumes_15min.leg,
    volumes_15min.dir,
    SUM(volumes_15min.volume)
   FROM miovision.volumes_15min
   INNER JOIN miovision.intersections USING (intersection_uid)
   INNER JOIN gis.centreline_intersection USING (int_id)
   INNER JOIN miovision.classifications USING(classification_uid)
      
  WHERE date_part('year'::text, volumes_15min.datetime_bin) = 2017::double precision
GROUP BY int_id, intersec5, datetime_bin, classification, leg, dir;
ALTER TABLE open_data.miovision_2018
  OWNER TO rdumas;
GRANT ALL ON TABLE open_data.miovision_2018 TO rdumas;
GRANT SELECT ON TABLE open_data.miovision_2018 TO od_extract_svc;
