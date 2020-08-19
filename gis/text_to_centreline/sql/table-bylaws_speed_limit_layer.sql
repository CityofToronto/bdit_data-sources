CREATE TABLE gis.bylaws_speed_limit_layer AS
 SELECT bylaw.bylaw_id,
    bylaw.lf_name,
    bylaw.geo_id,
    bylaw.speed_limit,
    bylaw.int1,
    bylaw.int2,
    bylaw.con,
    bylaw.note,
        CASE WHEN bylaw.section IS NOT NULL 
        THEN st_linesubstring(cl.geom, lower(bylaw.section)::double precision, upper(bylaw.section)::double precision)
        ELSE cl.geom
        END AS geom,
    bylaw.section,
    bylaw.oid1_geom,
    bylaw.oid1_geom_translated,
    bylaw.oid2_geom,
    bylaw.oid2_geom_translated,
    bylaw.date_added,
    bylaw.date_repealed
   FROM gis.bylaws_centreline_categorized bylaw
     JOIN gis.centreline cl USING (geo_id, lf_name)
WITH DATA;

ALTER TABLE gis.bylaws_speed_limit_layer
    OWNER TO jchew;

GRANT SELECT, REFERENCES, TRIGGER ON TABLE gis.bylaws_speed_limit_layer TO bdit_humans;
GRANT ALL ON TABLE gis.bylaws_speed_limit_layer TO jchew;