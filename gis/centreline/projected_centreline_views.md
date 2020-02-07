# Projected Centreline Views

We realized that there was an issue when querying the `centreline` and `centreline_intersection` tables 
in the `gis`schema. The geometry for these tables was not projected (i.e. lat/long) and the index on the table 
was solely for the unprojected geometry. An issue arised we wanted to do a distance calculation between the centreline tables and other 
points. The issue was that an accurate distance calculation requires that the geometry is projected. It would take forever to complete 
these distance calculations because there was no index on the projected geometry of the `centreline`/`centreline_intersection` tables.

When we tried to duplicate these tables in our personal schemas, and add an 
index on the transformed geometry,like what is done [here](http://postgis.net/docs/ST_Transform.html) the index would
not be accessed when running our queries. So our other option was to create a verison of the `centreline`/`centreline_intersection`
table that contains the projected geometry, and then add indexes to those tables. The code that we used to do so is: 

```
DROP MATERIALIZED VIEW IF EXISTS gis.centreline_intersection_prj;
CREATE MATERIALIZED VIEW gis.centreline_intersection_prj AS (
SELECT gid, int_id, elev_id, intersec5, classifi6, classifi7, num_elev, elevatio9, elevatio10, 
	elev_level, elevation, elevatio13, height_r14, height_r15, x, y, longitude, latitude, objectid,
	
ST_Transform(geom, 98012) AS geom
FROM gis.centreline_intersection
);	
	
CREATE INDEX idx_geom_98012_centreline_prj 
ON gis.centreline_intersection_prj
USING GIST (geom) ;
	
DROP MATERIALIZED VIEW IF EXISTS gis.centreline_prj;
CREATE MATERIALIZED VIEW gis.centreline_prj AS (
SELECT gid, geo_id, lfn_id, lf_name, address_l, address_r, oe_flag_l, oe_flag_r, lonuml, hinuml, 
lonumr, hinumr, fnode, tnode, fcode, fcode_desc, juris_code, objectid, ST_Transform(geom, 98012) AS geom
FROM gis.centreline
);

CREATE INDEX idx_geom_98012_centreline_line_prj 
ON gis.centreline_prj
USING GIST (geom) ;
```
