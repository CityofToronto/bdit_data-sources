# Updating Bluetooth Segments

## Overview

Ocasionally, new segments need to be added to the `bluetooth` schema. This document will go over the steps and the general process to add new routes along the newly installed bluetooth reader locations. It assumes that `bluetooth.all_analyses` table does not have rows already created for the proposed new routes. The steps listed here are followed to create an entirely new routes for newly installed bluetooth readers in the city.

## Table of Contents

- [Updating Bluetooth Segments](#updating-bluetooth-segments)
	- [Overview](#overview)
	- [Table of Contents](#table-of-contents)
	- [Data Updating](#data-updating)
	- [Adding Readers](#adding-readers)
	- [Preparatory Tables and Steps](#preparatory-tables-and-steps)
	- [Finding Nearest Intersection IDs](#finding-nearest-intersection-ids)
	- [Using pg_routing](#using-pg_routing)
	- [Things to note](#things-to-note)
	- [Validating Output](#validating-output)

## Data Updating

In this update, there exists an Excel Sheet template that contains details of newly added bluetooth detectors. The details include proposed route name, description, intersection name (BDIT convention), sensor id and lat/lon at start point and sensor id and lat/lon at the end points along with numerous other fields. The template screenshot. 

![new_readers_template](img/template.PNG)

This template was used to include all the details of the routes that can be useful for future analysis. The routes that were updated by adding this batch of new detectors were named with a prefix "DT3_". 

## Adding Readers

Depending on how many new readers there are, it may be worthwile to develop an automated process in PostgreSQL, but this update was done manually in the Excel template. In addition to the lat/long, the streets where the readers are is also needed in 2 columns, and the segment name. The segment name is always the first two letters of each street, with the corridor street first and the intersecting street second. For example, a reader at Bloor/Christie measuring travel times on Bloor would be named `BL_CH`. For each proposed route, the excel sheet is  populated with the `start reader`, `end_reader` and assigned a unique `analysis_id`. For this batch of new readers, analysis ids starting from 1600000 were added. 

Therefore the reader table will have the following fields populated: 

`analysis_id`, `street`, `direction`, `from_street`, `to_street`, `from_id`, `to_id`, `start_point_lat`, `start_point_lon`, `end_point_lat` and `end_point_lon`.



## Preparatory Tables and Steps
The following steps are utilized to create segments
1. Get start and end geom for each analysis_id
2. Create table with detector_id, detector_geom, centreline_int_id
3. Join the detector's geometry to the closest centreline intersection
4. Route the segments using street centreline's intersection `gis.centreline_both_dir` using [pgr_dijkstra].


After uploading excel data to PostgreSQL, the geometry column is needed. This can be done by adding a column using `ALTER TABLE schema.table_name ADD COLUMN geom GEOMETRY` we need two geometry columns: `from_geom` and `to_geom`.  Filling in the from_geom column with `UPDATE TABLE schema.table_name SET from_geom = ST_MakePoint(start_point_lat, start_point_lon)` and modify the query for `to_geom` = `ST_MakePoint (end_point_lat, end_point_lon)`.

## Finding Nearest Intersection IDs

To get the intersection ids that are close to the newly added detectors location, create a table named `bluetooth_nodes`. This table has four fields:
`bluetooth_id`, `geom` (this is geometry of bluetooth detectors), `int_id` (nearest intersection id) and `int_geom` (geometry of the nearest intersection to the )

This table is created using the following query:
```SQL
SELECT DISTINCT mohan.new_added_detectors.from_id::integer AS bluetooth_id,
    mohan.new_added_detectors.from_geom,
    nodes.int_id,
    st_transform(nodes.node_geom, 4326) AS int_geom
   FROM mohan.new_added_detectors
     CROSS JOIN LATERAL ( SELECT z.int_id,
            st_transform(z.geom, 98012) AS node_geom
           FROM gis.centreline_intersection z
          ORDER BY (z.geom <-> mohan.new_added_detectors.from_geom)
         LIMIT 1) nodes;
```
Check that correct intersections are returned from this query especially for odd shaped intersections. If required, correct the int_id and geom for such intersections and finalize the table `mohan.bluetooth_nodes`. 

## Using pg_routing
Once the nearest centreline intersection nodes are linked to the bluetooth readers geom `mohan.bluetooth_nodes`, we are ready to run the following Query to create new routes. 

```SQL
CREATE table mohan.bt_segments_new AS (
WITH lookup AS (
SELECT analysis_id, from_id, origin.int_id AS source, to_id, dest.int_id AS target
FROM mohan.new_added_detectors 
INNER JOIN mohan.bluetooth_nodes origin ON from_id = origin.bluetooth_id 
INNER JOIN mohan.bluetooth_nodes dest ON to_id = dest.bluetooth_id
),
results AS (
	SELECT * 
	FROM lookup
			 CROSS JOIN LATERAL pgr_dijkstra('SELECT id, source, target, cost FROM gis.centreline_routing_directional inner join gis.centreline on geo_id = id
where fcode != 207001', source::int, target::int, TRUE)		 
), 
lines as (
	SELECT analysis_id, street, direction, from_street, to_street, edge AS geo_id, geom 
	FROM results			 
INNER JOIN gis.centreline ON edge=geo_id
INNER JOIN mohan.new_added_detectors USING (analysis_id)
ORDER BY analysis_id
)
SELECT analysis_id, street, direction, from_street, to_street,
	CASE WHEN geom_dir != direction THEN ST_reverse(geom) 
	ELSE geom 
	END AS 
	geom
FROM ( 
SELECT analysis_id, street, direction, from_street, to_street, 
		gis.twochar_direction(gis.direction_from_line(ST_linemerge(ST_union(geom)))) AS geom_dir,
		ST_linemerge(ST_union(geom)) AS geom
FROM lines
GROUP BY analysis_id, street, direction, from_street, to_street) a)
```

![bt_new_segments](img/new_segments.JPG)

## Things to note 
Geostatistical lines and planning boundaries need to be avoided while pgrouting. 

## Validating Output
Validate the length of the segments with length ST_length(geom) and direction using gis.direction_from_line(geom) functions.If the detectors are located very close to the centerline intersections, it is not necessary to do the  centreline cutting. Else that step is necessary. 

The table is now ready to append to the existing routes table. 


[pgr_dijkstra]:https://docs.pgrouting.org/latest/en/pgr_dijkstra.html