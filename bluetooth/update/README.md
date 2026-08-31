# Updating Bluetooth Segments <!-- omit in toc -->

## Overview <!-- omit in toc -->

Ocasionally, new segments need to be added to the `bluetooth` schema. This document will go over the steps and the general process to add new routes along the newly installed bluetooth reader locations. It assumes that bluetooth.all_analyses table does not have rows already created for the proposed new routes. The steps listed here are followed to create an entirely new routes for newly installed bluetooth readers in the city.

## Table of Contents <!-- omit in toc -->

- [Data Updating](#data-updating)
- [Adding Readers](#adding-readers)
- [Preparatory Tables and Steps](#preparatory-tables-and-steps)
- [Finding Nearest Intersection IDs](#finding-nearest-intersection-ids)
- [Using pg\_routing](#using-pg_routing)
- [Things to note](#things-to-note)
- [Validating Output](#validating-output)

## Data Updating

To update segments, the vendor will provide an excel file containing the details of newly added bluetooth detectors, which includes proposed routes `Name`, `Description`, intersection name as `Name`, four digit bluetooth sensor identification number as `Sensor` and `latitude/longitude` as start point and the end points along with numerous other fields. 


## Adding Readers

New readers were added manually to the database. In addition to the lat/lon, street names at each intersection where the readers are located is also needed in two columns for each route as the `from_street` and `to_street`. The `Description` field contains this information. For each proposed route, the excel sheet is populated and assigned a unique `analysis_id`. 

Therefore the new reader table had the following fields populated: 

`analysis_id` 		- Unique ID for each route
`street` 			- Name of the street along the route
`direction` 		- Route direction(Eastbound, WestBound, NorthBound or SouthBound)
`from_street` 		- Name of the intersecting street where the route begins.
`to_street`			- Name of the intersecting street where the route ends 
`from_name`			- The reader name  at the start point of the route
`from_id` 			- The four digit Unique bluetooth id at the start point
`from_lat` 			- Latitude at the start point of the route
`from_lon` 			- Longitude at the start point of the route
`to_name` 			- The reader name  at the end point of the route
`to_id`				- The four digit Unique bluetooth id at the end point
`to_lat`			- Latitude at the end point of the route
`to_lon`			- Longitude at the end point of the route
`length`			- Length of the route in metres 

## Preparatory Tables and Steps

The following steps are utilized to create segments: 

1. Get start and end geom for each analysis_id using the provided lat and lon 
2. Create table with detector_id, detector_geom, centreline_int_id
3. Join the detector's geometry to the closest centreline intersection
4. Route the segments using centreline's intersection with the base network of `gis_core.routing_centreline_directional` using [pgr_dijkstra].


## Finding Nearest Intersection IDs

To get the intersection ids that are the closest to the newly added detectors location, create a table named `bluetooth_nodes`. This table has four fields:
`bluetooth_id`, `geom` (geometry of bluetooth detectors), `int_id` (nearest intersection id) and `int_geom` (geometry of the nearest intersection to the bluetooth detector)

This table can be created using the following query:
```SQL
CREATE TABLE your_schema.bluetooth_nodes AS(
SELECT DISTINCT your_schema.new_added_detectors.from_id::integer AS bluetooth_id,
    			your_schema.new_added_detectors.from_geom,
    			nodes.int_id,
    			st_transform(nodes.node_geom, 4326) AS int_geom
   	FROM 		your_schema.new_added_detectors
 	CROSS JOIN LATERAL (SELECT 		z.int_id,
            						st_transform(z.geom, 2952) AS node_geom
           				FROM 		gis_core.centreline_intersection_point_latest z
          				ORDER BY 	(z.geom <-> your_schema.new_added_detectors.from_geom)
         				LIMIT 1) nodes);
```
Check that correct intersections are returned from this query especially for oblique intersections with an offset. If required, correct the intersection_id and geom for such intersections and finalize the table `your_schema.bluetooth_nodes`. 
 

## Using pg_routing
Once the nearest centreline intersection nodes are linked to the bluetooth readers geom in `your_schema.bluetooth_nodes`, we are ready to run the following Query to create new routes by routing. 

```SQL
CREATE table your_schema.bt_segments_new AS (
WITH lookup AS (
	SELECT 		analysis_id, 
				from_id, 
				origin.int_id AS source, 
				to_id, 
				dest.int_id AS target
	FROM 		your_schema.new_added_detectors 
	INNER JOIN 	your_schema.bluetooth_nodes origin ON from_id = origin.bluetooth_id 
	INNER JOIN 	your_schema.bluetooth_nodes dest ON to_id = dest.bluetooth_id)

, results AS (
	SELECT * 
	FROM lookup
	CROSS JOIN LATERAL pgr_dijkstra('SELECT id, source, target, cost FROM gis_core.routing_centreline_directional inner join gis_core.centreline_latest on geo_id = id
where fcode != 207001', source::int, target::int, TRUE))

, lines as (
	SELECT 		analysis_id, 
				street, 
				direction, 
				from_street, 
				to_street, 
				edge AS geo_id, 
				geom 
	FROM 		results			 
	INNER JOIN 	gis_core.centreline_latest ON edge=geo_id
	INNER JOIN 	your_schema.new_added_detectors USING (analysis_id)
	ORDER BY 	analysis_id)

SELECT analysis_id, street, direction, from_street, to_street,
	CASE WHEN geom_dir != direction THEN ST_reverse(geom) 
	ELSE geom 
	END AS geom, 
	length
FROM ( 
SELECT analysis_id, street, direction, from_street, to_street, 
		gis.twochar_direction(gis.direction_from_line(ST_linemerge(ST_union(geom)))) AS geom_dir,
		ST_linemerge(ST_union(geom)) AS geom, 
		ST_length(ST_transform(ST_linemerge(ST_union(geom)), 2952)) AS length
FROM lines
GROUP BY analysis_id, street, direction, from_street, to_street) a)
```

## Things to note 
A number of centreline need to be excluded during routing, for example: Geostatistical lines and planning boundaries. Those can be filtered using the following where clause: 
```sql
WHERE fcode_desc IN ('Collector','Collector Ramp','Expressway','Expressway Ramp',
'Local','Major Arterial','Major Arterial Ramp','Minor Arterial',
'Minor Arterial Ramp','Pending')
``` 

## Validating Output
Validate the length of the segments with length `ST_length(geom)` and direction using `gis.direction_from_line(geom)` functions. If the detectors are located very close to the centerline intersections, it is not necessary to do the centreline cutting. If any bluetooth detectors are not located at the start or end point of a centreline, we will need to cut the centreline using `ST_linesubstring()` as explained in [here.](https://github.com/CityofToronto/bdit_data-sources/issues/234).  

Steps to cut centreline using `ST_linesubstring()`:

1. Find the closest point of the detector on the centreline with [ST_closespoint()](https://postgis.net/docs/ST_ClosestPoint.html).   
```sql
ST_closestpoint(detector_geom, centreline_geom)
```
2. Return the location of the point relative to the centreline using [`ST_linelocatepoint()`](https://postgis.net/docs/ST_LineLocatePoint.html)
```sql
ST_linelocatepoint(geom, closest_point_geom)
```
3. Cut the line using [`ST_linesubstring()`](https://postgis.net/docs/ST_LineSubstring.html) 
```sql
ST_linesubstring(geom, 0, st_linelocatepoint)
```

The new routes table is now ready to append to the existing routes table.  

[pgr_dijkstra]:https://docs.pgrouting.org/latest/en/pgr_dijkstra.html