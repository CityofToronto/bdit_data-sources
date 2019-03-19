/*
Create a set of data compatible with pg_routing.
The cost will come from traffic analytics data which contains both speed and link_dir length.
*/

CREATE MATERIALIZED VIEW here.routing_streets AS 

--Links in the FROM direction of travel
SELECT link_id ||'F' AS link_dir, ref_in_id as "source", nref_in_id AS target, geom
FROM here_gis.traffic_streets_17_4
INNER JOIN here_gis.streets_17_4 USING(link_id)
WHERE dir_travel IN ('F', 'B')
UNION ALL
--Links in the TO direction of travel, need to duplicate because HERE links are unique to both directions of travel (`dir_travel`)
SELECT link_id ||'T' AS link_dir, nref_in_id as "source", ref_in_id AS target, geom
FROM here_gis.traffic_streets_17_4
INNER JOIN here_gis.streets_17_4 USING(link_id)
WHERE dir_travel IN ('T', 'B') 
