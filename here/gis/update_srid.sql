/*Author: Raphael Dumas
Creates a number of UPDATE queries to execute to update the SRID for the layers uploaded through the PostGIS Shapefile importer. Copy and paste the resulting rows into a new query window.*/

SELECT 'UPDATE TABLE '|| schemaname || '.' || tablename ||' SET geom = TO ST_SETSRID(geom, 4326);'
FROM pg_tables WHERE NOT schemaname IN ('pg_catalog', 'here_gis')
ORDER BY schemaname, tablename;