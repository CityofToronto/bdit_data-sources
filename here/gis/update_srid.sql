/*Author: Raphael Dumas
Creates a number of UPDATE queries to execute to update the SRID for the layers uploaded through the PostGIS Shapefile importer. Copy and paste the resulting rows into a new query window.*/

--Shapefile import defaults to public schema.
SELECT 'ALTER TABLE '|| schemaname || '.' || tablename ||' SET SCHEMA here_gis;'
FROM pg_tables WHERE schemaname IN ('public')
ORDER BY schemaname, tablename;

--Update SRID since importer doesn't autodetect SRID
SELECT 'UPDATE '|| schemaname || '.' || tablename ||' SET geom = ST_SETSRID(geom, 4326);'
FROM pg_tables WHERE schemaname IN ('here_gis')
ORDER BY schemaname, tablename;

--Specifying SRID in column definition
SELECT 'ALTER TABLE '|| schemaname || '.' || tablename ||' ALTER geom TYPE geometry('||type||' , 4326);'
FROM pg_tables 
INNER JOIN geometry_columns ON f_table_schema = schemaname AND tablename = f_table_name
WHERE schemaname IN ('here_gis')
ORDER BY schemaname, tablename;
