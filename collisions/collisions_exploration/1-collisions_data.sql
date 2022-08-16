---------------------------------------------------------------------------------------------------------
--Get data for exploration
---------------------------------------------------------------------------------------------------------
/*1. **`Collisions_replicator.events`**
    Only data from 2010 to June 2022 queried on `July 6th, 2022` were used. The collision reports can be enetred to the system at anytime up to a month or a year after an incident happened; subsequently, `Collisions_replicator.events` table can change day by day. Therfore, I stored a copy of the queried data on July 6th, 2022 into my personal schema (`rbahreh.col_original`) as well as into a csv file for future reference.
*/

CREATE Table rbahreh.collisions2010_original As 
    (SELECT * 
    FROM collisions_replicator.events 
    WHERE accdate>='2010-01-01 00:00:00' 
    and accdate<'2022-07-01 00:00:00');

---------------------------------------------------------------------------------------------------------------------------------------------------
--CHANGEING THE NAME OF ORIGINAL DATA (CITY BOUNDARY)
---------------------------------------------------------------------------------------------------------------------------------------------------
CREATE TABLE rbahreh.col_city_boundary AS (SELECT * FROM rbahreh.citygcs_regional_mun_wgs84);-- Boundary of City of Toronto downloaded from opendata
---------------------------------------------------------------------------------------------------------------------------------------------------


---------------------------------------------------------------------------------------------------------------------------------------------------
--MAKE A COPY OF COLLISION DATA 
---------------------------------------------------------------------------------------------------------------------------------------------------
--DROP TABLE rbahreh.col_orig;
CREATE TABLE rbahreh.col_orig AS (SELECT * FROM rbahreh.collisions2010_original);--619162
---------------------------------------------------------------------------------------------------------------------------------------------------


---------------------------------------------------------------------------------------------------------------------------------------------------
--CHECK THE SRID OF DATA
---------------------------------------------------------------------------------------------------------------------------------------------------
SELECT FIND_SRID('rbahreh', 'col_original', 'geom'); --4326
SELECT FIND_SRID('rbahreh', 'col_city_boundary', 'geom');--4326
---------------------------------------------------------------------------------------------------------------------------------------------------