/* 
Adding a geometry column to the table wys.locations based on the loc column 
*/

ALTER TABLE wys.locations
ADD COLUMN geom geometry;

UPDATE wys.locations
SET geom = st_setsrid(st_makepoint(split_part(regexp_replace(loc, '[()]'::text, ''::text, 'g'::text), ','::text, 2)::double precision, 
                                   split_part(regexp_replace(loc, '[()]'::text, ''::text, 'g'::text), ','::text, 1)::double precision), 
                        4326);