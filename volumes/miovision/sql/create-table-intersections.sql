DROP TABLE IF EXISTS miovision.intersections;

CREATE TABLE miovision.intersections (
	intersection_uid serial,
	intersection_name text,
	street_main text,
	street_cross text,
	lat numeric,
	lng numeric
	);