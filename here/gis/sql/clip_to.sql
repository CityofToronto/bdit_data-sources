-- FUNCTION: here_gis.clip_to(text, text)

-- DROP FUNCTION IF EXISTS here_gis.clip_to(text, text);


CREATE OR REPLACE FUNCTION here_gis.clip_to(
	tablename text,
	revision text)
    RETURNS integer
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE STRICT PARALLEL UNSAFE
AS $BODY$
DECLARE
    boundary_table TEXT := 'adminbndy3_'||revision;
	geom_exists BOOLEAN;

BEGIN
DROP TABLE IF EXISTS bounded_table;

SELECT EXISTS (
 	SELECT column_name
	FROM information_schema.columns
	WHERE table_name = boundary_table AND column_name = 'geom'
    ) INTO geom_exists;

IF geom_exists THEN
	EXECUTE FORMAT('CREATE TEMP TABLE bounded_table AS 
				SELECT a.*  
				FROM here_gis.%I AS a,
				(SELECT DISTINCT geom FROM here_gis.%I WHERE polygon_nm = ''TORONTO''
				)AS b 
				WHERE ST_Intersects(ST_Transform(a.geom, 4326), ST_Transform(b.geom, 4326)) ', tablename, boundary_table);
	ELSE
	EXECUTE FORMAT('CREATE TEMP TABLE bounded_table AS 
				SELECT a.*  
				FROM here_gis.%I AS a,
				(SELECT DISTINCT wkb_geometry FROM here_gis.%I WHERE polygon_nm = ''TORONTO''
				)AS b 
				WHERE ST_Intersects(ST_Transform(a.wkb_geometry, 4326), ST_Transform(b.wkb_geometry, 4326)) ', tablename, boundary_table);
END IF;
EXECUTE FORMAT('TRUNCATE here_gis.%I',  tablename);
EXECUTE FORMAT('INSERT INTO here_gis.%I SELECT * FROM bounded_table',  tablename);
RETURN 1;
END;

$BODY$;

ALTER FUNCTION here_gis.clip_to(text, text)
    OWNER TO here_admins;

GRANT EXECUTE ON FUNCTION here_gis.clip_to(text, text) TO bdit_humans;

GRANT EXECUTE ON FUNCTION here_gis.clip_to(text, text) TO here_admins;

REVOKE ALL ON FUNCTION here_gis.clip_to(text, text) FROM PUBLIC;