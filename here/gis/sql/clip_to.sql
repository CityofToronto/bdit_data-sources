-- Function: here_gis.clip_to(text, text)

--DROP FUNCTION here_gis.clip_to(text, text);

CREATE OR REPLACE FUNCTION here_gis.clip_to(
    tablename text, revision text)
  RETURNS integer AS
$BODY$
DECLARE
    boundary_table TEXT := 'adminbndy3_'||revision;

BEGIN
DROP TABLE IF EXISTS bounded_table;
EXECUTE FORMAT('CREATE TEMP TABLE bounded_table AS SELECT a.*  
		FROM here_gis.%I AS a,
		(SELECT DISTINCT geom FROM here_gis.%I WHERE polygon_nm = ''TORONTO''
		)AS b 
		WHERE ST_Intersects(a.geom, b.geom) ', tablename, boundary_table);
EXECUTE FORMAT('TRUNCATE here_gis.%I',  tablename);
EXECUTE FORMAT('INSERT INTO here_gis.%I SELECT * FROM bounded_table',  tablename);
RETURN 1;
END;

$BODY$
  LANGUAGE plpgsql VOLATILE STRICT
  COST 100;
ALTER FUNCTION here_gis.clip_to(text, text)
  OWNER TO here_admins;
GRANT EXECUTE ON FUNCTION here_gis.clip_to(text, text) TO here_admins;
GRANT EXECUTE ON FUNCTION here_gis.clip_to(text, text) TO bdit_humans;
REVOKE ALL ON FUNCTION here_gis.clip_to(text, text) FROM public;
