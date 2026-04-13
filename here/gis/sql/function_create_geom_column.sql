CREATE OR REPLACE FUNCTION here_gis.update_geom_column(
    tablename text
)
RETURNS integer
LANGUAGE plpgsql
COST 100
VOLATILE STRICT PARALLEL UNSAFE
AS $BODY$
DECLARE
	wkbgeom_exists BOOLEAN;
BEGIN

SELECT EXISTS (
 	SELECT column_name
	FROM information_schema.columns
	WHERE table_name = tablename AND column_name = 'wkb_geometry'
    ) INTO wkbgeom_exists;

IF wkbgeom_exists THEN
	EXECUTE FORMAT('ALTER TABLE here_gis.%I RENAME COLUMN wkb_geometry TO geom;', tablename);
	ELSE
	RAISE NOTICE 'No wkb_geometry column.';
END IF;


-- see if geom is point or line
EXECUTE FORMAT(
    'SELECT ST_GeometryType(geom) FROM here_gis.%I WHERE geom IS NOT NULL LIMIT 1',
    tablename
) INTO geom_type;

-- set geometry for point
IF geom_type = 'ST_MultiPoint' THEN
    EXECUTE FORMAT(
        'ALTER TABLE here_gis.%I
         ALTER COLUMN geom
         SET DATA TYPE geometry(Point,4326)
         USING ST_GeometryN(geom,1);',
        tablename
    );
-- set geometry for line
ELSIF geom_type = 'ST_MultiLineString' THEN
    EXECUTE FORMAT(
        'ALTER TABLE here_gis.%I
         ALTER COLUMN geom
         SET DATA TYPE geometry(LineString,4326)
         USING ST_GeometryN(geom,1);',
        tablename
    );
ELSE
        RAISE NOTICE 'Geometry type % not handled.', geom_type;
    END IF;
RETURN 1;
END;
$BODY$;

ALTER FUNCTION here_gis.update_geom_column(text)
OWNER TO here_admins;


GRANT EXECUTE ON FUNCTION here_gis.update_geom_column(text) TO here_admins;

REVOKE ALL ON FUNCTION here_gis.update_geom_column(text) FROM public;