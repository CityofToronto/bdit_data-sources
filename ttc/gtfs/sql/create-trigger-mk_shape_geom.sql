-- FUNCTION: gtfs.mk_shape_geom()

-- DROP FUNCTION IF EXISTS gtfs.mk_shape_geom();

CREATE OR REPLACE FUNCTION gtfs.mk_shape_geom()
RETURNS trigger
LANGUAGE plpgsql
COST 100
VOLATILE NOT LEAKPROOF SECURITY DEFINER
AS $BODY$

    BEGIN

    INSERT INTO gtfs.shapes_geom (shape_id, feed_id, geom)
    SELECT
        shape_id,
        feed_id,
        ST_MakeLine(lat.pt ORDER BY shape_pt_sequence) AS geom
    FROM new_rows,
    LATERAL (
        SELECT ST_SetSrid(ST_MakePoint(shape_pt_lon::numeric, shape_pt_lat::numeric), 4326) AS pt
    ) AS lat
    GROUP BY
        shape_id,
        feed_id;
    RETURN NULL;
END;
$BODY$;

ALTER FUNCTION gtfs.mk_shape_geom()
OWNER TO gtfs_admins;

GRANT EXECUTE ON FUNCTION gtfs.mk_shape_geom() TO gtfs_admins;

GRANT EXECUTE ON FUNCTION gtfs.mk_shape_geom() TO gtfs_bot;

REVOKE ALL ON FUNCTION gtfs.mk_shape_geom() FROM public;
