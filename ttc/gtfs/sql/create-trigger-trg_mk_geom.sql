-- FUNCTION: gtfs.trg_mk_geom()

-- DROP FUNCTION IF EXISTS gtfs.trg_mk_geom();

CREATE OR REPLACE FUNCTION gtfs.trg_mk_geom()
RETURNS trigger
LANGUAGE plpgsql
COST 100
VOLATILE NOT LEAKPROOF SECURITY DEFINER
AS $BODY$
    BEGIN

    NEW.geom := ST_GeomFromText('POINT('||NEW.stop_lon||' '||NEW.stop_lat||')', 4326);

    RETURN NEW;

END;
$BODY$;

ALTER FUNCTION gtfs.trg_mk_geom()
OWNER TO gtfs_admins;

GRANT EXECUTE ON FUNCTION gtfs.trg_mk_geom() TO gtfs_admins;

GRANT EXECUTE ON FUNCTION gtfs.trg_mk_geom() TO gtfs_bot;

REVOKE ALL ON FUNCTION gtfs.trg_mk_geom() FROM public;
