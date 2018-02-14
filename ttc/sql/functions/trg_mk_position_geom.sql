CREATE OR REPLACE FUNCTION ttc.trg_mk_position_geom()
    RETURNS trigger AS
    $func$
    BEGIN

    NEW.position := ST_GeomFromText('POINT('||NEW.longitude||' '||NEW.latitude||')', 4326);

    RETURN NEW;

END;
$func$ LANGUAGE plpgsql SECURITY DEFINER;