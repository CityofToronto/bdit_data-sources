CREATE OR REPLACE FUNCTION inrix.movedata(yyyymm text)
RETURNS integer AS
$BODY$
DECLARE
    rawtable TEXT := 'raw_data'||yyyymm;

BEGIN
    EXECUTE format('CREATE TABLE IF NOT EXISTS inrix_beyond_to.%I () INHERITS (inrix_beyond_to.raw_data);', rawtable);
    EXECUTE format('INSERT INTO inrix_beyond_to.%I (tx, tmc, speed, score) '
                   'SELECT tx, tmc, speed, score ' 
                   'FROM inrix.%I '
                   'WHERE tmc NOT IN (SELECT tmc FROM gis.inrix_tmc_tor) ', rawtable, rawtable);
    RETURN 1;
END;
$BODY$
LANGUAGE plpgsql VOLATILE
COST 100;
ALTER FUNCTION inrix.movedata(text)
OWNER TO rdumas;
GRANT EXECUTE ON FUNCTION inrix.movedata (text) TO public;
GRANT EXECUTE ON FUNCTION inrix.movedata (text) TO rdumas;
GRANT EXECUTE ON FUNCTION inrix.movedata (text) TO rdumas_py;
