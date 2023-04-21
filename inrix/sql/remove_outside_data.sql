CREATE OR REPLACE FUNCTION inrix.removeoutsidedata(yyyymm text)
RETURNS integer AS
$BODY$
DECLARE
    rawtable TEXT := 'raw_data'||yyyymm;

BEGIN
    EXECUTE format('CREATE TEMP TABLE %I (LIKE inrix.raw_data)', rawtable);
    EXECUTE format('INSERT INTO %I  '
                   'SELECT * ' 
                   'FROM inrix.%I '
                   'WHERE tmc IN (SELECT tmc FROM gis.inrix_tmc_tor) ', rawtable, rawtable);
    EXECUTE format('DROP TABLE inrix.%I CASCADE ', rawtable);
    EXECUTE format('CREATE TABLE inrix.%I () INHERITS (inrix.raw_data)', rawtable);
    EXECUTE format('ALTER TABLE inrix.%I OWNER to rdumas_py', rawtable);
    EXECUTE format('INSERT INTO inrix.%I ' 
		   'SELECT * '
                   'FROM %I ', rawtable, rawtable);
    
    RETURN 1;
END;
$BODY$
LANGUAGE plpgsql VOLATILE
COST 100;
ALTER FUNCTION inrix.removeoutsidedata(text)
OWNER TO rdumas;
GRANT EXECUTE ON FUNCTION inrix.removeoutsidedata (text) TO rdumas;
GRANT EXECUTE ON FUNCTION inrix.removeoutsidedata (text) TO rdumas_py;
