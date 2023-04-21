CREATE OR REPLACE FUNCTION inrix.agg_extract_hour(yyyymm text)
RETURNS integer
AS $$
DECLARE
    rawtable TEXT := 'raw_data'||yyyymm;
    aggtable TEXT := 'agg_extract_hour'||yyyymm;
BEGIN
    EXECUTE format('CREATE TABLE IF NOT EXISTS inrix.%I ()INHERITS(inrix.agg_extract_hour);', aggtable);
    EXECUTE format('INSERT INTO inrix.%I '
                   'SELECT '
                   'tmc, '
                   'extract(hour from tx)*10 + trunc(extract(minute from tx)/15)+1 AS time_15_continuous, '
                   'tx::DATE as dt, '
                   'COUNT(speed) AS cnt, '
                   'AVG(speed) AS avg_speed '
                   'FROM inrix.%I '
                   'WHERE score = 30 '
                   'GROUP BY tmc, tx::date, time_15_continuous', aggtable, rawtable);
    RETURN 1;
END;
$$
LANGUAGE plpgsql;
GRANT EXECUTE ON FUNCTION inrix.agg_extract_hour (text) TO rdumas_py;

CREATE OR REPLACE FUNCTION inrix.agg_extract_hour_alldata(yyyymm text)
RETURNS integer
AS $$
DECLARE
    rawtable TEXT := 'raw_data'||yyyymm;
    aggtable TEXT := 'agg_extract_hour_alldata'||yyyymm;
BEGIN
    EXECUTE format('CREATE TABLE IF NOT EXISTS inrix.%I ()INHERITS(inrix.agg_extract_hour_alldata);', aggtable);
    EXECUTE format('INSERT INTO inrix.%I '
                   'SELECT '
                   'tmc, '
                   'extract(hour from tx)*10 + trunc(extract(minute from tx)/15)+1 AS time_15_continuous, '
                   'tx::DATE as dt, '
                   'COUNT(speed) AS cnt, '
                   'AVG(speed) AS avg_speed '
                   'FROM inrix.%I '
                   'GROUP BY tmc, tx::date, time_15_continuous;', aggtable, rawtable);
    RETURN 1;
END;
$$
LANGUAGE plpgsql;
GRANT EXECUTE ON FUNCTION inrix.agg_extract_hour_alldata (text) TO rdumas_py;

CREATE OR REPLACE FUNCTION inrix.agg_extract_hour_alldata(
    yyyymm text, tmcschema text, tmctable text
)
RETURNS integer
AS $$
DECLARE
    rawtable TEXT := 'raw_data'||yyyymm;
    aggtable TEXT := 'agg_extract_hour_alldata'||yyyymm;
    _existstest INT;
BEGIN

    EXECUTE format('SELECT 1 FROM pg_namespace WHERE nspname = %L;', tmcschema)
    INTO _existstest;
    
    IF _existstest IS NULL THEN
        RAISE EXCEPTION 'schema "%" doesn''t exist', tmcschema;
        RETURN 0;
    END IF;
    
    EXECUTE format('SELECT 1 FROM information_schema.tables WHERE table_schema = %L AND table_name = %L;', tmcschema, tmctable)
    INTO _existstest;
    
    IF _existstest IS NULL THEN
        RAISE EXCEPTION 'tmc table "%" doesn''t exist', tmctable;
        RETURN 0;
    END IF;
    
    EXECUTE format('CREATE TABLE IF NOT EXISTS inrix.%I ()INHERITS(inrix.agg_extract_hour_alldata);', aggtable);
    EXECUTE format('INSERT INTO inrix.%I '
                   'SELECT '
                   'tmc, '
                   'extract(hour from tx)*10 + trunc(extract(minute from tx)/15)+1 AS time_15_continuous, '
                   'tx::DATE as dt, '
                   'COUNT(rawdata.speed) AS cnt, '
                   'AVG(rawdata.speed) AS avg_speed '
                   'FROM inrix.%I AS rawdata '
                   'INNER JOIN %I.%I USING (tmc) '
                   'GROUP BY tmc, tx::date, time_15_continuous;', aggtable, rawtable, tmcschema, tmctable);
    RETURN 1;
        
        
END;
$$
LANGUAGE plpgsql;
GRANT EXECUTE ON FUNCTION inrix.agg_extract_hour_alldata (text, text, text) TO rdumas_py;

