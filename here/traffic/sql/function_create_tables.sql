-- FUNCTION: here.create_tables(text)

DROP FUNCTION  IF EXISTS here.create_tables(text);

CREATE OR REPLACE FUNCTION here.create_tables(
	_yyyy text)
    RETURNS VOID
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE STRICT 
AS $BODY$


DECLARE 
	_startdate DATE;
	_yyyymm TEXT;
	_basetablename TEXT := 'ta_';
	_tablename TEXT;

BEGIN
    FOR _mm IN 01..12 LOOP
        _startdate:= to_date(_yyyy||'-'||_mm||'-01', 'YYYY-MM-DD');
        IF _mm < 10 THEN
            _yyyymm:= _yyyy||'0'||_mm;
        ELSE
            _yyyymm:= _yyyy||''||_mm;
        END IF;
        _tablename:= _basetablename||_yyyymm;
        EXECUTE format($$CREATE TABLE here.%I 
            (CHECK (tx >= DATE %L AND tx < DATE %L + INTERVAL '1 month'),
            UNIQUE(link_dir, tx)
            ) INHERITS (here.ta);
            ALTER TABLE here.%I OWNER TO here_admins;
            $$
            , _tablename, _startdate, _startdate, _tablename);
        PERFORM here.create_link_dir_idx(_tablename);
        PERFORM here.create_tx_idx(_tablename);
        EXECUTE format($$ANALYZE here.%I $$,
            _tablename);

    END LOOP;
END;
$BODY$;

GRANT EXECUTE ON FUNCTION here.create_tables(text) TO here_admins;
GRANT EXECUTE ON FUNCTION here.create_tables (TEXT) TO here_admin_bot;

COMMENT ON FUNCTION here.create_tables(text) IS 'Loops through the months of the provided year to create a partitioned table for each month';

