CREATE OR REPLACE FUNCTION mto.agg_thirty_create_table(yyyymm TEXT, start_date TEXT)
RETURNS INTEGER
AS $$
DECLARE
	table_name TEXT := 'mto_agg_30_' || yyyymm;
	rule_name TEXT := 'mto_insert_' || yyyymm;
BEGIN
    EXECUTE format('CREATE TABLE IF NOT EXISTS mto.%I 
			(PRIMARY KEY (detector_id, count_bin),
			CHECK (count_bin >= %L::TIMESTAMP AND
			count_bin < %L::TIMESTAMP + ''1 mon''::interval)
			)
        INHERITS (mto.mto_agg_30);
        GRANT SELECT, INSERT, DELETE, TRUNCATE ON mto.%I TO mto_bot;',
         table_name, start_date, start_date, table_name);
        
    EXECUTE format('CREATE OR REPLACE RULE %I
        AS ON INSERT TO mto.mto_agg_30
        WHERE new.count_bin >= %L::TIMESTAMP
        AND new.count_bin < %L::TIMESTAMP + ''1 mon''::interval
        DO INSTEAD INSERT INTO mto.%I (detector_id, count_bin, volume)
        VALUES (new.detector_id, new.count_bin, new.volume);'
        , rule_name, start_date, start_date, table_name) ;
    
    RETURN 1;
END;
$$
SECURITY DEFINER
LANGUAGE plpgsql;
GRANT EXECUTE ON FUNCTION mto.agg_thirty_create_table(text, text) TO mto_bot;
