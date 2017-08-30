CREATE OR REPLACE FUNCTION mto.agg_thirty_create_table(yyyymm TEXT, start_date TIMESTAMP)
RETURNS INTEGER
AS $$
DECLARE
	table_name TEXT := 'mto_agg_30_' || yyyymm;
	rule_name TEXT := 'mto_insert_' || yyyymm;
BEGIN
    EXECUTE format('CREATE TABLE IF NOT EXISTS mto.%I 
			(PRIMARY KEY (detector_id, count_bin),
			CHECK (count_bin >= $1 AND
			count_bin < $1 + ''1 mon''::interval)
			)
        INHERITS (mto.mto_agg_30);', table_name) USING start_date;
        
    EXECUTE format('CREATE OR REPLACE RULE %I
        AS ON INSERT TO mto.mto_agg_30
        WHERE new.count_bin >= $1
        AND new.count_bin < $1 + ''1 mon''::interval
        DO INSTEAD INSERT INTO %I (detector_id, count_bin, volume)
        VALUES (new.detector_id, new.count_bin, new.volume);'
        , rule_name, table_name) USING start_date;
    
    RETURN 1;
END;
$$
SECURITY DEFINER
LANGUAGE plpgsql;