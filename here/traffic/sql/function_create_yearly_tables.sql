-- For yearly maintenance
-- Create 12 tables for each month in a year 
CREATE OR REPLACE FUNCTION here.create_yearly_tables(yyyy text)
    RETURNS void
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE STRICT SECURITY DEFINER PARALLEL UNSAFE
AS $BODY$

DO $do$
DECLARE 
	startdate DATE;
	enddate DATE;
	yyyymm TEXT;
	basetablename TEXT := 'ta_';
	tablename TEXT;

BEGIN
    FOR mm IN 01..12 LOOP
        startdate:= to_date(yyyy||'-'||mm||'-01', 'YYYY-MM-DD');
		enddate:= startdate + INTERVAL '1 month';
        IF mm < 10 THEN
            yyyymm:= yyyy||'0'||mm;
        ELSE
            yyyymm:= yyyy||''||mm;
        END IF;
        tablename:= basetablename||yyyymm;
        EXECUTE format($$CREATE TABLE here.%I 
                        PARTITION OF here.ta
                        FOR VALUES FROM  (%L) TO (%L);
                        CREATE INDEX ON here.%I  (link_dir);
                       	CREATE INDEX ON here.%I  (tod);
                        CREATE INDEX ON here.%I  (dt);
                        ALTER TABLE here.%I ADD UNIQUE(dt, tod, link_dir);
                        ALTER TABLE here.%I OWNER TO here_admins;
                        $$
                        , tablename, startdate, enddate, tablename, tablename, tablename, tablename);
    END LOOP;
END;
$BODY$;