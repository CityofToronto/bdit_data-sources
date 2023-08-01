--Create function to create yearly partition table in staging schema
--inspiration from /bdit_data-sources/here/traffic/sql/transition.sql
CREATE OR REPLACE FUNCTION vds.create_monthly_tables(yyyy text)
    RETURNS void
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE STRICT SECURITY DEFINER PARALLEL UNSAFE
AS $BODY$

DECLARE 
	startdate DATE;
	enddate DATE;
	yyyymm TEXT;
	basetablename TEXT := 'raw_vdsvehicledata_'||yyyy;
	tablename TEXT;

BEGIN
    FOR mm IN 01..12 LOOP
        startdate:= to_date(yyyy||'-'||mm||'-01', 'YYYY-MM-DD');
		enddate:= startdate + INTERVAL '1 month';
        IF mm < 10 THEN
            tablename:= basetablename||'0'||mm;
        ELSE
            tablename:= basetablename||mm;
        END IF;
        EXECUTE format($$CREATE TABLE vds.%I 
                        PARTITION OF vds.%I
                        FOR VALUES FROM (%L) TO (%L);
                        $$
                        , tablename, basetablename, startdate, enddate);
    END LOOP;
END;
$BODY$;
