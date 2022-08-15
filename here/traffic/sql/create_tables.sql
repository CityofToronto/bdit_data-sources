-- Parent table structure
CREATE TABLE IF NOT EXISTS here.ta
(
    link_dir text NOT NULL,
	dt date NOT NULL,
	tod time without time zone NOT NULL,
	tx timestamp without time zone NOT NULL, 
    length integer,
    mean numeric(4,1) NOT NULL,
    stddev numeric(4,1) NOT NULL,
    min_spd integer NOT NULL,
    max_spd integer NOT NULL,
    confidence integer NOT NULL,
	sample_size integer
) PARTITION BY RANGE (dt);

ALTER TABLE here.ta
  OWNER TO here_admins;

-- simple view structure
CREATE VIEW here.ta_view AS

SELECT *
FROM here.ta;

ALTER TABLE here.ta_view
  OWNER TO here_admins;
  
-- Instead of inserting to ta_view, 
-- insert to parent table trigger

CREATE TRIGGER transform_trigger
        INSTEAD OF INSERT
        ON here.ta_view
        FOR EACH ROW
        EXECUTE PROCEDURE here.here_insert_trigger();


--Loops through the years and then months for which we currently have data in order to create a partitioned table for each month
DO $do$
DECLARE
	startdate DATE;
	enddate DATE;
	yyyymm TEXT;
	basetablename TEXT := 'ta_';
	tablename TEXT;
BEGIN

	for yyyy IN 2012..2021 LOOP
		FOR mm IN 01..12 LOOP
			startdate:= to_date(yyyy||'-'||mm||'-01', 'YYYY-MM-DD');
			enddate:= startdate + INTERVAL '1 month';
			IF mm < 10 THEN
				yyyymm:= yyyy||'0'||mm;
			ELSE
				yyyymm:= yyyy||''||mm;
			END IF;
			tablename:= basetablename||yyyymm||'_new';
			EXECUTE format($$CREATE TABLE here.%I 
                            PARTITION OF here.ta_new
                            FOR VALUES FROM  (%L) TO (%L);
                            ALTER TABLE here.%I ADD UNIQUE(dt, tod, link_dir);
                            CREATE INDEX ON here.%I USING brin(tod);
                            CREATE INDEX ON here.%I (link_dir);
                            ALTER TABLE here.%I OWNER TO here_admins;
                            $$
                            , tablename, startdate, enddate, tablename, tablename, tablename, tablename);
		END LOOP;
	END LOOP;
END;
$do$ LANGUAGE plpgsql

