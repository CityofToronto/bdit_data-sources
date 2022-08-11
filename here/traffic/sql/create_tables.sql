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
			tablename:= basetablename||yyyymm;
			EXECUTE format($$CREATE TABLE here.%I 
                            PARTITION OF here.ta
                            FOR VALUES FROM  (%L) TO (%L);
                            ALTER TABLE here.%I OWNER TO here_admins;
                            $$
                            , tablename, startdate, enddate, tablename);
		END LOOP;
	END LOOP;
END;
$do$ LANGUAGE plpgsql

