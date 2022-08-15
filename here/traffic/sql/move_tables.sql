-- Move data from here.ta_* to the newly structured here.ta_*
-- while performing separation of tx column to dt (Date) and tod (time)
-- loop through each table, move data to new table, then truncate the old one
--Loops through the years and then months for which we currently have data in order to create a partitioned table for each month

DO $do$
DECLARE
	startdate DATE;
	yyyymm TEXT;
	basetablename TEXT := 'ta_';
	tablename TEXT;
	newtablename TEXT;
BEGIN

	for yyyy IN 2012..2018 LOOP
		FOR mm IN 01..12 LOOP
			startdate:= to_date(yyyy||'-'||mm||'-01', 'YYYY-MM-DD');
			IF mm < 10 THEN
				yyyymm:= yyyy||'0'||mm;
			ELSE
				yyyymm:= yyyy||''||mm;
			END IF;
			tablename:= basetablename||yyyymm;
			newtablename:= basetablename||yyyymm||'_new';
			EXECUTE format($$
						   INSERT INTO here.%I
						   SELECT link_dir, tx::date, tx::time without time zone, tx, length, mean, stddev, min_spd, max_spd, confidence, null
						   FROM here.%I;
						   TRUNCATE TABLE here.%I;
						   $$   
                            ,  newtablename, tablename, tablename);
		END LOOP;
	END LOOP;
END;
$do$ LANGUAGE plpgsql
