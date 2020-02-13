/*Loops to create monthly open_data views of the detailed stationary data
*/

DO $do$
DECLARE
	startdate DATE;
	yyyymm TEXT;
	basetablename TEXT := 'wys_stationary_detailed_';
	tablename TEXT;
BEGIN

	for yyyy IN 2019..2020 LOOP
		FOR mm IN 01..12 LOOP
			startdate:= yyyy||'-'||mm||'-01';
			IF mm < 10 THEN
				yyyymm:= yyyy||'0'||mm;
			ELSE
				yyyymm:= yyyy||''||mm;
			END IF;
			tablename:= basetablename||yyyymm;
			EXECUTE format($$CREATE VIEW open_data.%I AS
							SELECT sign_id, loc.address, loc.dir, datetime_bin, speed_bin::TEXT, volume
							FROM open_data.wys_stationary_locations od
							INNER JOIN wys.stationary_signs loc USING(sign_id)
							INNER JOIN wys.speed_counts_agg agg  ON loc.api_id = agg.api_id
																 AND datetime_bin >= od.start_date and datetime_bin < od.end_date
							INNER JOIN wys.speed_bins USING (speed_id)
							WHERE datetime_bin >= %L::DATE AND datetime_bin < %L::DATE + INTERVAL '1 month'
						   $$, tablename, startdate, startdate);
		END LOOP;
	END LOOP;
END;
$do$ LANGUAGE plpgsql;