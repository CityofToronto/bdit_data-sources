/*Bulk Inrix data copy with parameters tweaked to hopefully accelerate the bulk COPY process*/

DO $do$
DECLARE
	startdate DATE;
	yyyymm TEXT;
BEGIN

SET LOCAL synchronous_commit=off;
SET LOCAL commit_delay = 80000;
-- SELECT set_config('checkpoint_segments', 64, true);

	for yyyy IN 2012..2016 LOOP
		IF yyyy = 2012 THEN
			FOR mm IN 08..12 LOOP
				IF mm < 10 THEN
					yyyymm:= yyyy||'0'||mm;
				ELSE
					yyyymm:= yyyy||''||mm;
				END IF;
				RAISE NOTICE '%: COPYING FILE Ryerson_Toronto_%', timeofday(), yyyymm;
				EXECUTE $$ALTER TABLE inrix.raw_data$$||yyyymm||$$ SET UNLOGGED;$$;
				EXECUTE $$COPY inrix.raw_data$$||yyyymm||$$ FROM 'E:\BIGDATA\INRIX Raw\Phase 2\Ryerson_Toronto_$$||yyyymm||$$.csv' DELIMITER ',' CSV;$$;
				EXECUTE $$ALTER TABLE inrix.raw_data$$||yyyymm||$$ SET LOGGED;$$;
			END LOOP;

		ELSEIF yyyy = 2016 THEN
			FOR mm IN 01..03 LOOP
				startdate:= to_date(yyyy||'-'||mm||'-01', 'YYYY-MM-DD');
				IF mm < 10 THEN
					yyyymm:= yyyy||'0'||mm;
				ELSE
					yyyymm:= yyyy||''||mm;
				END IF;
				RAISE NOTICE '%: COPYING FILE Ryerson_Toronto_%', timeofday(), yyyymm;
				EXECUTE $$ALTER TABLE inrix.raw_data$$||yyyymm||$$ SET UNLOGGED;$$;
				EXECUTE $$COPY inrix.raw_data$$||yyyymm||$$ FROM 'E:\BIGDATA\INRIX Raw\Phase 2\Ryerson_Toronto_$$||yyyymm||$$.csv' DELIMITER ',' CSV;$$;
				EXECUTE $$ALTER TABLE inrix.raw_data$$||yyyymm||$$ SET LOGGED;$$;
			END LOOP;
		ELSEIF yyyy = 2013 THEN
			FOR mm IN 01..06 LOOP
				startdate:= to_date(yyyy||'-'||mm||'-01', 'YYYY-MM-DD');
				IF mm < 10 THEN
					yyyymm:= yyyy||'0'||mm;
				ELSE
					yyyymm:= yyyy||''||mm;
				END IF;
				RAISE NOTICE '%: COPYING FILE Ryerson_Toronto_%', timeofday(), yyyymm;
				EXECUTE $$ALTER TABLE inrix.raw_data$$||yyyymm||$$ SET UNLOGGED;$$;
				EXECUTE $$COPY inrix.raw_data$$||yyyymm||$$ FROM 'E:\BIGDATA\INRIX Raw\Phase 2\Ryerson_Toronto_$$||yyyymm||$$.csv' DELIMITER ',' CSV;$$;
				EXECUTE $$ALTER TABLE inrix.raw_data$$||yyyymm||$$ SET LOGGED;$$;
			END LOOP;

		ELSE 
			FOR mm IN 01..12 LOOP
				startdate:= to_date(yyyy||'-'||mm||'-01', 'YYYY-MM-DD');
				IF mm < 10 THEN
					yyyymm:= yyyy||'0'||mm;
				ELSE
					yyyymm:= yyyy||''||mm;
				END IF;
				RAISE NOTICE '%: COPYING FILE Ryerson_Toronto_%', timeofday(), yyyymm;
				EXECUTE $$ALTER TABLE inrix.raw_data$$||yyyymm||$$ SET UNLOGGED;$$;
				EXECUTE $$COPY inrix.raw_data$$||yyyymm||$$ FROM 'E:\BIGDATA\INRIX Raw\Phase 2\Ryerson_Toronto_$$||yyyymm||$$.csv' DELIMITER ',' CSV;$$;
				EXECUTE $$ALTER TABLE inrix.raw_data$$||yyyymm||$$ SET LOGGED;$$;
			END LOOP;
		END IF;
	END LOOP;
END;
$do$ LANGUAGE plpgsql
	
		