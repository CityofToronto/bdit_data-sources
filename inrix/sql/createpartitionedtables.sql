/*Master Table*/

DROP TABLE inrix.raw_data CASCADE;

CREATE TABLE inrix.raw_data
(
    tx timestamp without time zone NOT NULL,
    tmc char(9) NOT NULL,
    speed integer NOT NULL,
    score smallint NOT NULL
)
WITH (
    oids = FALSE
);
ALTER TABLE inrix.raw_data
OWNER TO rdumas;



/*Loops through the years and then months for which we currently have data in order to create a partitioned table for each month*/

DO $do$
DECLARE
	startdate DATE;
	yyyymm TEXT;
BEGIN

	for yyyy IN 2012..2016 LOOP
		IF yyyy = 2012 THEN
			FOR mm IN 07..12 LOOP
				startdate:= to_date(yyyy||'-'||mm||'-01', 'YYYY-MM-DD');
				IF mm < 10 THEN
					yyyymm:= yyyy||'0'||mm;
				ELSE
					yyyymm:= yyyy||''||mm;
				END IF;
				EXECUTE $$ CREATE TABLE inrix.raw_data$$||yyyymm||$$
					(--CHECK (tx >= DATE '$$||startdate ||$$'AND tx < DATE '$$||startdate ||$$'+ INTERVAL '1 month')
					) INHERITS (inrix.raw_data)
				$$;

			END LOOP;

		ELSEIF yyyy = 2016 THEN
			FOR mm IN 01..03 LOOP
				startdate:= to_date(yyyy||'-'||mm||'-01', 'YYYY-MM-DD');
				IF mm < 10 THEN
					yyyymm:= yyyy||'0'||mm;
				ELSE
					yyyymm:= yyyy||''||mm;
				END IF;
				EXECUTE $$ CREATE TABLE inrix.raw_data$$||yyyymm||$$
					(--CHECK (tx >= DATE '$$||startdate ||$$'AND tx < DATE '$$||startdate ||$$'+ INTERVAL '1 month')
					) INHERITS (inrix.raw_data)
				$$;
			END LOOP;

		ELSE 
			FOR mm IN 01..12 LOOP
				startdate:= to_date(yyyy||'-'||mm||'-01', 'YYYY-MM-DD');
				IF mm < 10 THEN
					yyyymm:= yyyy||'0'||mm;
				ELSE
					yyyymm:= yyyy||''||mm;
				END IF;
				EXECUTE $$ CREATE TABLE inrix.raw_data$$||yyyymm||$$
					(--CHECK (tx >= DATE '$$||startdate ||$$'AND tx < DATE '$$||startdate ||$$'+ INTERVAL '1 month')
					) INHERITS (inrix.raw_data)
				$$;
			END LOOP;
		END IF;
	END LOOP;
END;
$do$ LANGUAGE plpgsql


