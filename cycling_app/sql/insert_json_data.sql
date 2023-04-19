-- SELECT (json_each(data_)).key FROM cycling.survey_keys;

-- SELECT (json_each(data_ -> 'gender')).key::INT, REPLACE((json_each(data_ -> 'gender')).value::TEXT, '"','')   FROM cycling.survey_keys

/*Cycle through each key of the cycling.survey_keys json and create a table, then insert the codes*/
DO $$
DECLARE
	key_ TEXT;

BEGIN
	FOR key_ IN
		SELECT (json_each(data_)).key FROM cycling.survey_keys
	LOOP 
-- 		EXECUTE format($x$
-- 			DROP table cycling.usr_%s
-- 			$x$, key_);

		 EXECUTE format($x$
			CREATE table cycling.usr_%s (
				%s_id INT PRIMARY KEY,
				%s TEXT NOT NULL UNIQUE
				)
			$x$, key_, key_, key_);
/*Insert Data*/
		EXECUTE format($x$
			INSERT INTO cycling.usr_%s 
			SELECT (json_each(data_ -> %L )).key::INT, REPLACE((json_each(data_ -> %L)).value::TEXT, '"','')   FROM cycling.survey_keys

			$x$, key_, key_, key_);
	END LOOP;
	
END$$;