-- DROP FUNCTION miovision.delete_raw_data(date, date);

CREATE OR REPLACE FUNCTION miovision.delete_raw_data(
    start_date date,
    end_date date)
  RETURNS integer AS
$BODY$
BEGIN	
	IF end_date::date - start_date::date <30 THEN

		DELETE FROM miovision.raw_data
		WHERE datetime_bin BETWEEN start_date AND end_date;
		RETURN 1;
	ELSE 
		RAISE EXCEPTION 'Attempting to delete more than a month of data';
		RETURN 0;
	END IF;

END;
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;
ALTER FUNCTION miovision.delete_raw_data(date, date)
  OWNER TO rliu;