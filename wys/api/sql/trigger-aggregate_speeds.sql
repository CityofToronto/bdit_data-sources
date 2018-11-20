CREATE OR REPLACE FUNCTION wys.aggregate_speeds()
  RETURNS trigger AS
$BODY$

BEGIN
	INSERT INTO wys.speed_counts (api_id, datetime_bin, speed_id, count)
	SELECT NEW.api_id, NEW.datetime_bin, A.speed_id, NEW.count
	FROM wys.speed_bins A
	WHERE NEW.speed<@A.speed_bin;
	RETURN NEW;

	END;

$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;
ALTER FUNCTION wys.aggregate_speeds()
  OWNER TO rliu;

-- DROP TRIGGER insert_raw_data_trigger ON wys.raw_data;

CREATE TRIGGER insert_raw_data_trigger
  BEFORE INSERT
  ON wys.raw_data
  FOR EACH ROW
  EXECUTE PROCEDURE wys.aggregate_speeds();