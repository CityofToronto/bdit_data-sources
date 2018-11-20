-- DROP FUNCTION wys.speed_bins();

CREATE OR REPLACE FUNCTION wys.speed_bins()
  RETURNS trigger AS
$BODY$

BEGIN
	INSERT INTO wys.speed_counts (api_id, datetime_bin, speed_id, count)
	SELECT NEW.api_id, NEW.datetime_bin, A.speed_id, NEW.count
	FROM wys.speed_bins A
	WHERE NEW.speed<@A.speed_bin
	RETURNING speed_count_uid into NEW.speed_count_uid;
	RETURN NEW;

	END;

$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;