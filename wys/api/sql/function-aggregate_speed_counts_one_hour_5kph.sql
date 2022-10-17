-- FUNCTION: wys.aggregate_speed_counts_one_hour()

-- DROP FUNCTION wys.aggregate_speed_counts_one_hour();

CREATE OR REPLACE FUNCTION wys.aggregate_speed_counts_one_hour_5kph( _start_date date, _end_date date
	)
    RETURNS void
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE SECURITY DEFINER 
AS $BODY$

BEGIN
	

	WITH insert_data AS (
		--Aggregated into speed bins and 1 hour bin
		INSERT INTO 	wys.speed_counts_agg_5kph (api_id, datetime_bin, speed_id, volume)
		SELECT 			api_id, date_trunc('hour', datetime_bin) datetime_bin, speed_id, sum(count) AS volume
		FROM 			wys.raw_data
		INNER JOIN 		wys.speed_bins_old ON speed <@ speed_bin
		WHERE			speed_count_uid IS NULL AND 
                        "count" IS NOT NULL AND 
                        datetime_bin::date >= _start_date AND datetime_bin::date < _end_date
        
		GROUP BY 		api_id, datetime_bin,  speed_id 
		RETURNING 		speed_counts_agg_5kph_id, api_id, datetime_bin, speed_id
		)
	
	UPDATE 	wys.raw_data A
	SET 	speed_count_uid=B.speed_counts_agg_5kph_id
	FROM 	insert_data B
	WHERE 	A.speed_count_uid IS NULL AND
	 		A.api_id=B.api_id AND
	 		A.datetime_bin >= B.datetime_bin AND 
            A.datetime_bin < B.datetime_bin + INTERVAL '1 hour';

END;

$BODY$;

ALTER FUNCTION wys.aggregate_speed_counts_one_hour_5kph(date, date)
    OWNER TO wys_admins;

GRANT EXECUTE ON FUNCTION wys.aggregate_speed_counts_one_hour_5kph(date, date) TO dbadmin;

GRANT EXECUTE ON FUNCTION wys.aggregate_speed_counts_one_hour_5kph(date, date) TO wys_bot;

REVOKE EXECUTE ON FUNCTION wys.aggregate_speed_counts_one_hour_5kph(date, date) FROM PUBLIC;

CREATE OR REPLACE FUNCTION wys.aggregate_speed_counts_one_hour_5kph(_mon DATE
	)
    RETURNS void
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE SECURITY DEFINER 
AS $BODY$

BEGIN
	

		--Aggregated into speed bins and 1 hour bin
		INSERT INTO wys.speed_counts_agg_5kph (api_id, datetime_bin, speed_id, volume)
		SELECT api_id, date_trunc('hour', datetime_bin) dt, speed_id, sum(count) AS volume
		FROM wys.raw_data
		INNER JOIN wys.speed_bins_old ON speed <@ speed_bin
		WHERE	datetime_bin >= _mon AND datetime_bin < _mon + INTERVAL '1 month'
		GROUP BY api_id, dt,  speed_id
		ON CONFLICT DO NOTHING;
		
 
END;

$BODY$;

ALTER FUNCTION wys.aggregate_speed_counts_one_hour_5kph(date)
    OWNER TO rdumas;

GRANT EXECUTE ON FUNCTION wys.aggregate_speed_counts_one_hour_5kph(date) TO dbadmin;

GRANT EXECUTE ON FUNCTION wys.aggregate_speed_counts_one_hour_5kph(date) TO wys_bot;

REVOKE EXECUTE ON FUNCTION wys.aggregate_speed_counts_one_hour_5kph(date) FROM PUBLIC;