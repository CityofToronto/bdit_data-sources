-- FUNCTION: wys.aggregate_speed_counts_one_hour()

-- DROP FUNCTION wys.aggregate_speed_counts_one_hour();

CREATE OR REPLACE FUNCTION wys.aggregate_speed_counts_one_hour_5kph(
    _start_date date,
    _end_date date
)
RETURNS void
LANGUAGE 'plpgsql'
COST 100
VOLATILE SECURITY DEFINER 
AS $BODY$

BEGIN
    
    WITH insert_data AS (
        --Aggregated into speed bins and 1 hour bin
        INSERT INTO wys.speed_counts_agg_5kph (api_id, datetime_bin, speed_id, volume)
        SELECT
            raw.api_id,
            date_trunc('hour', raw.datetime_bin) AS hr,
            sbo.speed_id,
            sum(raw.count) AS volume
        FROM wys.raw_data AS raw
        JOIN wys.speed_bins_old AS sbo ON raw.speed <@ sbo.speed_bin
        WHERE 
            raw.speed_count_uid IS NULL
            AND raw.count IS NOT NULL
            AND raw.datetime_bin >= _start_date
            AND raw.datetime_bin < _end_date
        GROUP BY
            raw.api_id,
            hr, 
            sbo.speed_id 
        RETURNING 
            speed_counts_agg_5kph_id,
            api_id,
            datetime_bin,
            speed_id
        )
    
    UPDATE wys.raw_data AS raw
    SET speed_count_uid = ins.speed_counts_agg_5kph_id
    FROM insert_data AS ins
    JOIN wys.speed_bins_old AS sbo USING (speed_id)
    WHERE
        raw.speed_count_uid IS NULL
        AND raw.api_id = ins.api_id
        AND raw.speed <@ sbo.speed_bin
        AND raw.datetime_bin >= ins.datetime_bin
        AND raw.datetime_bin < ins.datetime_bin + interval '1 hour'
        --select the correct partitions
        AND raw.datetime_bin >= _start_date
        AND raw.datetime_bin < _end_date;

END;

$BODY$;

ALTER FUNCTION wys.aggregate_speed_counts_one_hour_5kph(date, date)
OWNER TO wys_admins;

GRANT EXECUTE ON FUNCTION wys.aggregate_speed_counts_one_hour_5kph(date, date) TO dbadmin;

GRANT EXECUTE ON FUNCTION wys.aggregate_speed_counts_one_hour_5kph(date, date) TO wys_bot;

REVOKE EXECUTE ON FUNCTION wys.aggregate_speed_counts_one_hour_5kph(date, date) FROM public;
