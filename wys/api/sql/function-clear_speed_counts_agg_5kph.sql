-- FUNCTION: wys.clear_speed_counts_agg_5kph()

-- DROP FUNCTION wys.clear_speed_counts_agg_5kph();

CREATE OR REPLACE FUNCTION wys.clear_speed_counts_agg_5kph(_start_date date, _end_date date)
RETURNS void
LANGUAGE 'plpgsql'
COST 100
VOLATILE SECURITY DEFINER 
AS $BODY$

BEGIN
    WITH deleted AS (
        DELETE FROM wys.speed_counts_agg_5kph
        WHERE
            datetime_bin >= _start_date
            AND datetime_bin < _end_date
        RETURNING speed_counts_agg_5kph_id
    )

    UPDATE wys.raw_data AS r
    SET speed_count_uid = NULL
    FROM deleted
    WHERE
        r.speed_count_uid = deleted.speed_counts_agg_5kph_id
        AND r.datetime_bin >= _start_date
        AND r.datetime_bin < _end_date;

END;

$BODY$;

ALTER FUNCTION wys.clear_speed_counts_agg_5kph(date, date) OWNER TO wys_admins;

GRANT EXECUTE ON FUNCTION wys.clear_speed_counts_agg_5kph(date, date) TO dbadmin;

GRANT EXECUTE ON FUNCTION wys.clear_speed_counts_agg_5kph(date, date) TO wys_bot;

REVOKE EXECUTE ON FUNCTION wys.clear_speed_counts_agg_5kph(date, date) FROM public;