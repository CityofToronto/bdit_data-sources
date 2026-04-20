-- FUNCTION: here_agg.hourly_avg_tt_agg(date)

-- DROP FUNCTION IF EXISTS here_agg.hourly_avg_tt_agg(date);

CREATE OR REPLACE FUNCTION here_agg.hourly_avg_tt_agg(
    mnth date
)
RETURNS void
SECURITY DEFINER
LANGUAGE sql
COST 100
VOLATILE PARALLEL UNSAFE
AS $BODY$

        INSERT INTO here_agg.hourly_avg_tt (segment_id, dt, hr, avg_tt)
        SELECT
            segment_id,
            dt,
            hr,
            AVG(tt) AS avg_tt
        FROM here_agg.raw_segments
        WHERE dt >= hourly_avg_tt_agg.mnth AND dt < hourly_avg_tt_agg.mnth::date + interval '1 month'
        GROUP BY
            segment_id,
            dt,
            hr
        ON CONFLICT ON CONSTRAINT hourly_avg_tt_pkey
        DO UPDATE SET
            avg_tt = EXCLUDED.avg_tt;

$BODY$;

ALTER FUNCTION here_agg.hourly_avg_tt_agg(date)
OWNER TO here_admins;

GRANT EXECUTE ON FUNCTION here_agg.hourly_avg_tt_agg TO congestion_bot;
