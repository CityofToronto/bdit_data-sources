-- FUNCTION: here_agg.hourly_avg_tt_agg(date, bigint [])

-- DROP FUNCTION IF EXISTS here_agg.hourly_avg_tt_agg(date, bigint []);

CREATE OR REPLACE FUNCTION here_agg.hourly_avg_tt_agg(
    dt date,
    segments bigint [] DEFAULT NULL -- NULL = "all segments"
)
RETURNS void
SECURITY DEFINER
LANGUAGE sql
COST 100
VOLATILE PARALLEL UNSAFE
AS $BODY$

        INSERT INTO here_agg.hourly_avg_tt (segment_id, ver_id, dt, hr, avg_tt, probe_count)
        SELECT
            rs.segment_id,
            rs.ver_id,
            rs.dt,
            rs.hr,
            AVG(rs.tt) AS avg_tt,
            SUM(rs.num_obs) AS probe_count
        FROM here_agg.raw_segments AS rs
        WHERE
            rs.dt >= hourly_avg_tt_agg.dt
            AND rs.dt < hourly_avg_tt_agg.dt::date + 1
            AND (
                hourly_avg_tt_agg.segments IS NULL
                OR rs.segment_id = ANY(hourly_avg_tt_agg.segments)
            )
        GROUP BY
            rs.segment_id,
            rs.ver_id,
            rs.dt,
            rs.hr
        ON CONFLICT ON CONSTRAINT hourly_avg_tt_pkey
        DO UPDATE SET
            avg_tt = EXCLUDED.avg_tt;

$BODY$;

ALTER FUNCTION here_agg.hourly_avg_tt_agg(date, bigint [])
OWNER TO here_admins;

GRANT EXECUTE ON FUNCTION here_agg.hourly_avg_tt_agg TO congestion_bot;
