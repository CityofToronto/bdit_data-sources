CREATE OR REPLACE FUNCTION ecocounter.open_data_daily_counts_insert(
    yyyy integer
)
RETURNS void
LANGUAGE sql
COST 100
VOLATILE

AS $BODY$

    WITH flow_info AS (
    --- full table of site_id-direction and number of flows
    SELECT
            s.site_id,
            f.bin_size,
            ARRAY_AGG(DISTINCT f.flow_id) AS flow_ids,
            COUNT(DISTINCT f.flow_id) AS n_flow_id_unique,
            CASE WHEN s.bidirectional THEN f.flow_direction ELSE f.direction_main END AS direction
        FROM dboucau.sites AS s
        JOIN ecocounter.flows AS f USING (site_id)
        WHERE f.date_decommissioned IS NULL
	    GROUP BY (s.site_id, direction, bin_size)
	)
    INSERT INTO ecocounter.open_data_daily_counts (
        site_id, site_description, direction, dt, daily_volume
    )
    SELECT
            s.site_id,
            s.site_description,
            fi.direction,
            cc.datetime_bin::date AS dt,
            SUM(calibrated_volume) AS daily_volume
        --this view excludes anomalous ranges
        FROM ecocounter.counts AS cc
    
        JOIN flow_info AS fi 
            ON cc.flow_id = ANY(fi.flow_ids)
        JOIN ecocounter.sites AS s USING (site_id)
        WHERE
            cc.datetime_bin >= to_date(2026::varchar, 'yyyy')
            AND cc.datetime_bin < to_date((2026+1)::varchar, 'yyyy')
        GROUP BY
            s.site_id,
            s.site_description,
            fi.direction,
            cc.datetime_bin::date,
            fi.bin_size
        HAVING
            --all datetime bins present, corrected for bin size
            COUNT(DISTINCT cc.datetime_bin) = (3600*24 / EXTRACT(epoch FROM bin_size))
            --all flows are present
		    AND COUNT(DISTINCT cc.flow_id) = fi.n_flow_id_unique
    ON CONFLICT (site_id, direction, dt)
    DO NOTHING;

$BODY$;

ALTER FUNCTION ecocounter.open_data_daily_counts_insert(integer) OWNER TO ecocounter_admins;

GRANT EXECUTE ON FUNCTION ecocounter.open_data_daily_counts_insert(integer) TO ecocounter_admins;
GRANT EXECUTE ON FUNCTION ecocounter.open_data_daily_counts_insert(integer) TO ecocounter_bot;
REVOKE EXECUTE ON FUNCTION ecocounter.open_data_daily_counts_insert(integer) FROM bdit_humans;

COMMENT ON FUNCTION ecocounter.open_data_daily_counts_insert(integer) IS
'Function to insert data for a year into `ecocounter.open_data_daily_counts`. '
'Does not overwrite existing data (eg. if sensitivity was retroactively updated).';