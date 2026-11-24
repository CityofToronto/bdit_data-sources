CREATE OR REPLACE FUNCTION ecocounter.open_data_daily_counts_insert(
    yyyy integer
)
RETURNS void
LANGUAGE sql
COST 100
VOLATILE

AS $BODY$

    INSERT INTO ecocounter.open_data_daily_counts (
        site_id, site_description, direction, dt, daily_volume
    )
    SELECT
            s.site_id,
            s.site_description,
            af.direction,
            cc.datetime_bin::date AS dt,
            SUM(calibrated_volume) AS daily_volume
        --this view excludes anomalous ranges
        FROM ecocounter.counts AS cc
        JOIN ecocounter.active_flow_dates AS af 
            ON cc.flow_id = ANY(af.flow_ids)
			AND cc.datetime_bin::date  <@ af.valid_range
        JOIN ecocounter.sites AS s USING (site_id)
        WHERE
            cc.datetime_bin >= to_date(yyyy::varchar, 'yyyy')
            AND cc.datetime_bin < to_date((yyyy+1)::varchar, 'yyyy')
        GROUP BY
            s.site_id,
            s.site_description,
            af.direction,
            cc.datetime_bin::date,
			af.n_flows,
            af.bin_size
        HAVING
            --all datetime bins present, corrected for bin size
            COUNT(DISTINCT cc.datetime_bin) = (3600*24 / EXTRACT(epoch FROM bin_size))
            --all flows are present
		    AND COUNT(DISTINCT cc.flow_id) = af.n_flows
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