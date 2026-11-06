CREATE OR REPLACE FUNCTION ecocounter.open_data_daily_counts_insert(
    yyyy integer
)
RETURNS void
LANGUAGE sql
COST 100
VOLATILE

AS $BODY$

WITH flow_info AS (
    --- full table of site_id, direction, flow_ids
	SELECT
        s.site_id,
		f.bin_size,
		f.flow_id,
		--- parse direction by flow_id
		CASE WHEN s.bidirectional THEN f.flow_direction ELSE f.direction_main END AS direction,
		---a date range is used to include all flows in their respective active ranges
		daterange(
		    COALESCE(f.first_active::date, (SELECT MIN(first_active) FROM ecocounter.flows)::date),
		    COALESCE(f.date_decommissioned::date, CURRENT_DATE)
		) AS flow_valid_range
	FROM ecocounter.sites AS s
    JOIN ecocounter.flows AS f USING (site_id)
	GROUP BY (s.site_id,f.flow_id, direction, bin_size, flow_valid_range)
	),
listed_flow_dts AS (
	--- aggregate flow_id counts by date
	SELECT
	    f.site_id,
		f.bin_size,
	    f.direction,
	    gs.valid_date::date,
	    ARRAY_AGG(f.flow_id ORDER BY f.flow_id DESC) AS flow_array,
		COUNT(DISTINCT(flow_id)) AS n_flows
	FROM flow_info AS f,
	generate_series(lower(f.flow_valid_range), upper(f.flow_valid_range), '1 day'::interval) AS gs(valid_date)
	GROUP BY
	    f.site_id,
	    f.direction,
		bin_size,
	    gs.valid_date::date
		),
aggregated_flow_dts AS (
	--- aggregate dateranges by flow_id count
	SELECT
		site_id, 
		bin_size,
		direction, 
		flow_array,
		n_flows,	
		daterange(
	            MIN(valid_date)::date,
	            MAX(valid_date)::date,
				'[]'
	        ) AS valid_range
		FROM listed_flow_dts AS f
		GROUP BY (site_id, direction, flow_array, n_flows,bin_size)
		)
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
        JOIN aggregated_flow_dts AS af 
            ON cc.flow_id = ANY(af.flow_array)
			AND cc.datetime_bin::date  <@ af.valid_range
        JOIN ecocounter.sites AS s USING (site_id)
        WHERE
            cc.datetime_bin >= to_date(2026::varchar, 'yyyy')
            AND cc.datetime_bin < to_date((2026+1)::varchar, 'yyyy')
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