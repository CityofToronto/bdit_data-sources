CREATE OR REPLACE FUNCTION ecocounter.open_data_15min_counts_insert(
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
		ARRAY_AGG(DISTINCT f.flow_id) AS flow_ids,
		COUNT(DISTINCT f.flow_id) AS n_flow_id_unique,
		CASE WHEN s.bidirectional THEN f.flow_direction ELSE f.direction_main END AS direction
	FROM ecocounter.sites AS s
    JOIN ecocounter.flows AS f USING (site_id)
	WHERE f.date_decommissioned IS NULL
	GROUP BY (s.site_id, direction, bin_size)
	),

    complete_dates AS (
    --- contains full dates with full flows to exclude partial data
        SELECT
            fi.site_id,
            cc.datetime_bin::date,
            ARRAY_AGG(DISTINCT cc.flow_id) AS flow_ids,
            fi.direction,			
            COUNT(DISTINCT cc.datetime_bin) AS datetime_bin_unique
        --this view excludes anomalous ranges
        FROM ecocounter.counts cc
        JOIN directional_flow_count AS fi 
            ON cc.flow_id = ANY(fi.flow_ids)
        WHERE
            cc.datetime_bin >= to_date(yyyy::varchar, 'yyyy')
            AND cc.datetime_bin < to_date((yyyy+1)::varchar, 'yyyy')
        GROUP BY
            fi.site_id,
            cc.datetime_bin::date,
            fi.direction,
            fi.bin_size,
            fi.n_flow_id_unique
        HAVING
            --all datetime bins present, corrected for bin size
            COUNT(DISTINCT cc.datetime_bin) = (3600*24 / EXTRACT(epoch FROM bin_size))
            --all flows are present
            AND COUNT(DISTINCT cc.flow_id) = fi.n_flow_id_unique
        )

    INSERT INTO ecocounter.open_data_15min_counts (
        site_id, site_description, direction, datetime_bin, bin_volume
    )
    SELECT  
        s.site_id,
        s.site_description,
        cd.direction,
        cc.datetime_bin AS datetime_bin,
        SUM(cc.calibrated_volume) AS bin_volume
    --this view excludes anomalous ranges
    FROM ecocounter.counts AS cc
    JOIN ecocounter.flows AS f USING (flow_id)
    JOIN ecocounter.sites AS s USING (site_id)
    JOIN complete_dates AS cd
    ON
        cd.site_id = s.site_id
		AND cc.flow_id = ANY(cd.flow_ids) 
        AND cc.datetime_bin >= cd.datetime_bin
        AND cc.datetime_bin < cd.datetime_bin + interval '1 day'
    WHERE
        cc.datetime_bin >= to_date(yyyy::varchar, 'yyyy')
        AND cc.datetime_bin < to_date((yyyy+1)::varchar, 'yyyy')
    GROUP BY
        s.site_id,
        s.site_description,
        cd.direction,
        cc.datetime_bin
    ON CONFLICT (site_id, direction, datetime_bin)
    DO NOTHING;

$BODY$;

ALTER FUNCTION ecocounter.open_data_15min_counts_insert(integer) OWNER TO ecocounter_admins;

GRANT EXECUTE ON FUNCTION ecocounter.open_data_15min_counts_insert(integer) TO ecocounter_admins;
GRANT EXECUTE ON FUNCTION ecocounter.open_data_15min_counts_insert(integer) TO ecocounter_bot;
REVOKE EXECUTE ON FUNCTION ecocounter.open_data_15min_counts_insert(integer) FROM bdit_humans;

COMMENT ON FUNCTION ecocounter.open_data_15min_counts_insert(integer) IS
'Function to insert disaggregate data for a year into `ecocounter.open_data_15min_counts`. '
'Does not overwrite existing data (eg. if sensitivity was retroactively updated).';
