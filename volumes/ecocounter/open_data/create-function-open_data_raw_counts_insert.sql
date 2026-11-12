CREATE OR REPLACE FUNCTION ecocounter.open_data_15min_counts_insert(
    yyyy integer
)
RETURNS void
LANGUAGE sql
COST 100
VOLATILE

AS $BODY$

WITH complete_dates AS (
    --- contains full dates with full flows to exclude partial data
        SELECT
            af.site_id,
            cc.datetime_bin::date,
            ARRAY_AGG(DISTINCT cc.flow_id) AS flow_ids,
            af.direction,			
            COUNT(DISTINCT cc.datetime_bin) AS datetime_bin_unique
        --this view excludes anomalous ranges
        FROM ecocounter.counts cc
        JOIN ecocounter.active_flow_dates AS af 
            ON cc.flow_id = ANY(af.flow_ids)
			AND cc.datetime_bin::date  <@ af.valid_range
        WHERE
            cc.datetime_bin >= to_date(yyyy::varchar, 'yyyy')
            AND cc.datetime_bin < to_date((yyyy+1)::varchar, 'yyyy')
        GROUP BY
            af.site_id,
            cc.datetime_bin::date,
            af.direction,
            af.bin_size,
            af.n_flows
        HAVING
            --all datetime bins present, corrected for bin size
            COUNT(DISTINCT cc.datetime_bin) = (3600*24 / EXTRACT(epoch FROM bin_size))
            --all flows are present
            AND COUNT(DISTINCT cc.flow_id) = af.n_flows
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
