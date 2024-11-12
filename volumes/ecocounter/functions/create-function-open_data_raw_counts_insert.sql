CREATE OR REPLACE FUNCTION ecocounter.open_data_raw_counts_insert(
    yyyy integer
)
RETURNS void
LANGUAGE sql
COST 100
VOLATILE

AS $BODY$

    INSERT INTO ecocounter.open_data_raw_counts (
        site_id, site_description, direction, datetime_bin, bin_volume
    )
    SELECT
        s.site_id,
        s.site_description,
        f.direction_main AS direction,
        cc.datetime_bin AS datetime_bin,
        SUM(cc.corrected_volume) AS bin_volume
    --this view excludes anomalous ranges
    FROM ecocounter.counts_corrected AS cc
    JOIN ecocounter.flows AS f USING (flow_id)
    JOIN ecocounter.sites AS s USING (site_id)
    WHERE
        cc.datetime_bin >= to_date(yyyy::varchar, 'yyyy')
        AND cc.datetime_bin < to_date((yyyy+1)::varchar, 'yyyy')
    GROUP BY
        s.site_id,
        s.site_description,
        f.direction_main,
        cc.datetime_bin
    HAVING SUM(cc.corrected_volume) > 0
    ON CONFLICT (site_id, direction, datetime_bin)
    DO NOTHING;

$BODY$;

ALTER FUNCTION ecocounter.open_data_raw_counts_insert(integer) OWNER TO ecocounter_admins;

GRANT EXECUTE ON FUNCTION ecocounter.open_data_raw_counts_insert(integer) TO ecocounter_admins;
GRANT EXECUTE ON FUNCTION ecocounter.open_data_raw_counts_insert(integer) TO ecocounter_bot;
REVOKE EXECUTE ON FUNCTION ecocounter.open_data_raw_counts_insert(integer) FROM bdit_humans;

COMMENT ON FUNCTION ecocounter.open_data_raw_counts_insert(integer) IS
'Function to insert disaggregate data for a year into `ecocounter.open_data_raw_counts`. '
'Does not overwrite existing data (eg. if sensitivity was retroactively updated).';
