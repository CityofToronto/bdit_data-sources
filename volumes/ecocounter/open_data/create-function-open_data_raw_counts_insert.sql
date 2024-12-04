CREATE OR REPLACE FUNCTION ecocounter.open_data_15min_counts_insert(
    yyyy integer
)
RETURNS void
LANGUAGE sql
COST 100
VOLATILE

AS $BODY$

    WITH complete_dates AS (
        SELECT
            s.site_id,
            f.direction_main,
            cc.datetime_bin::date,
            COUNT(DISTINCT cc.datetime_bin) AS datetime_bin_unique
        --this view excludes anomalous ranges
        FROM ecocounter.counts AS cc
        JOIN ecocounter.flows AS f USING (flow_id)
        JOIN ecocounter.sites AS s USING (site_id)
        WHERE
            cc.datetime_bin >= to_date(yyyy::varchar, 'yyyy')
            AND cc.datetime_bin < to_date((yyyy+1)::varchar, 'yyyy')
        GROUP BY
            s.site_id,
            f.direction_main,
            cc.datetime_bin::date,
            f.bin_size
        HAVING
            --all datetime bins present, corrected for bin size
            COUNT(DISTINCT cc.datetime_bin) = (3600*24 / EXTRACT(epoch FROM bin_size))
            --non-zero count days
            AND SUM(cc.calibrated_volume) > 0
    )

    INSERT INTO ecocounter.open_data_15min_counts (
        site_id, site_description, direction, datetime_bin, bin_volume
    )
    SELECT  
        s.site_id,
        s.site_description,
        f.direction_main AS direction,
        cc.datetime_bin AS datetime_bin,
        SUM(cc.calibrated_volume) AS bin_volume
    --this view excludes anomalous ranges
    FROM ecocounter.counts AS cc
    JOIN ecocounter.flows AS f USING (flow_id)
    JOIN ecocounter.sites AS s USING (site_id)
    JOIN complete_dates AS cd
    ON
        cd.site_id = s.site_id
        AND cd.direction_main = f.direction_main
        AND cc.datetime_bin >= cd.datetime_bin
        AND cc.datetime_bin < cd.datetime_bin + interval '1 day'
    WHERE
        cc.datetime_bin >= to_date(yyyy::varchar, 'yyyy')
        AND cc.datetime_bin < to_date((yyyy+1)::varchar, 'yyyy')
    GROUP BY
        s.site_id,
        s.site_description,
        f.direction_main,
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
