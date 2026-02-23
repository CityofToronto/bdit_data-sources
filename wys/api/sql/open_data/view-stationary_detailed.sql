-- View: open_data.wys_stationary_detailed

-- DROP VIEW open_data.wys_stationary_detailed;

CREATE OR REPLACE VIEW open_data.wys_stationary_detailed
AS
SELECT 
    od.sign_id,
    loc.address,
    loc.dir,
    agg.datetime_bin,
    bins.speed_bin::text AS speed_bin,
    agg.volume
FROM open_data.wys_stationary_locations AS od
JOIN wys.stationary_signs AS loc USING (sign_id)
JOIN wys.speed_counts_agg_5kph AS agg ON 
    loc.api_id = agg.api_id
    AND agg.datetime_bin >= od.start_date 
    AND (od.end_date IS NULL 
        OR agg.datetime_bin < od.end_date)
JOIN wys.speed_bins_old AS bins USING (speed_id)
WHERE agg.datetime_bin < date_trunc('month'::text, now());

ALTER TABLE open_data.wys_stationary_detailed OWNER TO wys_admins;

GRANT SELECT ON TABLE open_data.wys_stationary_detailed TO od_extract_svc;
GRANT SELECT ON TABLE open_data.wys_stationary_detailed TO bdit_humans;
