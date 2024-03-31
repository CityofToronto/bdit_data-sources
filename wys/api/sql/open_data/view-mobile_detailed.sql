-- DROP VIEW open_data.wys_mobile_detailed;

CREATE OR REPLACE VIEW open_data.wys_mobile_detailed
AS
SELECT 
    loc.location_id,
    loc.ward_no,
    loc.location,
    loc.from_street,
    loc.to_street,
    loc.direction,
    agg.datetime_bin,
    bins.speed_bin::text AS speed_bin,
    agg.volume
FROM wys.mobile_api_id AS loc
--include details only for signs included in summary view. 
JOIN open_data.wys_mobile_summary USING (location_id)
JOIN wys.speed_counts_agg_5kph AS agg ON 
    loc.api_id = agg.api_id 
    AND agg.datetime_bin > loc.installation_date 
    AND (
        loc.removal_date IS NULL
        OR agg.datetime_bin < loc.removal_date
    )
JOIN wys.speed_bins_old AS bins USING (speed_id)
--mobile_summary now has still active signs, so we need to restrict
--which data is included for those signs here.
WHERE agg.datetime_bin < date_trunc('month', now());

ALTER TABLE open_data.wys_mobile_detailed OWNER TO wys_admins;

GRANT SELECT ON TABLE open_data.wys_mobile_detailed TO od_extract_svc;
GRANT SELECT ON TABLE open_data.wys_mobile_detailed TO bdit_humans;
COMMENT ON VIEW open_data.wys_mobile_detailed IS 'Semi-aggregated Watch Your Speed data from wys.speed_counts_agg_5kph for signs in the mobile WYS program';