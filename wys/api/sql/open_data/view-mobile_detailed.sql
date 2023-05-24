
-- DROP VIEW open_data.wys_mobile_detailed;

CREATE OR REPLACE VIEW open_data.wys_mobile_detailed
  AS
SELECT 
    location_id,
    loc.ward_no,
    loc.location,
    loc.from_street,
    loc.to_street,
    loc.direction,
    agg.datetime_bin,
    bins.speed_bin::text AS speed_bin,
    agg.volume
FROM wys.mobile_api_id loc
INNER JOIN open_data.wys_mobile_summary USING (location_id)
JOIN wys.speed_counts_agg_5kph agg ON 
    loc.api_id = agg.api_id 
    AND agg.datetime_bin > loc.installation_date 
    AND agg.datetime_bin < loc.removal_date
JOIN wys.speed_bins_old AS bins USING (speed_id)
WHERE loc.removal_date < date_trunc('month', now());

ALTER TABLE open_data.wys_mobile_detailed
    OWNER TO rdumas;

GRANT SELECT ON TABLE open_data.wys_mobile_detailed TO od_extract_svc;
GRANT ALL ON TABLE open_data.wys_mobile_detailed TO rdumas;
GRANT SELECT ON TABLE open_data.wys_mobile_detailed TO bdit_humans;