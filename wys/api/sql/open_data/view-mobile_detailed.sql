-- View: open_data.wys_mobile_detailed_2019

-- DROP VIEW open_data.wys_mobile_detailed_2019;

CREATE OR REPLACE VIEW open_data.wys_mobile_detailed_2019
 AS
 SELECT loc.ward_no,
    loc.location,
    loc.from_street,
    loc.to_street,
    loc.direction,
    agg.datetime_bin,
    speed_bins.speed_bin::text AS speed_bin,
    agg.volume
   FROM wys.mobile_api_id loc
     JOIN wys.speed_counts_agg agg ON loc.api_id = agg.api_id AND agg.datetime_bin >= loc.installation_date AND agg.datetime_bin < loc.removal_date
     JOIN wys.speed_bins USING (speed_id)
  WHERE agg.datetime_bin >= '2019-01-01 00:00:00'::timestamp without time zone AND agg.datetime_bin < ('2019-01-01'::date + '1 year'::interval);

ALTER TABLE open_data.wys_mobile_detailed_2019
    OWNER TO rdumas;

GRANT SELECT ON TABLE open_data.wys_mobile_detailed_2019 TO od_extract_svc;
GRANT ALL ON TABLE open_data.wys_mobile_detailed_2019 TO rdumas;
GRANT SELECT ON TABLE open_data.wys_mobile_detailed_2019 TO bdit_humans;