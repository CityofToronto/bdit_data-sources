-- View: ckousin.mio_dashboard_data

DROP VIEW IF EXISTS ckousin.mio_dashboard_data;

/*
SELECT *
FROM miovision_api.volumes_daily_unfiltered
WHERE dt = '2024-07-17'
*/

--alerts or anomalous_range = malfunctioning
--has data = online
--offline = zero data (> 2 weeks)
--date decommissioned = date decommissioned 

CREATE OR REPLACE VIEW ckousin.mio_dashboard_data
 AS
 SELECT
     i.id AS unique_id,
    'Miovision'::text AS device_family,
    'Online'::text AS status,
    i.api_name AS location_name,
    i.geom,
    'Video' AS det_tech,
    i.date_installed,
    i.date_decommissioned,
    i.px,
    i.int_id
   FROM miovision_api.intersections i;

ALTER TABLE ckousin.mio_dashboard_data
    OWNER TO ckousin;

GRANT SELECT ON TABLE ckousin.mio_dashboard_data TO bdit_humans;
GRANT ALL ON TABLE ckousin.mio_dashboard_data TO ckousin;

