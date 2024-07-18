DROP VIEW ckousin.vds_dashboard_data;
CREATE OR REPLACE VIEW ckousin.vds_dashboard_data AS

WITH last_active AS (
    SELECT
        detector_id,
        MIN(first_active) AS first_active,
        MAX(last_active) AS last_active
    FROM vds.detector_inventory
    WHERE division_id = 2
    GROUP BY detector_id
)

SELECT DISTINCT ON (detector_id)
    di.detector_id AS unique_id,
    di.det_type AS device_family,
    CASE 
        WHEN di.det_type = 'Blue City AI' THEN 'LIDAR'
        WHEN di.det_type = 'Smartmicro Sensors' OR det_tech = 'Wavetronix' THEN 'Radar'
        ELSE COALESCE(di.det_tech, di.det_type) END AS det_tech,
        CASE
            WHEN la.last_active >= now()::date - interval '2 day' THEN 'Online'::text
            WHEN la.last_active >= now()::date - interval '14 day' THEN 'Not Reporting'::text
            --decommissioned? from chris's list?
            WHEN la.last_active < now()::date - interval '14 day' THEN 'Offline'::text
            ELSE 'Unknown'::text
        END AS status,
    di.detector_loc AS location_name,
    di.sensor_geom AS geom,
    di.centreline_id,
    la.first_active::date AS date_installed,
    la.last_active::date,
    null::date AS date_decommissioned
FROM vds.detector_inventory AS di
JOIN last_active AS la USING (detector_id)
WHERE di.division_id = 2
ORDER BY di.detector_id, la.last_active DESC;

ALTER TABLE ckousin.vds_dashboard_data OWNER TO ckousin;

GRANT SELECT ON TABLE ckousin.vds_dashboard_data TO bdit_humans;
GRANT ALL ON TABLE ckousin.vds_dashboard_data TO ckousin;