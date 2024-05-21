DROP MATERIALIZED VIEW vds.vds_inventory CASCADE;
CREATE MATERIALIZED VIEW vds.vds_inventory AS (

    SELECT
        c15.vdsconfig_uid,
        c15.entity_location_uid,
        v.detector_id,
        UPPER(e.main_road_name) || ' and ' || UPPER(e.cross_road_name) AS detector_loc,
        e.geom AS sensor_geom,
        cl_vds.centreline_id,
        cl.geom AS centreline_geom,
        MIN(c15.datetime_15min) AS first_active,
        MAX(c15.datetime_15min) AS last_active,
        di.det_type,
        di.det_loc,
        di.det_group,
        di.direction,
        di.expected_bins,
        di.comms_desc,
        di.det_tech
    FROM vds.counts_15min AS c15
    LEFT JOIN vds.entity_locations AS e
        ON e.uid = c15.entity_location_uid
        AND e.division_id = c15.division_id
    LEFT JOIN vds.vdsconfig AS v
        ON v.uid = c15.vdsconfig_uid
        AND v.division_id = c15.division_id
    LEFT JOIN vds.centreline_vds AS cl_vds ON cl_vds.vdsconfig_uid = c15.vdsconfig_uid
    LEFT JOIN gis_core.centreline_latest AS cl USING (centreline_id)
    LEFT JOIN vds.detector_inventory AS di ON v.uid = di.uid
    WHERE c15.division_id = 2
    GROUP BY
        c15.vdsconfig_uid,
        c15.entity_location_uid,
        v.detector_id,
        e.geom,
        cl_vds.centreline_id,
        cl.geom,
        detector_loc,
        di.det_type,
        di.det_loc,
        di.det_group,
        di.direction,
        di.expected_bins,
        di.comms_desc,
        di.det_tech
);

ALTER TABLE IF EXISTS vds.vds_inventory OWNER TO vds_admins;

GRANT SELECT ON vds.vds_inventory TO bdit_humans;