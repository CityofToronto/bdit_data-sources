-- DROP FUNCTION vds.insert_detector_inventory_rows;

CREATE OR REPLACE FUNCTION vds.insert_detector_inventory_rows()
RETURNS TABLE (
    row_id integer,
    detector_id character varying,
    detector_loc text,
    division_id smallint
) AS $$
BEGIN
    RETURN QUERY
    INSERT INTO gwolofs.detector_inventory AS di (
        vdsconfig_uid, entity_location_uid, division_id, detector_id,
        first_active, last_active, detector_loc, sensor_geom, centreline_id,
        centreline_geom, det_type, det_loc, det_group, direction,
        expected_bins, comms_desc, det_tech
    )

    SELECT DISTINCT ON (
        pairs.vdsconfig_uid, pairs.entity_location_uid, pairs.division_id
    )
        pairs.vdsconfig_uid,
        pairs.entity_location_uid,
        pairs.division_id,
        c.detector_id,
        pairs.first_active,
        pairs.last_active,
        upper(e.main_road_name)
        || COALESCE(' and ' || upper(e.cross_road_name), '') AS detector_loc,
        e.geom AS sensor_geom,
        cl_vds.centreline_id,
        cl.geom AS centreline_geom,
        dtypes.det_type,
        CASE dtypes.det_type = 'RESCU Detectors'
                WHEN TRUE THEN
                CASE substring(substring(upper(c.detector_id), '(?=.*\d)D\w[^_\s]{7}'), 2, 1)
                    WHEN 'N' THEN 'DVP/Allen North' --North of Don Mills 
                    WHEN 'S' THEN 'DVP South' --South of Don Mills 
                    WHEN 'E' THEN 'Gardiner/Lakeshore East' --East of Yonge
                    WHEN 'W' THEN 'Gardiner/Lakeshore West' --West of Yonge
                    WHEN 'K' THEN 'Kingston Rd'
                END
            END AS det_loc,
            CASE dtypes.det_type = 'RESCU Detectors'
                WHEN true THEN
                CASE substring(substring(upper(c.detector_id), '(?=.*\d)D\w[^_\s]{7}'), 9, 1)
                    WHEN 'D' THEN 'DVP'
                    WHEN 'L' THEN 'Lakeshore'
                    WHEN 'G' THEN 'Gardiner'
                    WHEN 'A' THEN 'Allen'
                    WHEN 'K' THEN 'Kingston Rd'
                    WHEN 'R' THEN 'On-Ramp'
                END
            END AS det_group,
            CASE dtypes.det_type = 'RESCU Detectors'
                WHEN true THEN
                CASE substring(substring(upper(c.detector_id), '(?=.*\d)D\w[^_\s]{7}'), 8, 1)
                    WHEN 'E' THEN 'Eastbound'
                    WHEN 'W' THEN 'Westbound'
                    WHEN 'S' THEN 'Southbound'
                    WHEN 'N' THEN 'Northbound'
                END
            END AS direction,
            --new cases need to be updated manually and then updated in vds.count_15min%.
            CASE
                WHEN c.division_id = 8001 THEN 1 --15 min bins
                --remainder are division_id = 2
                WHEN
                    dtypes.det_type = 'Smartmicro Sensors'
                    OR lower(c.detector_id) SIMILAR TO 'smartmicro - d\w{8}' THEN 3 --5 min bins
                WHEN
                    comms.source_id ILIKE '%whd%'
                    OR comms.source_id ILIKE '%wavetronix%' THEN 45 --20 sec bins
                WHEN dtypes.det_type = 'RESCU Detectors' THEN 45 --20 sec bins
                WHEN dtypes.det_type = 'Blue City AI' THEN 1 --15 min bins
                WHEN comms.source_id ILIKE '%houston radar%' THEN 30 --30 sec bins
                --some unique cases
                WHEN pairs.vdsconfig_uid = ANY (ARRAY[3683400, 3683403]) THEN 3
            END AS expected_bins,
            comms.source_id AS comms_desc,
            --rescu techology type, determined by communication device in some cases.
            CASE
                WHEN
                    comms.source_id ILIKE '%smartmicro%'
                    OR c.detector_id ILIKE '%smartmicro%' THEN 'Smartmicro'
                WHEN
                    comms.source_id ILIKE '%whd%'
                    OR comms.source_id ILIKE '%wavetronix%' THEN 'Wavetronix'
                WHEN comms.source_id ILIKE '%houston radar%' THEN 'Radar'
                WHEN dtypes.det_type = 'RESCU Detectors' THEN 'Inductive'
            END AS det_tech
    FROM vds.last_active AS pairs
    LEFT JOIN vds.vdsconfig AS c
        ON pairs.vdsconfig_uid = c.uid
    LEFT JOIN vds.entity_locations AS e
        ON pairs.entity_location_uid = e.uid
    LEFT JOIN vds.centreline_vds AS cl_vds
        ON cl_vds.vdsconfig_uid = pairs.vdsconfig_uid
    LEFT JOIN gis_core.centreline_latest AS cl USING (centreline_id)
    LEFT JOIN vds.config_comms_device AS comms
        ON comms.fss_id = c.fss_id
        AND comms.division_id = c.division_id
        AND tsrange(c.start_timestamp, COALESCE(c.end_timestamp, now()::timestamp))
        && tsrange(comms.start_timestamp, COALESCE(comms.end_timestamp, now()::timestamp)),
            LATERAL (
                SELECT CASE
                    WHEN c.detector_id ILIKE '%QWS%'
                        THEN 'Smartmicro Sensors'
                    WHEN
                        c.division_id = 2 AND lower(c.detector_id) SIMILAR TO 'd\w{8}%'
                        --smartmicro installed in place of old rescu sensors on highways
                        OR lower(c.detector_id) SIMILAR TO 'smartmicro - d\w{8}'
                        THEN 'RESCU Detectors'
                    WHEN
                        lower(c.detector_id) SIMILAR TO 'px[0-9]{4}-det%'
                        AND c.division_id = 8001
                        THEN 'Signal Detectors'
                    WHEN
                        lower(c.detector_id) SIMILAR TO 'px[0-9]{4}-sf%'
                        AND c.division_id = 8001
                        THEN 'Signal Special Function'
                    WHEN
                        lower(c.detector_id) SIMILAR TO 'px[0-9]{4}-pe%' AND c.division_id = 8001
                        THEN 'Signal Preemption'
                    WHEN
                        (c.detector_id ILIKE 'BCT%' OR c.detector_id ILIKE 'OBC%')
                        THEN 'Blue City AI'
                    WHEN
                        comms.source_id ILIKE '%wavetronix%' THEN 'Wavetronix'
                    WHEN
                        comms.source_id ILIKE 'houston radar%' THEN 'Houston Radar'
                    --note there is also a smartmicro categorization above -> Rescu Detectors
                    WHEN comms.source_id ILIKE '%smartmicro%' THEN 'Smartmicro Sensors'
                END AS det_type
            ) AS dtypes
    ORDER BY
        pairs.vdsconfig_uid ASC,
        pairs.entity_location_uid ASC,
        pairs.division_id ASC,
        comms.start_timestamp DESC --most recently installed comms
    ON CONFLICT
    ON CONSTRAINT detector_inventory_vdsconfig_uid_entity_location_uid_divisi_key
    --so manual updates to table don't get overridden
    DO NOTHING
    RETURNING di.row_id, di.detector_id, di.detector_loc, di.division_id;

END;
$$
LANGUAGE plpgsql
SECURITY DEFINER;

ALTER FUNCTION vds.insert_detector_inventory_rows OWNER TO vds_admins;
GRANT EXECUTE ON FUNCTION vds.insert_detector_inventory_rows TO vds_bot;
