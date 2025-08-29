--DROP VIEW vds.detector_inventory;
CREATE OR REPLACE VIEW vds.detector_inventory AS (
    SELECT DISTINCT ON (pairs.vdsconfig_uid, pairs.entity_location_uid, pairs.division_id)
        row_number() OVER () AS row_number, --uid to make this view mappable in QGIS
        pairs.vdsconfig_uid,
        pairs.entity_location_uid,
        pairs.division_id,
        c.detector_id,
        pairs.first_active,
        pairs.last_active,
        upper(e.main_road_name::text)
        || COALESCE(' and '::text || upper(e.cross_road_name::text), ''::text) AS detector_loc,
        e.geom AS sensor_geom,
        cl_vds.centreline_id,
        cl.geom AS centreline_geom,
        dtypes.det_type,
        CASE dtypes.det_type = 'RESCU Detectors'::text
            WHEN TRUE THEN CASE substring(substring(c.detector_id::text, 'D\w{8}'::text), 2, 1)
                WHEN 'N' THEN 'DVP/Allen North' --North of Don Mills 
                WHEN 'S' THEN 'DVP South' --South of Don Mills 
                WHEN 'E' THEN 'Gardiner/Lakeshore East' --East of Yonge
                WHEN 'W' THEN 'Gardiner/Lakeshore West' --West of Yonge
                WHEN 'K'::text THEN 'Kingston Rd'::text
                ELSE NULL::text
            END
            ELSE NULL::text
        END AS det_loc,
        CASE dtypes.det_type = 'RESCU Detectors'::text
            WHEN true THEN
            CASE substring(substring(c.detector_id::text, 'D\w{8}'::text), 9, 1)
                WHEN 'D'::text THEN 'DVP'::text
                WHEN 'L'::text THEN 'Lakeshore'::text
                WHEN 'G'::text THEN 'Gardiner'::text
                WHEN 'A'::text THEN 'Allen'::text
                WHEN 'K'::text THEN 'Kingston Rd'::text
                WHEN 'R'::text THEN 'On-Ramp'::text
                ELSE NULL::text
            END
            ELSE NULL::text
        END AS det_group,
        CASE dtypes.det_type = 'RESCU Detectors'::text
            WHEN true THEN
            CASE substring(substring(c.detector_id::text, 'D\w{8}'::text), 8, 1)
                WHEN 'E'::text THEN 'Eastbound'::text
                WHEN 'W'::text THEN 'Westbound'::text
                WHEN 'S'::text THEN 'Southbound'::text
                WHEN 'N'::text THEN 'Northbound'::text
                ELSE NULL::text
            END
            ELSE NULL::text
        END AS direction,
        --new cases need to be updated manually and then updated in vds.count_15min%.
        CASE
            WHEN c.division_id = 8001 THEN 1 --15 min bins
            --remainder are division_id = 2
            WHEN
                dtypes.det_type = 'Smartmicro Sensors'::text
                OR c.detector_id::text ~ similar_to_escape('SMARTMICRO - D\w{8}'::text) THEN 3 --5 min bins
            WHEN
                lower(comms.source_id::text) ~ similar_to_escape('%whd%'::text)
                OR lower(comms.source_id::text) ~ similar_to_escape('%wavetronix%'::text) THEN 45 --20 sec bins
            WHEN dtypes.det_type = 'RESCU Detectors'::text THEN 45 --20 sec bins
            WHEN dtypes.det_type = 'Blue City AI'::text THEN 1 --15 min bins
            WHEN dtypes.det_type = 'Houston Radar'::text THEN 1 --15 min bins
            --some unique cases
            WHEN pairs.vdsconfig_uid = ANY (ARRAY[5621308, 5621332]) THEN 30
            WHEN pairs.vdsconfig_uid = ANY (ARRAY[3683400, 3683403]) THEN 3
            ELSE NULL::integer
        END AS expected_bins,
        comms.source_id AS comms_desc,
        --rescu techology type, determined by communication device in some cases.
        CASE
            WHEN
                lower(comms.source_id::text) ~ similar_to_escape('%smartmicro%'::text)
                OR lower(c.detector_id::text) ~ similar_to_escape('%smartmicro%'::text) THEN 'Smartmicro'::text
            WHEN
                lower(comms.source_id::text) ~ similar_to_escape('%whd%'::text)
                OR lower(comms.source_id::text) ~ similar_to_escape('%wavetronix%'::text) THEN 'Wavetronix'::text
            WHEN lower(c.detector_id::text) ~ similar_to_escape('%(whalespout)|(houstonradar)%'::text) THEN 'Radar'::text
            WHEN dtypes.det_type = 'RESCU Detectors'::text THEN 'Inductive'::text
            ELSE NULL::text
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
        AND tsrange(c.start_timestamp, COALESCE(c.end_timestamp, now()::timestamp without time zone))
        && tsrange(comms.start_timestamp, COALESCE(comms.end_timestamp, now()::timestamp without time zone)),
        LATERAL (
            SELECT CASE
                WHEN c.detector_id::text ~~ 'QWS%'::text
                    THEN 'Smartmicro Sensors'::text
                WHEN
                    c.division_id = 2 AND (c.detector_id::text ~ similar_to_escape('D\w{8}%'::text)
                    --smartmicro installed in place of old rescu sensors on highways
                    OR c.detector_id::text ~ similar_to_escape('SMARTMICRO - D\w{8}'::text))
                    THEN 'RESCU Detectors'::text
                WHEN
                    c.detector_id::text ~ similar_to_escape('PX[0-9]{4}-DET%'::text)
                    AND c.division_id = 8001
                    THEN 'Signal Detectors'::text
                WHEN
                    c.detector_id::text ~ similar_to_escape('PX[0-9]{4}-SF%'::text)
                    AND c.division_id = 8001
                    THEN 'Signal Special Function'::text
                WHEN
                    c.detector_id::text ~ similar_to_escape('PX[0-9]{4}-PE%'::text) AND c.division_id = 8001
                    THEN 'Signal Preemption'::text
                WHEN
                    (c.detector_id::text ~~ 'BCT%'::text OR c.detector_id::text ~~ 'OBC%'::text)
                    THEN 'Blue City AI'::text
                WHEN
                    lower(comms.source_id::text) ~~ '%wavetronix%'::text THEN 'Wavetronix'::text
                WHEN
                    lower(c.detector_id::text) ~ similar_to_escape('%(whalespout)|(houstonradar)%'::text)
                    THEN 'Houston Radar'::text
                WHEN
                    (c.detector_id::text ~~ ANY ('{%SMARTMICRO%,"%YONGE HEATH%","%YONGE DAVISVILLE%","%YONGE AND ROXBOROUGH%"}'::text[]))
                    --new lakeshore/spadina smartmicro sensors
                    OR (c.vds_id = ANY (ARRAY[6949838, 6949843, 6949845, 7030552, 7030554, 7030564, 7030575, 7030577, 7030578, 7030591]))
                    --new lakeshore smartmicro sensors
                    OR c.vds_id >= 7011490 AND c.vds_id <= 7011519
                    THEN 'Smartmicro Sensors'::text
                ELSE NULL::text
            END AS det_type
        ) AS dtypes
    ORDER BY
        pairs.vdsconfig_uid ASC,
        pairs.entity_location_uid ASC,
        pairs.division_id ASC,
        comms.start_timestamp DESC --most recently installed comms
);

COMMENT ON VIEW vds.detector_inventory IS 'Centralize information about
expected bin width for each detector to be used in converting vehicle per hour to 
vehicle count. May need to periodically update `expected_bins` column using
bdit_data-sources/volumes/vds/exploration/time_gaps.sql';

ALTER VIEW vds.detector_inventory OWNER TO vds_admins;
GRANT SELECT ON vds.detector_inventory TO bdit_humans;
GRANT SELECT ON vds.detector_inventory TO vds_bot;
