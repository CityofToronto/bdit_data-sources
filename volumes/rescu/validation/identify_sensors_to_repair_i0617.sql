--this query can be used to identify detectors for repair requests.
--recommend opening in qgis and styling based on 'classify'. Look for geographical gaps with
--bad/inactive sensors. 
--designate a set of `time_bins` (ie. current year) to get exact % active bins 
--includes inactive sensors that don't have any records during the designated `time_bins`
--runs in 20s for 2023-01--2023-05

DROP TABLE gwolofs.i0617_rescu_sensor_eval;

CREATE TABLE gwolofs.i0617_rescu_sensor_eval (
    detector_id text,
    number_of_lanes int,
    primary_road text,
    cross_road text,
    avg_volume numeric,
    total_volume bigint,
    bins_active_percent numeric,
    bins_active bigint,
    last_active timestamp,
    geom geometry,
    classify text
);

INSERT INTO gwolofs.i0617_rescu_sensor_eval (
    detector_id,
    number_of_lanes,
    primary_road,
    cross_road,
    avg_volume,
    total_volume,
    bins_active_percent,
    bins_active,
    last_active,
    geom,
    classify
)

--select all time bins in order to get a true % active count
WITH time_bins AS (
    SELECT generate_series(
        '2023-01-01 00:00'::timestamp,
        '2023-05-15 14:30',
        '15 minutes'
    ) AS time_bin
),

--exclude network outages from time bins so not to penalize all detectors for network issue. 
tbins_omit_outages AS (
    SELECT tb.time_bin
    FROM time_bins AS tb
    LEFT JOIN gwolofs.network_outages AS ro ON ro.time_range @> tb.time_bin
    WHERE ro.time_range IS NULL
),

--determine volume by detectors
sensor_volumes AS (
    SELECT
        v1.detector_id,
        AVG(v1.volume_15min) AS avg_volume,
        SUM(v1.volume_15min) AS total_volume,
        ROUND(
            (COUNT(DISTINCT v1.datetime_bin)::decimal / (SELECT COUNT(*) FROM tbins_omit_outages)),
            3
        ) AS bins_active_percent,
        COUNT(DISTINCT v1.datetime_bin) AS bins_active,
        MAX(v1.datetime_bin) AS last_active
    FROM tbins_omit_outages AS tboo
    LEFT JOIN rescu.volumes_15min AS v1 ON v1.datetime_bin = tboo.time_bin
    GROUP BY 1
    ORDER BY 3
),

--no volumes during time_bins period. Get last active date from volumes.
inactive_sensors AS (
    SELECT
        v1.detector_id,
        MAX(v1.datetime_bin) AS last_active
    FROM rescu.volumes_15min AS v1
    LEFT JOIN sensor_volumes AS sv ON v1.detector_id = sv.detector_id
    WHERE sv.detector_id IS NULL
    GROUP BY 1
)

--active detectors during period 'time_bins'
SELECT
    di.detector_id,
    di.number_of_lanes,
    di.primary_road,
    di.cross_road,
    sv.avg_volume,
    sv.total_volume,
    sv.bins_active_percent,
    sv.bins_active,
    sv.last_active,
    ST_SetSRID(ST_MakePoint(di.longitude, di.latitude), 4326) AS geom,
    CASE
        WHEN
            sv.last_active >= '2023-05-15'
            AND sv.bins_active_percent >= 0.6 THEN 'good'
        WHEN
            sv.last_active < '2023-05-15' --the most recent day with data
            OR sv.bins_active_percent < 0.6 THEN 'bad' --<60% of days have data
    END AS classify
FROM sensor_volumes AS sv
JOIN rescu.detector_inventory AS di ON di.detector_id = sv.detector_id
WHERE di.primary_road NOT LIKE 'RAMP%'

UNION

--inactive detectors (not active during time_bins)
SELECT
    di.detector_id,
    di.number_of_lanes,
    di.primary_road,
    di.cross_road,
    NULL::numeric AS avg_volume,
    NULL::numeric AS total_volume,
    NULL::numeric AS bins_active_percent,
    NULL::numeric AS bins_active,
    inactive.last_active,
    ST_SetSRID(ST_MakePoint(di.longitude, di.latitude), 4326) AS geom,
    'inactive' AS classify
FROM inactive_sensors AS inactive
LEFT JOIN rescu.detector_inventory AS di USING (detector_id)
WHERE di.primary_road NOT LIKE 'RAMP%'
ORDER BY di.detector_id;