--find the arterycodes for rescu sensors
SELECT
    arterydata.stat_code,
    arterydata.street1,
    arterydata.street2,
    array_agg(DISTINCT arterydata.count_type) AS count_types,
    array_agg(DISTINCT category.category_id) AS category_ids,
    array_agg(DISTINCT category.category_name) AS category_name,
    MIN(countinfo.count_date) AS min_count_date,
    MAX(countinfo.count_date) AS max_count_date,
    array_agg(DISTINCT countinfo.speed_info_id) AS speed_info_ids
FROM traffic.arterydata
JOIN traffic.countinfo USING (arterycode)
JOIN traffic.category USING (category_id)
WHERE
    arterydata.stat_code SIMILAR TO '%D\w{8}%'
    AND countinfo.category_id IN (1, 2) --{"24 HOUR",RESCU}. Exclues "speed" and "class".
GROUP BY
    arterydata.stat_code,
    arterydata.street1,
    arterydata.street2
ORDER BY
    arterydata.stat_code ASC;


--30 VDS sensors that do not exist in ITS Central (at least for that time period)
--184 are pre-2007 > 15 years old. 1 is a ramp that goes up to 2014.
--add entries for these into vdsconfig and entity_locations tables.
--DROP TABLE gwolofs.old_rescu_sensors;
CREATE TABLE gwolofs.old_rescu_sensors AS (
    SELECT
        stat_code AS detector_id,
        MIN(datetime_15min) AS start_timestamp,
        MAX(datetime_15min) AS end_timestamp
    FROM (
        SELECT
            arterydata.stat_code,
            dt.datetime_15min,
            cnt_det.count AS count_15min
        FROM traffic.arterydata
        JOIN traffic.countinfo USING (arterycode)
        JOIN traffic.cnt_det USING (count_info_id)
        --anti join
        LEFT JOIN vds.vdsconfig AS v
            ON arterydata.stat_code = substring(v.detector_id, 'D\w{8}')
            AND countinfo.count_date + cnt_det.timecount::time >= v.start_timestamp
            AND (
                countinfo.count_date + cnt_det.timecount::time < v.end_timestamp
                OR v.end_timestamp IS NULL --no end date
            ),
            LATERAL (
                SELECT countinfo.count_date + cnt_det.timecount::time AS datetime_15min
            ) AS dt
        WHERE
            countinfo.count_date >= '1993-01-01'::date --there is a lot of bogus data from 1899
            AND countinfo.count_date < '2021-11-01'::date --data beyond 2021 is available in VDS
            AND arterydata.stat_code SIMILAR TO '%D\w{8}%'
            AND countinfo.category_id IN (1, 2) --{"24 HOUR",RESCU}. Exclues "speed" and "class".
            AND cnt_det.count >= 0 --a few negative 1s are present
            AND v.uid IS NULL
    ) AS missing
    GROUP BY stat_code
);
   
--insert these 
INSERT INTO vds.vdsconfig (
    detector_id, start_timestamp, end_timestamp, division_id, vds_id
)
SELECT
    detector_id,
    start_timestamp,
    end_timestamp,
    2 AS division_id,
    --(SELECT MAX(vds_id) + 10000000 FROM vds.vdsconfig) == 18868730
    dense_rank() OVER (ORDER BY detector_id) + 18868730 AS vds_id
FROM gwolofs.old_rescu_sensors;

INSERT INTO vds.entity_locations (
    start_timestamp, end_timestamp, division_id, entity_id
)
SELECT
    start_timestamp,
    end_timestamp,
    2 AS division_id,
    --offset the new id by 10M from the current max
    --(SELECT MAX(vds_id) + 10000000 FROM vds.vdsconfig) == 18868730
    dense_rank() OVER (ORDER BY detector_id) + 18868730 AS vds_id
FROM gwolofs.old_rescu_sensors;

--add new sensors to centreline_vds table where match exists in centreline_latest.
--INSERT 0 29
INSERT INTO vds.centreline_vds (centreline_id, vdsconfig_uid)
SELECT centreline_id, vdsconfig.uid
FROM gwolofs.old_rescu_sensors AS ors
LEFT JOIN traffic.arterydata ON arterydata.stat_code = ors.detector_id
LEFT JOIN traffic.arteries_centreline USING (arterycode)
--only 4 don't match in centreline_latest, acceptable.
--JOIN gis_core.centreline_latest
--ON centreline_latest.centreline_id = arteries_centreline.centreline_id   
LEFT JOIN vds.vdsconfig USING (detector_id, start_timestamp, end_timestamp)
WHERE centreline_id IS NOT NULL

    
--SELECT 56726480
--Query returned successfully in 4 min 45 secs.

DROP TABLE IF EXISTS gwolofs.old_rescu_staging;
CREATE TABLE gwolofs.old_rescu_staging AS (
    WITH distinct_data AS (
        --there are duplicates representing about .4% of total row count.
        --Decided to take higher count - not much impact either way.
        SELECT DISTINCT ON (arterydata.stat_code, datetime_15min_rounded)
            arterydata.stat_code,
            dt.datetime_15min,
            --the data is generally in 15 minute bins, but some of them do not fall
            --exactly on the 15. Round (up/down).
            timestamp without time zone 'epoch' + interval '1 second'
            * (ROUND(extract('epoch' FROM dt.datetime_15min) / 900.0) * 900)
            AS datetime_15min_rounded,
            cnt_det.count AS count_15min
        FROM traffic.arterydata
        JOIN traffic.countinfo USING (arterycode)
        JOIN traffic.cnt_det USING (count_info_id),
            LATERAL (
                SELECT countinfo.count_date + cnt_det.timecount::time AS datetime_15min
            ) AS dt
        WHERE
            countinfo.count_date >= '1993-01-01'::date
            --data from 2021-11 onwards is available in ITS Central
            AND countinfo.count_date < '2021-11-01'::date
            AND arterydata.stat_code SIMILAR TO '%D\w{8}%'
            AND countinfo.category_id IN (1, 2) --{"24 HOUR",RESCU}. Exclues "speed", "class".
            AND cnt_det.count >= 0 --a few negative 1s are present
        ORDER BY
            arterydata.stat_code ASC,
            datetime_15min_rounded ASC,
            cnt_det.count DESC
    )

    SELECT
        dd.stat_code,
        v.division_id,
        v.uid AS vdsconfig_uid,
        e.uid AS entity_location_uid,
        v.vds_id,
        dd.datetime_15min,
        --round up or down.
        dd.datetime_15min_rounded,
        dd.count_15min,
        v.lanes AS num_lanes,
        di.expected_bins
    FROM distinct_data AS dd
    --added entries for the missing sensors via `gwolofs.old_rescu_sensors` above.
    JOIN vds.vdsconfig AS v
        --the allen ones don't match otherwise
        ON dd.stat_code = substring(v.detector_id, 'D\w{8}')
        AND dd.datetime_15min_rounded >= v.start_timestamp
        AND (
            dd.datetime_15min_rounded < v.end_timestamp
            OR v.end_timestamp IS NULL --no end date
        )
    JOIN vds.entity_locations AS e
        ON e.entity_id = v.vds_id
        AND dd.datetime_15min_rounded >= e.start_timestamp
        AND (
            dd.datetime_15min_rounded < e.end_timestamp
            OR e.end_timestamp IS NULL --no end date
        )
    LEFT JOIN vds.detector_inventory AS di
        ON di.vdsconfig_uid = v.uid
        AND di.entity_location_uid = e.uid
    ORDER BY v.vds_id, dd.datetime_15min_rounded
);

--add constraints to make sure data will insert smoothly
ALTER TABLE gwolofs.old_rescu_staging
ADD CONSTRAINT counts_15min_partitioned_pkey PRIMARY KEY (
    division_id, vdsconfig_uid, datetime_15min_rounded
);

CREATE INDEX IF NOT EXISTS counts_15min_div2_datetime_15min_idx
ON gwolofs.old_rescu_staging USING brin (datetime_15min_rounded);

CREATE INDEX IF NOT EXISTS counts_15min_div2_vdsconfig_uid_datetime_15min_idx
ON gwolofs.old_rescu_staging USING btree (
    vdsconfig_uid ASC NULLS LAST, datetime_15min ASC NULLS LAST
);


--compare the new and old data during overlapping period! (count_15min vs vds_counts_15min_count)
--SELECT 175336
DROP TABLE IF EXISTS gwolofs.old_rescu_compare;
CREATE TABLE gwolofs.old_rescu_compare AS (
    WITH ors AS (
        SELECT
            vdsconfig_uid,
            datetime_15min_rounded::date AS dt,
            SUM(count_15min) AS flow_sum
        FROM gwolofs.old_rescu_staging
        WHERE
            datetime_15min_rounded >= '2017-01-01'
            AND datetime_15min_rounded < '2021-11-01'
        GROUP BY
            vdsconfig_uid,
            datetime_15min_rounded::date
    ),

    c15 AS (
        SELECT
            vdsconfig_uid,
            datetime_15min::date AS dt,
            SUM(count_15min) AS vds_sum
        FROM vds.counts_15min_div2
        WHERE
            datetime_15min >= '2017-01-01'
            AND datetime_15min < '2021-11-01'
            AND vdsconfig_uid IN (
                SELECT DISTINCT vdsconfig_uid FROM ors
            )
        GROUP BY
            vdsconfig_uid,
            dt
    )

    SELECT
        COALESCE(ors.vdsconfig_uid, c15.vdsconfig_uid) AS vdsconfig_uid,
        COALESCE(ors.dt, c15.dt) AS dt,
        ors.flow_sum,
        c15.vds_sum
    FROM ors
    FULL JOIN c15 USING (vdsconfig_uid, dt)
);

--where both have volumes, >99% are within 1%. 
SELECT
    --vdsconfig_uid,
    COUNT(*) FILTER (WHERE flow_sum / vds_sum >= '0.99' AND flow_sum / vds_sum < '1.01')
    AS within_1percent, --77,949
    COUNT(*) FILTER (WHERE NOT (flow_sum / vds_sum >= '0.99' AND flow_sum / vds_sum < '1.01'))
    AS outside_1percent, --373
    COUNT(*) FILTER (WHERE flow_sum IS NULL AND vds_sum IS NOT NULL) AS flow_missing, --91,157
    COUNT(*) FILTER (WHERE vds_sum IS NULL AND flow_sum IS NOT NULL) AS vds_missing, --5,867
    COUNT(*) AS total_count --175,336
FROM gwolofs.old_rescu_compare
--GROUP BY vdsconfig_uid

--create new partitions:
SELECT vds.partition_vds_yyyy(
	base_table := 'counts_15min_div2', 
	year_ := yr
)
FROM generate_series(1993, 2016) AS years(yr);

--for data 2017-2021, let's only fill in the sensors x days that
--don't currently have data, but do in old FLOW data.
--INSERT 0 559356
--Query returned successfully in 2 min 23 secs.
INSERT INTO vds.counts_15min (
    division_id, vdsconfig_uid, entity_location_uid,
    num_lanes, datetime_15min, count_15min, expected_bins
)
SELECT
    ors.division_id,
    ors.vdsconfig_uid,
    ors.entity_location_uid,
    ors.num_lanes,
    ors.datetime_15min_rounded,
    ors.count_15min,
    ors.expected_bins
FROM gwolofs.old_rescu_compare AS orc
JOIN gwolofs.old_rescu_staging AS ors
    ON ors.vdsconfig_uid = orc.vdsconfig_uid
    AND ors.datetime_15min_rounded >= orc.dt
    AND ors.datetime_15min_rounded < orc.dt + interval '1 day'
WHERE
    orc.flow_sum IS NOT NULL
    AND orc.vds_sum IS NULL;

--then insert the records pre-2017. 
--INSERT 0 48650924
--Query returned successfully in 22 min 23 secs.
INSERT INTO vds.counts_15min (
    division_id, vdsconfig_uid, entity_location_uid,
    num_lanes, datetime_15min, count_15min, expected_bins
)
SELECT
    division_id,
    vdsconfig_uid,
    entity_location_uid,
    num_lanes,
    datetime_15min_rounded,
    count_15min,
    expected_bins
FROM gwolofs.old_rescu_staging
WHERE datetime_15min_rounded < '2017-01-01';

--update vdsconfig_x_entity_locations with first and last dates for new sensors
--UPDATE 308
--Query returned successfully in 17 secs 385 msec.
WITH updated_dates AS (
    SELECT
        division_id,
        vdsconfig_uid,
        entity_location_uid,
        MIN(datetime_15min_rounded) AS min_dt,
        MAX(datetime_15min_rounded) AS max_dt
    FROM gwolofs.old_rescu_staging
    GROUP BY
        division_id,
        vdsconfig_uid,
        entity_location_uid
)

UPDATE vds.vdsconfig_x_entity_locations AS x
SET
    first_active = LEAST(ud.min_dt, x.first_active),
    last_active = GREATEST(ud.max_dt, x.last_active)
FROM updated_dates AS ud
WHERE
    x.division_id = ud.division_id
    AND x.vdsconfig_uid = ud.vdsconfig_uid
    AND x.entity_location_uid = ud.entity_location_uid;

--insert any new sensors into vdsconfig_x_entity_locations
--INSERT 0 30
INSERT INTO vds.vdsconfig_x_entity_locations (
    division_id, vdsconfig_uid, entity_location_uid, first_active, last_active
)
SELECT
    division_id,
    vdsconfig_uid,
    entity_location_uid,
    MIN(datetime_15min_rounded) AS min_dt,
    MAX(datetime_15min_rounded) AS max_dt
FROM gwolofs.old_rescu_staging
GROUP BY
    division_id,
    vdsconfig_uid,
    entity_location_uid
ON CONFLICT (vdsconfig_uid, entity_location_uid)
DO NOTHING;

--check row counts for verification
SELECT
    date_part('year', datetime_15min) AS yr,
    COUNT(*)
FROM vds.counts_15min_div2
GROUP BY yr
ORDER BY yr;
