--find the arterycodes for rescu sensors
SELECT
    arterydata.stat_code,
    arterydata.street1,
    arterydata.street2,
    array_agg(DISTINCT countinfo.count_type) AS count_types,
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
    AND countinfo.category_id IN (1, 2) --{"24 HOUR",RESCU}. Exclues "speed" and "class" counts.
GROUP BY
    arterydata.stat_code,
    arterydata.street1,
    arterydata.street2
ORDER BY
    arterydata.stat_code ASC;

--SELECT 46632403
--Query returned successfully in 4 min 37 secs.

DROP TABLE IF EXISTS gwolofs.old_rescu_staging;
CREATE TABLE gwolofs.old_rescu_staging AS (
    WITH distinct_data AS (
        --there are duplicates representing about .4% of total row count. Decided to take higher count - not much impact either way.
        SELECT DISTINCT ON (arterydata.stat_code, datetime_15min_rounded)
            arterydata.stat_code,
            cnt_det.timecount AS datetime_15min,
            --the data is generally in 15 minute bins, but some of them do not fall exactly on the 15. Round (up/down).
            timestamp without time zone 'epoch' + interval '1 second' * (ROUND(extract('epoch' FROM cnt_det.timecount) / 900.0) * 900) AS datetime_15min_rounded,
            cnt_det.count AS count_15min
        FROM traffic.arterydata
        JOIN traffic.countinfo USING (arterycode)
        JOIN traffic.cnt_det USING (count_info_id)
        WHERE
            cnt_det.timecount >= '1993-01-01'::date --there is a lot of bogus data from 1899
            AND cnt_det.timecount < '2021-11-01'::date --data from here onwards is available in VDS
            AND arterydata.stat_code SIMILAR TO '%D\w{8}%'
            AND countinfo.category_id IN (1, 2) --{"24 HOUR",RESCU}. Exclues "speed" and "class" counts.
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
        dd.count_15min AS count_15min,
        v.lanes AS num_lanes,
        di.expected_bins
    FROM distinct_data AS dd
    --29385 records excluded across 30 sensors by switching to JOINs from LEFT JOINs on vdsconfig, entity_locations.
    --alternative is manually creating entries for these sensors.
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
    LEFT JOIN vds.detector_inventory AS di ON di.uid = v.uid
    ORDER BY v.vds_id, dd.datetime_15min_rounded
);

--add constraints to make sure data will insert smoothly
ALTER TABLE gwolofs.old_rescu_staging
ADD CONSTRAINT counts_15min_partitioned_pkey PRIMARY KEY (division_id, vdsconfig_uid, datetime_15min_rounded);

CREATE INDEX IF NOT EXISTS counts_15min_div2_datetime_15min_idx
ON gwolofs.old_rescu_staging USING brin(datetime_15min_rounded);

CREATE INDEX IF NOT EXISTS counts_15min_div2_vdsconfig_uid_datetime_15min_idx
ON gwolofs.old_rescu_staging USING btree(
    vdsconfig_uid ASC nulls last, datetime_15min ASC nulls last
);

--30 VDS sensors that do not exist in ITS Central (at least for that time period)
--29 are pre-2007 > 15 years old. 1 is a ramp that goes up to 2014.
--omitting these for now (via inner join above).
SELECT
    stat_code,
    MIN(datetime_15min) AS max_dt,
    MAX(datetime_15min) AS min_dt
FROM (
    SELECT
        arterydata.stat_code,
        cnt_det.timecount AS datetime_15min,
        cnt_det.count AS count_15min --the duplicates are .4% of total row count. Decided to take higher count - not much impact either way.
    FROM traffic.arterydata
    JOIN traffic.countinfo USING (arterycode)
    JOIN traffic.cnt_det USING (count_info_id)
    LEFT JOIN vds.vdsconfig AS v
        ON arterydata.stat_code = substring(v.detector_id, 'D\w{8}')
        AND cnt_det.timecount >= v.start_timestamp
        AND (
            cnt_det.timecount < v.end_timestamp
            OR v.end_timestamp IS NULL --no end date
        )
    WHERE
        cnt_det.timecount >= '1993-01-01'::date --there is a lot of bogus data from 1899
        AND cnt_det.timecount < '2021-11-01'::date --data from here onwards is available in VDS
        AND arterydata.stat_code SIMILAR TO '%D\w{8}%'
        AND countinfo.category_id IN (1, 2) --{"24 HOUR",RESCU}. Exclues "speed" and "class" counts.
        AND cnt_det.count >= 0 --a few negative 1s are present
        AND v.uid IS NULL
) AS missing
GROUP BY stat_code;

--compare the new and old data during overlapping period! (count_15min vs vds_counts_15min_count)
DROP TABLE IF EXISTS gwolofs.old_rescu_compare;
CREATE TABLE gwolofs.old_rescu_compare AS (
    WITH ors AS (
        SELECT
            vdsconfig_uid,
            datetime_15min_rounded::date AS dt,
            SUM(count_15min) AS flow_sum
        FROM gwolofs.old_rescu_staging
        WHERE datetime_15min_rounded >= '2017-01-01'
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
        FROM vds.counts_15min_div2 AS ors
        WHERE
            datetime_15min >= '2017-01-01'
            AND datetime_15min < '2021-11-01'
            AND vdsconfig_uid IN (
                SELECT DISTINCT vdsconfig_uid FROM ors
            )
        GROUP BY 1, 2
    )
    
    SELECT
        COALESCE(ors.vdsconfig_uid, c15.vdsconfig_uid) AS vdsconfig_uid,
        COALESCE(ors.dt, c15.dt) AS dt,
        ors.flow_sum AS flow_sum,
        c15.vds_sum AS vds_sum
    FROM ors
    FULL JOIN c15 USING (vdsconfig_uid, dt)
);

--where both have volumes, >99% are within 1%. 
SELECT
    --vdsconfig_uid,
    COUNT(*) FILTER (WHERE flow_sum / vds_sum >= '0.99' AND flow_sum / vds_sum < '1.01') AS within_1percent, --77,949
    COUNT(*) FILTER (WHERE NOT(flow_sum / vds_sum >= '0.99' AND flow_sum / vds_sum < '1.01')) AS outside_1percent, --373
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
    division_id, vdsconfig_uid, entity_location_uid, num_lanes, datetime_15min, count_15min, expected_bins
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
JOIN gwolofs.old_rescu_staging AS ors ON 
    ors.vdsconfig_uid = orc.vdsconfig_uid
    AND ors.datetime_15min_rounded >= orc.dt
    AND ors.datetime_15min_rounded < orc.dt + interval '1 day'
WHERE
    orc.flow_sum IS NOT NULL
    AND orc.vds_sum IS NULL;

--then insert the records pre-2017. 
--INSERT 0 38429396
--Query returned successfully in 17 min 24 secs.
INSERT INTO vds.counts_15min (
    division_id, vdsconfig_uid, entity_location_uid, num_lanes, datetime_15min, count_15min, expected_bins
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

--update vdsconfig_x_entity_locations with first and last dates.
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