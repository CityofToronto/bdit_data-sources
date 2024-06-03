/*
You will need to follow this adhoc process if you discover records in raw_vdsdata
that are missing vdsconfig_uid or entity_locations_uid foreign keys, in order to
add fkeys and then summarize those records in to counts_15min, counts_15min_bylane. 

This particular case occured when raw table was backfilled without latest lookups. 
*/

WITH updates AS ( --noqa: L045
    SELECT
        d.volume_uid,
        c.uid AS vdsconfig_uid,
        e.uid AS entity_location_uid
    FROM vds.raw_vdsdata AS d
    JOIN vds.vdsconfig AS c ON
        d.vds_id = c.vds_id
        AND d.division_id = c.division_id
        AND d.dt >= c.start_timestamp
        AND (
            d.dt < c.end_timestamp
            OR c.end_timestamp IS NULL) --no end date
    JOIN vds.entity_locations AS e ON
        d.vds_id = e.entity_id
        AND d.division_id = e.division_id
        AND d.dt >= e.start_timestamp
        AND (
            d.dt < e.end_timestamp
            OR e.end_timestamp IS NULL) --no end date
    WHERE (
        d.vdsconfig_uid IS NULL 
        OR d.entity_location_uid IS NULL
        )
        --want both to be not null so we can safely insert into counts_15min
        AND c.uid IS NOT NULL
        AND e.uid IS NOT NULL
),

--update missing fkeys in raw table.
updated AS (
    UPDATE vds.raw_vdsdata AS dest --noqa: PRS
    SET
        vdsconfig_uid = updates.vdsconfig_uid,
        entity_location_uid = updates.entity_location_uid
    FROM updates
    WHERE dest.volume_uid = updates.volume_uid
    --return updated rows to summarize
    RETURNING
        dest.division_id,
        dest.vdsconfig_uid,
        dest.entity_location_uid,
        dest.datetime_15min,
        dest.volume_veh_per_hr,
        dest.lane
),

--insert `updated` records into counts_15min
counts_15min_update AS ( --noqa: L045
    INSERT INTO vds.counts_15min (division_id, vdsconfig_uid, entity_location_uid, num_lanes, datetime_15min,
        count_15min, expected_bins, num_obs, num_distinct_lanes) --noqa: PRS

    /* Conversion of hourly volumes to count depends on size of bin.
    These bin counts were determined by looking at the most common bin gap using:
    bdit_data-sources/volumes/vds/exploration/time_gaps.sql */

    SELECT
        d.division_id,
        d.vdsconfig_uid,
        d.entity_location_uid,
        c.lanes AS num_lanes,
        d.datetime_15min,
        SUM(d.volume_veh_per_hr) / 4 / di.expected_bins AS count_15min,
        -- / 4 to convert hourly volume to 15 minute volume
        -- / (expected_bins) to get average 15 minute volume depending on bin size
        -- (assumes blanks are 0)
        di.expected_bins,
        COUNT(*) AS num_obs,
        COUNT(DISTINCT d.lane) AS num_distinct_lanes
    FROM updated AS d
    JOIN vds.vdsconfig AS c ON d.vdsconfig_uid = c.uid
    JOIN vds.detector_inventory AS di USING (vdsconfig_uid, entity_location_uid)
    WHERE
        d.division_id = 2
        AND d.vdsconfig_uid IS NOT NULL
        AND d.entity_location_uid IS NOT NULL
    GROUP BY
        d.division_id,
        d.vdsconfig_uid,
        d.entity_location_uid,
        c.lanes,
        di.expected_bins,
        d.datetime_15min
    ON CONFLICT DO NOTHING
)

--insert `updated` records into counts_15min_Bylane
INSERT INTO vds.counts_15min_bylane (division_id, vdsconfig_uid, entity_location_uid, lane, datetime_15min,
    count_15min, expected_bins, num_obs)

SELECT 
    d.division_id,
    d.vdsconfig_uid,
    d.entity_location_uid,
    d.lane,
    d.datetime_15min,
    SUM(d.volume_veh_per_hr) / 4 / di.expected_bins AS count_15min,
    -- / 4 to convert hourly volume to 15 minute volume
    -- / (expected_bins) to get average 15 minute volume depending on 
    -- bin size (assumes blanks are 0)
    di.expected_bins,
    COUNT(*) AS num_obs
FROM updated AS d
JOIN vds.detector_inventory AS di USING (vdsconfig_uid, entity_location_uid)
WHERE
    d.division_id = 2 --division 8001 sensors have only 1 lane, aggregate only into view counts_15min_div8001.
    AND d.vdsconfig_uid IS NOT NULL
    AND d.entity_location_uid IS NOT NULL
GROUP BY
    d.division_id,
    d.vdsconfig_uid,
    d.entity_location_uid,
    di.expected_bins,
    d.lane,
    d.datetime_15min
ON CONFLICT DO NOTHING