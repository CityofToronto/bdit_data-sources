WITH centrelines AS (
    SELECT
        divisionid,
        pathid,
        ARRAY_AGG(featureid ORDER BY featureindex) AS centreline_ids --noqa: LT14
    FROM public.traveltimepathfeature
    GROUP BY
        divisionid,
        pathid
)

SELECT
    paths.divisionid AS division_id,
    paths.pathid AS path_id,
    paths.sourceid AS source_id,
    paths.algorithm,
    paths.firstfeaturestartoffsetmeters AS first_feature_start_off_set_meters,
    paths.lastfeatureendoffsetmeters AS last_feature_end_off_set_meters,
    paths.firstfeatureforward AS first_feature_forward,
    paths.rawdatatypes AS raw_data_types,
    paths.externaldataorigin AS external_data_origin,
    paths.externaldatadestination AS external_data_destination,
    paths.externaldatawaypoints AS external_data_waypoints,
    paths.timeadjustfactor AS time_adjust_factor,
    paths.timeadjustconstantseconds AS time_adjust_constant_seconds,
    paths.queuemaxspeedkmh AS queue_max_speed_kmh,
    paths.minimaldelayspeedkmh AS minimal_delay_speed_kmh,
    paths.majordelayspeedkmh AS major_delay_speed_kmh,
    paths.severedelayspeedkmh AS severe_delay_speed_kmh,
    paths.useminimumspeed AS use_minimum_speed,
    paths.lengthmeters AS length_m,
    timezone('UTC', paths.starttimestamputc) AT TIME ZONE 'Canada/Eastern' AS start_timestamp,
    timezone('UTC', paths.endtimestamputc) AT TIME ZONE 'Canada/Eastern' AS end_timestamp,
    paths.featurespeeddivisionid AS feature_speed_division_id,
    paths.severedelayissuedivisionid AS severe_delay_issue_division_id,
    paths.majordelayissuedivisionid AS major_delay_issue_division_id,
    paths.minordelayissuedivisionid AS minor_delay_issue_division_id,
    paths.queueissuedivisionid AS queue_issue_division_id,
    paths.queuedetectionclearancespeedkmh AS queue_detection_clearance_speed_kmh,
    paths.severedelayclearancespeedkmh AS severe_delay_clearance_speed_kmh,
    paths.pathtype AS path_type,
    paths.pathdatatimeoutforissuecreationseconds AS path_data_timeout_for_issue_creation_seconds,
    paths.encodedpolyline AS encoded_polyline,
    centrelines.centreline_ids
FROM public.traveltimepathconfig AS paths
JOIN centrelines USING (divisionid, pathid)
WHERE
    divisionid IN (
        2, --RESCU (Stinson)
        8046, --miovision tt
        8026 --TPANA Bluetooth
    )
    AND paths.starttimestamputc >= timezone('UTC', {start}::timestamptz) -- noqa: PRS
    AND paths.starttimestamputc < timezone('UTC', {start}::timestamptz + interval '1 day'); -- noqa: PRS