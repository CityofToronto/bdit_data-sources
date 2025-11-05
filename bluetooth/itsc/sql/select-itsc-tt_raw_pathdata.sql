SELECT
    divisionid AS division_id,
    pathid AS path_id,
    --convert timestamp (without timezone) at UTC to EDT/EST
    timezone('UTC', timestamputc) AT TIME ZONE 'Canada/Eastern' AS dt,
    round(traveltimems / 1000, 1) AS travel_time_s,
    qualitymetric AS quality_metric,
    numsamples AS num_samples,
    congestionstartmeters AS congestion_start_meters,
    congestionendmeters AS congestion_end_meters,
    minspeedkmh AS min_speed_kmh,
    unmatched,
    round(fifthpercentiletraveltimems / 1000, 1) AS fifth_percentile_tt_s,
    round(nintyfifthpercentiletraveltimems / 1000, 1) AS ninty_fifth_percentile_tt_s
FROM public.traveltimepathdata
WHERE
    divisionid IN (
        2, --RESCU (Stinson)
        8046, --miovision tt
        8026 --TPANA Bluetooth
    )
    AND timestamputc >= timezone('UTC', {start}::timestamptz) -- noqa: PRS
    AND timestamputc < timezone('UTC', {start}::timestamptz + interval '1 day'); -- noqa: PRS