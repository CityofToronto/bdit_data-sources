SELECT
    divisionid AS division_id,
    pathid AS path_id,
    rawdatatype AS raw_data_type,
    --convert timestamp (without timezone) at UTC to EDT/ES AS dtT
    timezone('UTC', timestamputc) AT TIME ZONE 'Canada/Eastern' AS dt,
    round(traveltimems / 1000, 1) AS travel_time_s,
    qualitymetric AS quality_metric,
    numsamples AS num_samples,
    congestionstartmeters AS congestion_start_meters,
    congestionendmeters AS congestion_end_meters,
    minspeedkmh AS min_speed_kmh,
    round(fifthpercentiletraveltimems / 1000, 1) AS fifth_percentile_tt_s,
    round(nintyfifthpercentiletraveltimems / 1000, 1) AS nintyfifth_percentile_tt_s,
    unmatched
FROM public.traveltimepathrawdata
WHERE
    --this table only has divisionid = 2 data
    divisionid = 2 --RESCU (Stinson)
    AND timestamputc >= timezone('UTC', {start}::timestamptz) -- noqa: PRS
    AND timestamputc < timezone('UTC', {start}::timestamptz + interval '1 day'); -- noqa: PRS