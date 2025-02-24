SELECT
    divisionid,
    pathid,
    --convert timestamp (without timezone) at UTC to EDT/EST
    timezone('UTC', timestamputc) AT TIME ZONE 'Canada/Eastern' AS dt,
    round(traveltimems / 1000, 1) AS traveltime_s,
    qualitymetric,
    congestionstartlandmarkindex,
    congestionendlandmarkindex,
    numsamples,
    congestionstartmeters,
    congestionendmeters,
    minspeedkmh,
    unmatched,
    round(fifthpercentiletraveltimems / 1000, 1) AS fifthpercentiletraveltime_s,
    round(nintyfifthpercentiletraveltimems / 1000, 1) AS nintyfifthpercentiletraveltime_s
FROM public.traveltimepathdata
WHERE
    divisionid IN (
        2, --RESCU (Stinson)
        8046, --miovision tt
        8026 --TPANA Bluetooth
    )
    AND timestamputc >= timezone('UTC', '2024-12-01'::timestamptz) --need tz conversion on RH side to make use of index. -- noqa: PRS
    --AND timestamputc < timezone('UTC', {start}::timestamptz + interval '1 day'); -- noqa: PRS