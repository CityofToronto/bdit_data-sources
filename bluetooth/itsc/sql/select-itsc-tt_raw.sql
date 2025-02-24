SELECT
    divisionid,
    pathid,
    rawdatatype,
    timezone('UTC', timestamputc) AT TIME ZONE 'Canada/Eastern' AS dt, --convert timestamp (without timezone) at UTC to EDT/EST
    round(traveltimems / 1000, 1) AS traveltime_s,
    qualitymetric,
    numsamples,
    congestionstartmeters,
    congestionendmeters,
    minspeedkmh,
    round(fifthpercentiletraveltimems / 1000, 1) AS fifthpercentiletraveltime_s,
    round(nintyfifthpercentiletraveltimems / 1000, 1) AS nintyfifthpercentiletraveltime_s,
    unmatched
FROM public.traveltimepathrawdata
WHERE
    --this table only has divisionid = 2 data
    divisionid = 2 --RESCU (Stinson)
    AND timestamputc >= timezone('UTC', '2024-12-01'::timestamptz) --need tz conversion on RH side to make use of index. -- noqa: PRS
    --AND timestamputc < timezone('UTC', {start}::timestamptz + interval '1 day'); -- noqa: PRS
--LIMIT 10000;