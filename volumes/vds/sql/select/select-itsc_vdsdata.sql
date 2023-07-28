SELECT 
    divisionid,
    vdsid,
    timestamputc, --timestamp in INTEGER (UTC)
    lanedata
FROM public.vdsdata
WHERE
    timestamputc >= extract('epoch' FROM {start}::timestamptz)::integer
    AND timestamputc < extract('epoch' FROM {start}::timestamptz + interval '1 DAY')::integer
    AND divisionid = 2 --other is 8001 which are traffic signal detectors and are mostly empty.
    AND length(lanedata) > 0; --these records don't have any data to unpack.