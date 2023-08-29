SELECT 
    divisionid,
    vdsid,
    timestamputc, --timestamp in INTEGER (UTC)
    lanedata
FROM public.vdsdata
WHERE
    timestamputc >= extract('epoch' FROM {start}::timestamptz)::integer -- noqa: PRS
    AND timestamputc < extract('epoch' FROM {start}::timestamptz + interval '1 DAY')::integer -- noqa: PRS
    AND divisionid IN (2, 8001)
    AND length(lanedata) > 0; --these records don't have any data to unpack.