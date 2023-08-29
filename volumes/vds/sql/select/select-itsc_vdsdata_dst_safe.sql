--special select statement for EDT->EST day
SELECT 
    divisionid,
    vdsid,
    timestamputc, --timestamp in INTEGER (UTC)
    lanedata
FROM public.vdsdata
WHERE
    divisionid IN (2, 8001)
    AND length(lanedata) > 0 --these records don't have any data to unpack.
    --omits the 3rd hour of the day to avoid inserting duplicates when tz changes from EDT->EST
    AND ((
        timestamputc >= extract('epoch' FROM {start}::timestamptz)::integer -- noqa: PRS
        AND timestamputc < extract('epoch' FROM {start}::timestamptz + interval '2 HOURS')::integer -- noqa: PRS
    ) OR (
        timestamputc >= extract('epoch' FROM {start}::timestamptz + interval '3 HOURS')::integer -- noqa: PRS
        AND timestamputc < extract('epoch' FROM {start}::timestamptz + interval '1 DAY')::integer -- noqa: PRS
    ))