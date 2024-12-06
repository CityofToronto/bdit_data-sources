SELECT
    divisionid,
    issueid,
    timestamputc,
    locationindex,
    mainroadname,
    fromroadname,
    toroadname,
    direction,
    lanesaffected,
    geometry,
    streetnumber,
    locationtype,
    groupid,
    groupdescription
--Note there are multiple locations for each issue (unique locationindex)
FROM public.issuelocationnew
WHERE
    divisionid IN (
        8048, --rodars new
        8014, --rodars (old)
        8023 --TMMS TM3 Planned Work
    )
    AND timestamputc >= {start}::date -- noqa: PRS
    AND timestamputc < {start}::date + interval '1 day' -- noqa: PRS
