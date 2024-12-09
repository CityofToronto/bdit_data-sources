WITH issues AS (
    --select the most recent version of each issue
    SELECT DISTINCT ON (divisionid, issueid)
        divisionid,
        issueid,
        timestamputc,
        issuetype,
        description,
        priority,
        proposedstarttimestamputc,
        proposedendtimestamputc,
        earlyendtimestamputc,
        status,
        timeoption
    FROM public.issuedata
    WHERE
        divisionid IN (
            8048, --rodars new
            8014, --rodars (old)
            8023 --TMMS TM3 Planned Work
        )
        --AND timestamputc >= {start}::date -- noqa: PRS
        --AND timestamputc < {start}::date + interval '1 day' -- noqa: PRS
    ORDER BY divisionid ASC, issueid ASC, timestamputc DESC
)

SELECT
    divisionid,
    issueid,
    timestamputc,
    --Old rodars data doesn't have this value
    COALESCE(locationindex, 0) AS locationindex,
    mainroadname,
    fromroadname,
    toroadname,
    direction AS direction_toplevel,
    lanesaffected,
    geometry,
    streetnumber,
    locationtype,
    groupid,
    groupdescription
--Note there are multiple locations for each issue (unique locationindex)
FROM public.issuelocationnew
JOIN issues USING (divisionid, issueid, timestamputc)