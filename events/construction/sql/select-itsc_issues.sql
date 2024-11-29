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
        AND timestamputc >= {start}::date -- noqa: PRS
        AND timestamputc < {start}::date + interval '1 day' -- noqa: PRS
    ORDER BY divisionid ASC, issueid ASC, timestamputc DESC
)

SELECT
    issues.divisionid,
    datadivision.shortname AS divisionname,
    issues.issueid,
    issues.timestamputc,
    issues.issuetype,
    issues.description,
    issues.priority,
    issues.proposedstarttimestamputc,
    issues.proposedendtimestamputc,
    issues.earlyendtimestamputc,
    issues.status,
    issues.timeoption,
    issuelocationnew.locationindex,
    issuelocationnew.mainroadname,
    issuelocationnew.fromroadname,
    issuelocationnew.toroadname,
    issuelocationnew.direction,
    issuelocationnew.lanesaffected,
    issuelocationnew.geometry,
    issuelocationnew.streetnumber,
    issuelocationnew.locationtype,
    issuelocationnew.groupid,
    issuelocationnew.groupdescription,
    issueconfig.sourceid,
    issueconfig.starttimestamputc,
    issueconfig.endtimestamputc,
    issueconfig.kmpost,
    issueconfig.managementurl,
    issueconfig.cancellationstatus,
    issueconfig.closeissueonplannedendtime,
    issueconfig.plannedstartadvancenoticeseconds,
    issueconfig.plannedendadvancenoticeseconds,
    issueconfig.locationdescriptionoverwrite,
    issueconfig.startissueonplannedstarttime,
    issueconfig.startstatus,
    issueconfig.updateremindernoticeseconds
FROM issues
LEFT JOIN public.issuelocationnew USING (divisionid, issueid, timestamputc)
LEFT JOIN public.issueconfig USING (divisionid, issueid)
LEFT JOIN public.datadivision USING (divisionid)
ORDER BY issueid DESC