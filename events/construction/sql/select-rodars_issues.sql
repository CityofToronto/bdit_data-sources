--this select query is used to select issue metadata from ITSC database in rodars_pull pipeline.

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
            8014 --rodars (old)
        )
        AND timestamputc >= {start}::date -- noqa: PRS, LT02
        AND timestamputc < {start}::date + interval '1 day' -- noqa: PRS
    ORDER BY
        divisionid ASC,
        issueid ASC,
        timestamputc DESC
)

SELECT
    issues.divisionid,
    datadivision.shortname AS divisionname,
    issues.issueid,
    issues.timestamputc,
    issues.issuetype,
    issues.description,
    issues.priority,
    TIMEZONE('UTC', issues.proposedstarttimestamputc) AT TIME ZONE 'America/Toronto'
    AS proposedstarttimestamp,
    TIMEZONE('UTC', issues.proposedendtimestamputc) AT TIME ZONE 'America/Toronto'
    AS proposedendtimestamp,
    TIMEZONE('UTC', issues.earlyendtimestamputc) AT TIME ZONE 'America/Toronto'
    AS earlyendtimestamp,
    issues.status,
    issues.timeoption,
    issueconfig.sourceid,
    TIMEZONE('UTC', issueconfig.starttimestamputc) AT TIME ZONE 'America/Toronto'
    AS starttimestamp,
    TIMEZONE('UTC', issueconfig.endtimestamputc) AT TIME ZONE 'America/Toronto'
    AS endtimestamp,
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
LEFT JOIN public.issueconfig USING (divisionid, issueid)
LEFT JOIN public.datadivision USING (divisionid)
ORDER BY issues.issueid DESC
