INSERT INTO congestion_events.itsc_issues (
    divisionid, divisionname, issueid, timestamputc, issuetype, description, priority,
    proposedstarttimestamp, proposedendtimestamp, earlyendtimestamp, status, timeoption,
    sourceid, starttimestamp,
    endtimestamp, kmpost, managementurl, cancellationstatus, closeissueonplannedendtime,
    plannedstartadvancenoticeseconds, plannedendadvancenoticeseconds,
    locationdescriptionoverwrite, startissueonplannedstarttime, startstatus,
    updateremindernoticeseconds
)
VALUES %s
ON CONFLICT (divisionid, issueid)
DO UPDATE SET
divisionname = excluded.divisionname,
timestamputc = excluded.timestamputc,
issuetype = excluded.issuetype,
description = excluded.description,
priority = excluded.priority,
proposedstarttimestamp = excluded.proposedstarttimestamp,
proposedendtimestamp = excluded.proposedendtimestamp,
earlyendtimestamp = excluded.earlyendtimestamp,
status = excluded.status,
timeoption = excluded.timeoption,
sourceid = excluded.sourceid,
starttimestamp = excluded.starttimestamp,
endtimestamp = excluded.endtimestamp,
kmpost = excluded.kmpost,
managementurl = excluded.managementurl,
cancellationstatus = excluded.cancellationstatus,
closeissueonplannedendtime = excluded.closeissueonplannedendtime,
plannedstartadvancenoticeseconds = excluded.plannedstartadvancenoticeseconds,
plannedendadvancenoticeseconds = excluded.plannedendadvancenoticeseconds,
locationdescriptionoverwrite = excluded.locationdescriptionoverwrite,
startissueonplannedstarttime = excluded.startissueonplannedstarttime,
startstatus = excluded.startstatus,
updateremindernoticeseconds = excluded.updateremindernoticeseconds;
