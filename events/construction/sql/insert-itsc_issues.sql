INSERT INTO congestion_events.itsc_issues (
    divisionid, divisionname, issueid, timestamputc, issuetype, description, priority,
    proposedstarttimestamputc, proposedendtimestamputc, earlyendtimestamputc, status, timeoption,
    sourceid, starttimestamputc,
    endtimestamputc, kmpost, managementurl, cancellationstatus, closeissueonplannedendtime,
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
proposedstarttimestamputc = excluded.proposedstarttimestamputc,
proposedendtimestamputc = excluded.proposedendtimestamputc,
earlyendtimestamputc = excluded.earlyendtimestamputc,
status = excluded.status,
timeoption = excluded.timeoption,
sourceid = excluded.sourceid,
starttimestamputc = excluded.starttimestamputc,
endtimestamputc = excluded.endtimestamputc,
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
