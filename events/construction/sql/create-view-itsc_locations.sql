-- View: congestion_events.itsc_locations

DROP VIEW congestion_events.itsc_locations;

CREATE OR REPLACE VIEW congestion_events.itsc_locations
 AS
 SELECT
    ii.divisionid,
    ii.divisionname,
    ii.issueid,
    ii.timestamputc,
    ii.issuetype,
    ii.description,
    ii.priority,
    ii.proposedstarttimestamp,
    ii.proposedendtimestamp,
    ii.proposedendtimestamp - ii.proposedstarttimestamp AS proposed_duration,
    ii.proposedstarttimestamp::time AS proposedstart_tod,
    ii.proposedendtimestamp::time AS proposedend_tod,
    ii.earlyendtimestamp,
    ii.status,
    CASE ii.timeoption WHEN 0 THEN 'Continuous' WHEN 1 THEN 'Daily' WHEN 2 THEN 'Weekdays' WHEN 3 THEN 'Weekends' WHEN 4 THEN 'Activity Schedule' END AS recurrence_schedule,
    ii.sourceid,
    ii.starttimestamp,
    ii.endtimestamp,
    ii.endtimestamp - ii.starttimestamp AS actual_duration,
    ii.cancellationstatus,
    ii.closeissueonplannedendtime,
    ii.plannedstartadvancenoticeseconds,
    ii.plannedendadvancenoticeseconds,
    ii.locationdescriptionoverwrite,
    ii.startissueonplannedstarttime,
    ii.startstatus,
    iil.locationindex,
    iil.mainroadname,
    iil.fromroadname,
    iil.toroadname,
    iil.direction_toplevel,
    d1.direction AS direction_toplevel_desc,
    iil.streetnumber,
    iil.locationtype,
    iil.groupid,
    iil.groupdescription,
    iil.lanesaffected,
    iil.locationblocklevel_toplevel,
    iil.roadclosuretype_toplevel,
    iil.locationdescription_toplevel,
    iil.direction,
    d2.direction AS direction_desc,
    iil.roadname,
    iil.centreline_id,
    iil.linear_name_id,
    iil.lanesaffectedpattern,
    lap.lane_open_auto,
    lap.lane_closed_auto,
    lap.lane_open_bike,
    lap.lane_closed_bike,
    lap.lane_open_ped,
    lap.lane_closed_ped,
    lap.lane_open_bus,
    lap.lane_closed_bus,
    lb.locationblocklevel,
    rct_o.roadclosuretype AS roadclosuretype_desc,
    iil.geom
   FROM congestion_events.itsc_issues AS ii
   JOIN congestion_events.itsc_issue_locations AS iil USING (issueid, divisionid, timestamp)
     LEFT JOIN itsc_factors.direction d1 ON d1.code = iil.direction_toplevel
     LEFT JOIN itsc_factors.direction d2 ON d2.code = iil.direction::numeric::integer
     LEFT JOIN itsc_factors.locationblocklevel lb ON iil.laneblocklevel::numeric::integer = lb.code
     LEFT JOIN itsc_factors.roadclosuretype_old rct_o ON iil.roadclosuretype::numeric::integer = rct_o.code,
    LATERAL ( SELECT get_lanesaffected_sums.lane_open_auto,
            get_lanesaffected_sums.lane_closed_auto,
            get_lanesaffected_sums.lane_open_bike,
            get_lanesaffected_sums.lane_closed_bike,
            get_lanesaffected_sums.lane_open_ped,
            get_lanesaffected_sums.lane_closed_ped,
            get_lanesaffected_sums.lane_open_bus,
            get_lanesaffected_sums.lane_closed_bus
           FROM itsc_factors.get_lanesaffected_sums(iil.lanesaffectedpattern) ) lap;

ALTER TABLE congestion_events.itsc_locations OWNER TO congestion_admins;

GRANT SELECT ON TABLE congestion_events.itsc_locations TO bdit_humans;
GRANT ALL ON TABLE congestion_events.itsc_locations TO congestion_admins;

GRANT SELECT ON TABLE congestion_events.itsc_locations TO events_bot;
