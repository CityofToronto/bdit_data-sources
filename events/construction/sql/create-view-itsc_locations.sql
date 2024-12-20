-- View: congestion_events.itsc_locations

DROP VIEW congestion_events.itsc_locations;

CREATE OR REPLACE VIEW congestion_events.itsc_locations
AS
SELECT
    ii.divisionid,
    ii.divisionname,
    ii.issueid,
    ii.timestamputc,
    ii.description,
    CASE ii.priority
        WHEN 5 THEN 'Critical'
        WHEN 4 THEN 'High'
        WHEN 3 THEN 'Medium'
        WHEN 2 THEN 'Low'
        WHEN 1 THEN 'None'
    END AS priority,
    ii.proposedstarttimestamp,
    ii.proposedendtimestamp,
    ii.proposedstarttimestamp::time without time zone AS proposedstart_tod,
    ii.proposedendtimestamp::time without time zone AS proposedend_tod,
    ii.earlyendtimestamp,
    CASE ii.status
        WHEN 1 THEN 'Future'
        WHEN 2 THEN 'In Progress'
        WHEN 3 THEN 'Ended'
        WHEN 4 THEN 'Cancelled'
        WHEN 8 THEN 'Overdue'
        WHEN 12 THEN 'In Progress'
    END AS status,
    ii.sourceid,
    ii.starttimestamp,
    ii.endtimestamp,
    ii.cancellationstatus,
    ii.startstatus,
    iil.mainroadname,
    iil.fromroadname,
    iil.toroadname,
    iil.streetnumber,
    iil.locationtype,
    iil.groupid,
    iil.groupdescription,
    itsc_factors.locationblocklevel.locationblocklevel,
    itsc_factors.roadclosuretype_old.roadclosuretype AS roadclosuretype_desc,
    iil.locationdescription_toplevel,
    d2.direction,
    iil.roadname,
    iil.centreline_id,
    centreline_latest.geom AS centreline_geom,
    iil.linear_name_id,
    lap.lane_open_auto,
    lap.lane_closed_auto,
    lap.lane_open_bike,
    lap.lane_closed_bike,
    lap.lane_open_ped,
    lap.lane_closed_ped,
    lap.lane_open_bus,
    lap.lane_closed_bus,
    iil.geom,
    ii.proposedendtimestamp - ii.proposedstarttimestamp AS proposed_duration,
    CASE ii.timeoption
        WHEN 0 THEN 'Continuous'::text
        WHEN 1 THEN 'Daily'::text
        WHEN 2 THEN 'Weekdays'::text
        WHEN 3 THEN 'Weekends'::text
        WHEN 4 THEN 'Activity Schedule'::text
        ELSE NULL::text
    END AS recurrence_schedule,
    ii.endtimestamp - ii.starttimestamp AS actual_duration
FROM congestion_events.itsc_issues AS ii
JOIN congestion_events.itsc_issue_locations AS iil
    ON iil.issueid = ii.issueid
    AND iil.divisionid = ii.divisionid
    AND iil.timestamputc = ii.timestamputc
LEFT JOIN gis_core.centreline_latest USING (centreline_id)
LEFT JOIN itsc_factors.direction AS d1
    ON d1.code = iil.direction_toplevel
LEFT JOIN itsc_factors.direction AS d2
    ON d2.code = iil.direction::numeric::integer
LEFT JOIN itsc_factors.locationblocklevel
    ON iil.laneblocklevel::numeric::integer = itsc_factors.locationblocklevel.code
LEFT JOIN itsc_factors.roadclosuretype_old
    ON iil.roadclosuretype::numeric::integer = itsc_factors.roadclosuretype_old.code,
LATERAL(
    SELECT
        get_lanesaffected_sums.lane_open_auto,
        get_lanesaffected_sums.lane_closed_auto,
        get_lanesaffected_sums.lane_open_bike,
        get_lanesaffected_sums.lane_closed_bike,
        get_lanesaffected_sums.lane_open_ped,
        get_lanesaffected_sums.lane_closed_ped,
        get_lanesaffected_sums.lane_open_bus,
        get_lanesaffected_sums.lane_closed_bus
    --expand lanesaffectedpattern column.
    FROM itsc_factors.get_lanesaffected_sums(iil.lanesaffectedpattern) AS get_lanesaffected_sums(
        lane_open_auto, lane_closed_auto, lane_open_bike, lane_closed_bike,
        lane_open_ped, lane_closed_ped, lane_open_bus, lane_closed_bus
    )
) AS lap;

ALTER TABLE congestion_events.itsc_locations
OWNER TO congestion_admins;

GRANT SELECT ON TABLE congestion_events.itsc_locations TO bdit_humans;
GRANT ALL ON TABLE congestion_events.itsc_locations TO congestion_admins;
GRANT SELECT ON TABLE congestion_events.itsc_locations TO events_bot;

/*
SELECT * FROM congestion_events.itsc_locations WHERE divisionid = 8048 LIMIT 1000 

SELECT DISTINCT ON (status) status, sourceid, description FROM congestion_events.itsc_issues ORDER BY status, sourceid DESC


SELECT * FROM congestion_events.itsc_issues WHERE status = 2*/