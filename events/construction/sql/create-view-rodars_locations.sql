-- View: congestion_events.rodars_locations

-- DROP VIEW congestion_events.rodars_locations;

CREATE OR REPLACE VIEW congestion_events.rodars_locations
AS
SELECT
    ii.divisionid,
    ii.divisionname,
    ii.issueid,
    ii.sourceid,
    ii.description,
    CASE ii.priority
        WHEN 5 THEN 'Critical'
        WHEN 4 THEN 'High'
        WHEN 3 THEN 'Medium'
        WHEN 2 THEN 'Low'
        WHEN 1 THEN 'None'
    END AS priority,
    CASE ii.status
        WHEN 1 THEN 'Future'
        WHEN 2 THEN 'In Progress'
        WHEN 3 THEN 'Ended'
        WHEN 4 THEN 'Cancelled'
        WHEN 8 THEN 'Overdue'
        WHEN 12 THEN 'In Progress'
    END AS status,
    ii.starttimestamp,
    ii.endtimestamp,
    ii.endtimestamp - ii.starttimestamp AS actual_duration,
    ii.proposedstarttimestamp,
    ii.proposedendtimestamp,
    ii.proposedendtimestamp - ii.proposedstarttimestamp AS proposed_duration,
    ii.proposedstarttimestamp::time without time zone AS proposedstart_tod,
    ii.proposedendtimestamp::time without time zone AS proposedend_tod,
    CASE ii.timeoption
        WHEN 0 THEN 'Continuous'::text
        WHEN 1 THEN 'Daily'::text
        WHEN 2 THEN 'Weekdays'::text
        WHEN 3 THEN 'Weekends'::text
        WHEN 4 THEN 'Activity Schedule'::text
        ELSE NULL::text
    END AS recurrence_schedule,
    iil.groupdescription AS location_description,
    iil.mainroadname,
    iil.fromroadname,
    iil.toroadname,
    iil.streetnumber,
    itsc_factors.locationblocklevel.locationblocklevel,
    itsc_factors.roadclosuretype_old.roadclosuretype AS roadclosuretype_desc,
    iil.locationdescription_toplevel,
    d2.direction,
    iil.roadname,
    CASE iil.centreline_id WHEN 0 THEN NULL ELSE iil.centreline_id END AS centreline_id,
    COALESCE(centreline_latest.geom,
        --find geoms for centreline that do not appear in centreline_latest
        --wrapping inside case statement coalesce/case prevents unnecessary execution
        CASE WHEN iil.centreline_id > 0 THEN
            (
                SELECT centreline.geom
                FROM gis_core.centreline
                WHERE centreline.centreline_id = iil.centreline_id
                ORDER BY centreline.version_date DESC
                LIMIT 1
            )
        END
    ) AS centreline_geom,
    iil.geom AS issue_geom,
    CASE iil.linear_name_id WHEN 0 THEN NULL ELSE iil.linear_name_id END AS linear_name_id,
    iil.lanesaffectedpattern,
    lap.lap_descriptions,
    lap.lane_open_auto,
    lap.lane_closed_auto,
    lap.lane_open_bike,
    lap.lane_closed_bike,
    lap.lane_open_ped,
    lap.lane_closed_ped,
    lap.lane_open_bus,
    lap.lane_closed_bus
FROM congestion_events.rodars_issues AS ii
JOIN congestion_events.rodars_issue_locations AS iil
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
    LATERAL (
        SELECT
            get_lanesaffected_sums.lap_descriptions,
            get_lanesaffected_sums.lane_open_auto,
            get_lanesaffected_sums.lane_closed_auto,
            get_lanesaffected_sums.lane_open_bike,
            get_lanesaffected_sums.lane_closed_bike,
            get_lanesaffected_sums.lane_open_ped,
            get_lanesaffected_sums.lane_closed_ped,
            get_lanesaffected_sums.lane_open_bus,
            get_lanesaffected_sums.lane_closed_bus
        --expand lanesaffectedpattern column.
        FROM itsc_factors.get_lanesaffected_sums(iil.lanesaffectedpattern)
            AS get_lanesaffected_sums (
                lap_descriptions, lane_open_auto, lane_closed_auto, lane_open_bike,
                lane_closed_bike, lane_open_ped, lane_closed_ped, lane_open_bus, lane_closed_bus
            )
    ) AS lap;

ALTER TABLE congestion_events.rodars_locations
OWNER TO congestion_admins;

GRANT SELECT ON TABLE congestion_events.rodars_locations TO bdit_humans;
GRANT ALL ON TABLE congestion_events.rodars_locations TO congestion_admins;
GRANT SELECT ON TABLE congestion_events.rodars_locations TO events_bot;
