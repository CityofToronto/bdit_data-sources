--this redundant CTE is just to apply ST_GeomFromText to geom_text.
WITH locations (
    divisionid, issueid, timestamputc, locationindex, mainroadname, fromroadname,
    toroadname, direction_toplevel, lanesaffected, streetnumber, locationtype, groupid,
    groupdescription, locationblocklevel_toplevel, roadclosuretype_toplevel,
    encodedcoordinates_toplevel, locationdescription_toplevel, direction, roadname,
    centreline_id, linear_name_id, lanesaffectedpattern, laneblocklevel,
    roadclosuretype, geom_text
) AS (
    VALUES %s --noqa: PRS
)

INSERT INTO congestion_events.rodars_issue_locations (
    divisionid, issueid, timestamputc, locationindex, mainroadname, fromroadname, toroadname,
    direction_toplevel, streetnumber, locationtype, groupid, groupdescription, lanesaffected,
    locationblocklevel_toplevel, roadclosuretype_toplevel, encodedcoordinates_toplevel,
    locationdescription_toplevel, direction, roadname, centreline_id, linear_name_id,
    lanesaffectedpattern, laneblocklevel, roadclosuretype, geom
)
SELECT
    divisionid,
    issueid,
    timestamputc,
    locationindex,
    mainroadname,
    fromroadname,
    toroadname,
    direction_toplevel,
    streetnumber,
    locationtype,
    groupid,
    groupdescription,
    lanesaffected,
    locationblocklevel_toplevel,
    roadclosuretype_toplevel,
    encodedcoordinates_toplevel,
    locationdescription_toplevel,
    direction,
    roadname,
    centreline_id,
    linear_name_id,
    lanesaffectedpattern,
    laneblocklevel,
    roadclosuretype,
    st_geomfromtext(geom_text, 4326) AS geom
FROM locations;
--ON CONFLICT (divisionid, issueid, locationindex, direction)
--DO UPDATE SET
--divisionid = excluded.divisionid,
--issueid = excluded.issueid,
--timestamputc = excluded.timestamputc,
--locationindex = excluded.locationindex,
--mainroadname = excluded.mainroadname,
--fromroadname = excluded.fromroadname,
--toroadname = excluded.toroadname,
--direction_toplevel = excluded.direction_toplevel,
--lanesaffected = excluded.lanesaffected,
--streetnumber = excluded.streetnumber,
--locationtype = excluded.locationtype,
--groupid = excluded.groupid,
--groupdescription = excluded.groupdescription,
--lanesaffected_direction = excluded.lanesaffected_direction,
--roadname = excluded.roadname,
--centreline_id = excluded.centreline_id,
--linear_name_id = excluded.linear_name_id,
--lanesaffectedpattern = excluded.lanesaffectedpattern,
--laneblocklevel = excluded.laneblocklevel,
--roadclosuretype = excluded.roadclosuretype,
--geom = excluded.geom;
