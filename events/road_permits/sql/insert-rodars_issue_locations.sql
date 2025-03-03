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
--rarely, you can get duplicate values from the unnested lanesaffected json.
SELECT DISTINCT
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
