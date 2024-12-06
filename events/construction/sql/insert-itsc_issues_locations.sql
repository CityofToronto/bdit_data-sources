--this redundant CTE is just to apply ST_GeomFromText to geom_text.
WITH locations (
    divisionid, issueid, timestamputc, locationindex, mainroadname,
    fromroadname, toroadname, direction, lanesaffected, streetnumber,
    locationtype, groupid, groupdescription, direction, roadname,
    centreline_id, linear_name_id, lanesaffectedpattern, laneblocklevel,
    roadclosuretype, geom_text
) AS (
    VALUES %s
)

INSERT INTO congestion_events.itsc_issues_locations (
    divisionid, issueid, timestamputc, locationindex, mainroadname,
    fromroadname, toroadname, direction, lanesaffected, streetnumber,
    locationtype, groupid, groupdescription, direction, roadname,
    centreline_id, linear_name_id, lanesaffectedpattern, laneblocklevel,
    roadclosuretype, geom
)
SELECT
    divisionid, issueid, timestamputc, locationindex, mainroadname,
    fromroadname, toroadname, direction, lanesaffected, streetnumber,
    locationtype, groupid, groupdescription, direction, roadname,
    centreline_id, linear_name_id, lanesaffectedpattern, laneblocklevel,
    roadclosuretype, ST_GeomFromText(geom_text, 4326) AS geom
FROM locations
ON CONFLICT (divisionid, issueid, locationindex)
DO UPDATE SET
divisionid = excluded.divisionid,
issueid = excluded.issueid,
timestamputc = excluded.timestamputc,
locationindex = excluded.locationindex,
mainroadname = excluded.mainroadname,
fromroadname = excluded.fromroadname,
toroadname = excluded.toroadname,
direction = excluded.direction,
lanesaffected = excluded.lanesaffected,
streetnumber = excluded.streetnumber,
locationtype = excluded.locationtype,
groupid = excluded.groupid,
groupdescription = excluded.groupdescription,
direction = excluded.direction,
roadname = excluded.roadname,
centreline_id = excluded.centreline_id,
linear_name_id = excluded.linear_name_id,
lanesaffectedpattern = excluded.lanesaffectedpattern,
laneblocklevel = excluded.laneblocklevel,
roadclosuretype = excluded.roadclosuretype,
geom = excluded.geom;