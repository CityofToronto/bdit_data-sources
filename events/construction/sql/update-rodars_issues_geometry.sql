WITH new_geoms (issueid, divisionid, locationindex, geom_text) AS (
    VALUES %s
)

UPDATE congestion_events.rodars_issues
SET geometry = ST_GeomFromText(geom_text, 4326)
FROM new_geoms
WHERE
    rodars_issues.issueid = new_geoms.issueid
    AND rodars_issues.divisionid = new_geoms.divisionid
    AND rodars_issues.locationindex = new_geoms.locationindex;
