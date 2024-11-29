WITH new_geoms (issueid, divisionid, locationindex, geom_text) AS (
    VALUES %s
)

UPDATE gwolofs.itsc_issues
SET geometry = ST_GeomFromText(geom_text, 4326)
FROM new_geoms
WHERE
    itsc_issues.issueid = new_geoms.issueid
    AND itsc_issues.divisionid = new_geoms.divisionid
    AND itsc_issues.locationindex = new_geoms.locationindex;
