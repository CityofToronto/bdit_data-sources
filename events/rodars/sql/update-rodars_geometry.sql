WITH new_geoms (issueid, divisionid, geom_text) AS (
    VALUES %s
)

UPDATE gwolofs.rodars_new
SET geometry = ST_GeomFromText(geom_text, 4326)
FROM new_geoms
WHERE
    rodars_new.issueid = new_geoms.issueid
    AND rodars_new.divisionid = new_geoms.divisionid;
