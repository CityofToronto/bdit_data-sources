CREATE OR REPLACE VIEW aduyves.artery_objectid_pavement_asset
AS
WITH cp18 AS ( 
    SELECT cp.geo_id, 
	cp.objectid, 
	row_number() OVER (PARTITION BY cp.geo_id ORDER BY (SELECT cp.geo_id)) AS rownum
    FROM gis_shared_streets.centreline_pavement_180430 AS cp
)
SELECT ad.arterycode, 
	ac.centreline_id, 
	cp18.objectid
FROM traffic.arterydata AS ad
JOIN aduyves.arteries_centreline AS ac USING (arterycode)
JOIN cp18 ON cp18.geo_id = ac.centreline_id AND cp18.rownum = 1


COMMENT ON VIEW aduyves.artery_objectid_pavement_asset IS 'Lookup between artery codes and objectid (to retrieve pavement asset data)';
GRANT SELECT ON TABLE aduyves.artery_objectid_pavement_asset TO bdit_humans;


-- col arteries_centreline.centreline_id is equivalent to col arterydata.geo_id.
-- counts.arteries_centreline in FLASHCROW database was copied to user schema
-- aduyves.arteries_centreline for working (I do not have access to flashcrow db).
-- window function on gis_shared_streets.centreline_pavement_180430 is used to remove duplicates, keeping arterycode unique in the final table.