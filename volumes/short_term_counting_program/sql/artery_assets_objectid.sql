CREATE OR REPLACE VIEW traffic.artery_objectid_pavement_asset
AS
WITH clpav18 AS ( 
    SELECT 
    clpav.geo_id, 
    clpav.objectid, 
        row_number() OVER (PARTITION BY clpav.geo_id ORDER BY (SELECT clpav.geo_id)) AS rownum
    FROM gis_shared_streets.centreline_pavement_180430 AS clpav
)

SELECT 
    ad.arterycode, 
    ac.centreline_id, 
    clpav18.objectid
FROM traffic.arterydata AS ad
JOIN traffic.arteries_centreline AS ac USING (arterycode)
JOIN clpav18 ON clpav18.geo_id = ac.centreline_id AND clpav18.rownum = 1;

COMMENT ON VIEW traffic.artery_objectid_pavement_asset IS 'Lookup between artery codes and objectid (to join pavement asset data e.g. vz_analysis.gcc_pavement_asset)';
ALTER TABLE IF EXISTS traffic.artery_objectid_pavement_asset
    OWNER TO traffic_admins;

GRANT ALL ON TABLE traffic.artery_objectid_pavement_asset TO bdit_humans;
GRANT ALL ON TABLE traffic.artery_objectid_pavement_asset TO traffic_admins;


-- flashcrow.counts.arteries_centreline must be migrated to the bigdata.traffic schema and copied over regularly
-- column arteries_centreline.centreline_id links to column centreline_pavement_180430.geo_id.
-- window function on gis_shared_streets.centreline_pavement_180430 is used to remove duplicates, keeping arterycode unique in the lookup table.