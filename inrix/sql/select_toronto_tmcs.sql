DROP TABLE gis.inrix_tmc_tor;
SELECT gis.inrix_tmc.*
INTO gis.inrix_tmc_tor
FROM gis.inrix_tmc
INNER JOIN gis.to ON ST_INTERSECTS(gis.inrix_tmc.geom, ST_TRANSFORM(gis.to.geom, 4326));
COMMENT ON TABLE gis.inrix_tmc_tor IS
'Inrix TMCs that are within or intersect the Toronto municipal boundary'