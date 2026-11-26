CREATE OR REPLACE FUNCTION vds.update_detector_inventory_centreline()
RETURNS trigger
LANGUAGE plpgsql
COST 100
VOLATILE
NOT LEAKPROOF
SECURITY DEFINER
AS $BODY$
BEGIN

    UPDATE vds.detector_inventory AS di
    SET
        centreline_id = NEW.centreline_id,
        centreline_geom = (SELECT geom FROM gis_core.centreline_latest WHERE centreline_id = NEW.centreline_id)
    WHERE di.vdsconfig_uid = NEW.vdsconfig_uid;  

    RETURN NEW;

END;
$BODY$;

ALTER FUNCTION vds.update_detector_inventory_centreline()
OWNER TO vds_admins;

COMMENT ON FUNCTION vds.update_detector_inventory_centreline()
IS 'After insert/update on vds.centreline_vds, cascade changes to detector_inventory.';
