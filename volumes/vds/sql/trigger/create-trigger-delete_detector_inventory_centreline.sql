CREATE OR REPLACE FUNCTION vds.delete_detector_inventory_centreline()
RETURNS trigger
LANGUAGE plpgsql
COST 100
VOLATILE
NOT LEAKPROOF
SECURITY DEFINER
AS $BODY$
BEGIN

    UPDATE gwolofs.detector_inventory AS di
    SET
        centreline_id = NULL,
        centreline_geom = NULL
    WHERE di.vdsconfig_uid = OLD.vdsconfig_uid;

    RETURN OLD;

END;
$BODY$;

ALTER FUNCTION vds.delete_detector_inventory_centreline()
OWNER TO vds_admins;

COMMENT ON FUNCTION vds.delete_detector_inventory_centreline()
IS 'After delete on vds.centreline_vds, clear centreline fields in detector_inventory.';
