CREATE OR REPLACE FUNCTION miovision_api.intersection_movements_exclude()
RETURNS trigger
LANGUAGE plpgsql
AS $BODY$

DECLARE
    row_in_im numeric;
    opposing_table text;

BEGIN

    --use the opposing table to validate
    opposing_table := CASE TG_TABLE_NAME
        WHEN 'intersection_movements' THEN 'intersection_movements_denylist'
        WHEN 'intersection_movements_denylist' THEN 'intersection_movements'
    END;
    
    EXECUTE format (
        'SELECT COUNT(*)
        FROM miovision_api.%I AS im
        WHERE
            im.intersection_uid = $1.intersection_uid
            AND im.leg = $1.leg
            AND im.movement_uid = $1.movement_uid
            AND im.classification_uid = $1.classification_uid', opposing_table
    ) INTO row_in_im USING NEW;
    
    IF (row_in_im) THEN
        RAISE NOTICE 'Row % is not being inserted. Already in % table.', new, TG_TABLE_NAME::text;
        --Return null to stop insert. 
        RETURN NULL;
    ELSE
        RETURN new;
    END IF;

END;
$BODY$;

ALTER FUNCTION miovision_api.intersection_movements_exclude
OWNER TO miovision_admins;

GRANT EXECUTE ON FUNCTION miovision_api.intersection_movements_exclude TO miovision_api_bot;

COMMENT ON FUNCTION miovision_api.intersection_movements_exclude
IS 'Runs before insert into miovision_api.intersection_movements and
miovision_api.intersection_movements_denylist to prevent inserts that are already in the opposite
table.';
