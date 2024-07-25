DROP TABLE gwolofs.miovision_movement_map_new;
CREATE TABLE gwolofs.miovision_movement_map_new AS (
    SELECT
        entries.movement_uid,
        entries.leg_old AS leg,
        entries.dir AS entry_dir,
        mov.movement_name AS movement,
        --assign exits for peds, bike entry only movements
        COALESCE(exits.leg_new, entries.leg_old) AS exit_leg,
        COALESCE(exits.dir, entries.dir) AS exit_dir
    FROM miovision_api.movement_map AS entries
    JOIN miovision_api.movements AS mov USING (movement_uid)
    LEFT JOIN miovision_api.movement_map AS exits
        ON exits.leg_old = entries.leg_old
        AND exits.movement_uid = entries.movement_uid
        AND exits.leg_new = substr(exits.dir, 1, 1) --eg. E leg going East is an exit
    WHERE entries.leg_new <> substr(entries.dir, 1, 1) --eg. E leg going West is an entry
);

ALTER TABLE gwolofs.miovision_movement_map_new
ADD CONSTRAINT movement_map_new_pkey PRIMARY KEY (movement_uid, leg);

COMMENT ON TABLE gwolofs.miovision_movement_map_new
IS 'A more intuitive version of miovision_api.movement_map.';