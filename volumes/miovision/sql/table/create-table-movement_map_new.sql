CREATE TABLE miovision_api.movement_map (
    movement_uid integer,
    leg text,
    entry_dir text,
    movement text,
    exit_leg text,
    exit_dir text,
    CONSTRAINT movement_map_pkey PRIMARY KEY (movement_uid, leg)
);

COMMENT ON TABLE miovision_api.movement_map
IS 'A more intuitive version of former `miovision_api.movement_map`.';

INSERT INTO miovision_api.movement_map (
    movement_uid, leg, entry_dir, movement, exit_leg, exit_dir
)
(
    VALUES
    (1, 'E', 'WB', 'thru', 'W', 'WB'),
    (2, 'E', 'WB', 'left', 'S', 'SB'),
    (3, 'E', 'WB', 'right', 'N', 'NB'),
    (4, 'E', 'WB', 'u_turn', 'E', 'EB'),
    (5, 'E', 'SB', 'cw', null, null),
    (6, 'E', 'NB', 'ccw', null, null),
    (7, 'E', 'WB', 'enter', null, null),

    (1, 'N', 'SB', 'thru', 'S', 'SB'),
    (2, 'N', 'SB', 'left', 'E', 'EB'),
    (3, 'N', 'SB', 'right', 'W', 'WB'),
    (4, 'N', 'SB', 'u_turn', 'N', 'NB'),
    (5, 'N', 'EB', 'cw', null, null),
    (6, 'N', 'WB', 'ccw', null, null),
    (7, 'N', 'SB', 'enter', null, null),

    (1, 'S', 'NB', 'thru', 'N', 'NB'),
    (2, 'S', 'NB', 'left', 'W', 'WB'),
    (3, 'S', 'NB', 'right', 'E', 'EB'),
    (4, 'S', 'NB', 'u_turn', 'S', 'SB'),
    (5, 'S', 'WB', 'cw', null, null),
    (6, 'S', 'EB', 'ccw', null, null),
    (7, 'S', 'NB', 'enter', null, null),

    (1, 'W', 'EB', 'thru', 'E', 'EB'),
    (2, 'W', 'EB', 'left', 'N', 'NB'),
    (3, 'W', 'EB', 'right', 'S', 'SB'),
    (4, 'W', 'EB', 'u_turn', 'W', 'WB'),
    (5, 'W', 'NB', 'cw', null, null),
    (6, 'W', 'SB', 'ccw', null, null),
    (7, 'W', 'EB', 'enter', null, null)
);