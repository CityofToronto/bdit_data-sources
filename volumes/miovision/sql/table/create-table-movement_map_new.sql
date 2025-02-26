CREATE TABLE miovision_api.movement_map_new (
    movement_uid integer,
    leg text,
    entry_dir text,
    movement text,
    exit_leg text,
    exit_dir text,
    CONSTRAINT movement_map_new_pkey PRIMARY KEY (movement_uid, leg)
);

COMMENT ON TABLE miovision_api.movement_map_new
IS 'A more intuitive version of former `miovision_api.movement_map`.';

INSERT INTO miovision_api.movement_map_new (
    movement_uid, leg, entry_dir, movement, exit_leg, exit_dir
)
(
    VALUES
    (1, 'E', 'WB', 'thru', 'W', 'WB'),
    (2, 'E', 'WB', 'left', 'S', 'SB'),
    (3, 'E', 'WB', 'right', 'N', 'NB'),
    (4, 'E', 'WB', 'u_turn', 'E', 'EB'),
    (5, 'E', 'SB', 'cw', 'E', 'SB'),
    (6, 'E', 'NB', 'ccw', 'E', 'NB'),
    (7, 'E', 'WB', 'enter', 'E', 'WB'),

    (1, 'N', 'SB', 'thru', 'S', 'SB'),
    (2, 'N', 'SB', 'left', 'E', 'EB'),
    (3, 'N', 'SB', 'right', 'W', 'WB'),
    (4, 'N', 'SB', 'u_turn', 'N', 'NB'),
    (5, 'N', 'EB', 'cw', 'N', 'EB'),
    (6, 'N', 'WB', 'ccw', 'N', 'WB'),
    (7, 'N', 'SB', 'enter', 'N', 'SB'),

    (1, 'S', 'NB', 'thru', 'N', 'NB'),
    (2, 'S', 'NB', 'left', 'W', 'WB'),
    (3, 'S', 'NB', 'right', 'E', 'EB'),
    (4, 'S', 'NB', 'u_turn', 'S', 'SB'),
    (5, 'S', 'WB', 'cw', 'S', 'WB'),
    (6, 'S', 'EB', 'ccw', 'S', 'EB'),
    (7, 'S', 'NB', 'enter', 'S', 'NB'),

    (1, 'W', 'EB', 'thru', 'E', 'EB'),
    (2, 'W', 'EB', 'left', 'N', 'NB'),
    (3, 'W', 'EB', 'right', 'S', 'SB'),
    (4, 'W', 'EB', 'u_turn', 'W', 'WB'),
    (5, 'W', 'NB', 'cw', 'W', 'NB'),
    (6, 'W', 'SB', 'ccw', 'W', 'SB'),
    (7, 'W', 'EB', 'enter', 'W', 'EB')
);