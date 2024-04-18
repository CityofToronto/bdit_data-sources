CREATE TABLE traffic.oti_class (
    class_id int,
    class_desc text
);

INSERT INTO traffic.oti_class VALUES 
(1, 'motorcycles'),
(2, 'passenger cars'),
(3, 'two axle, 4 tire single units'),
(4, 'buses'),
(5, 'two axis, 6 tire single units'),
(6, 'three axle single units'),
(7, 'four or more axle single units'),
(8, 'four or less axle single trailers'),
(9, 'five axle single trailers'),
(10, 'six or more axle single trailers'),
(11, 'five or less axle multi-trailers'),
(12, 'six axle multi-trailers'),
(13, 'seven or more axle multi-trailers');

ALTER TABLE traffic.oti_class TO traffic_admins;
GRANT SELECT ON TABLE traffic.oti_class TO bdit_humans;

COMMENT ON TABLE traffic.oti_class
IS 'Traffic classifications ids and their description as used by OTI.
Join to `traffic.cnt_det` on `cnt_det.speed_class = `oti_class.oti_class`.
Source: https://www.notion.so/bditto/Feature-Classification-ATRs-27ece0049d654c9ba06136bffc07e2e8?pvs=4#e618feab5f8d4bb48e88f879915cbeab'; -- noqa: L016