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
IS '';