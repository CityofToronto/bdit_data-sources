CREATE TABLE traffic.fhwa_classes (
    class_id int,
    class_desc text
);

INSERT INTO traffic.fhwa_classes VALUES
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

ALTER TABLE traffic.fhwa_classes OWNER TO traffic_admins;
GRANT SELECT ON TABLE traffic.fhwa_classes TO bdit_humans;

COMMENT ON TABLE traffic.fhwa_classes IS E''
'FHWA axle based traffic classifications ids and their description as used by OTI, Spectrum.'
'CAUTION: At some point in 2024 Spectrum will be switching to Houston Radar length based classification method.' -- noqa: L016
'Example: `... FROM traffic.cnt_det JOIN traffic.fhwa_classification ON cnt_det.speed_class = fhwa_classification.class_id`.' -- noqa: L016
'Source: https://www.notion.so/bditto/Feature-Classification-ATRs-27ece0049d654c9ba06136bffc07e2e8?pvs=4#e618feab5f8d4bb48e88f879915cbeab'; -- noqa: L016
