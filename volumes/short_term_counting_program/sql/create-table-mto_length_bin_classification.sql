--DROP TABLE IF EXISTS traffic.mto_length_bin_classification;

CREATE TABLE traffic.mto_length_bin_classification
(
    mto_class_uid smallserial,
    class_desc text,
    length_range numrange,
    CONSTRAINT mto_length_pkey PRIMARY KEY (mto_class_uid),
    CONSTRAINT mto_length_bin_classification_length_range_excl
    EXCLUDE USING gist (length_range WITH &&)
);

INSERT INTO traffic.mto_length_bin_classification (length_range, class_desc)
(
    VALUES
    (numrange(0.0, 6.5, '[)'), 'Lights'::text),
    (numrange(6.5, 12.5, '[)'), 'Short Trucks'::text),
    (numrange(12.5, 21.1, '[)'), 'Long Trucks'::text),
    (numrange(21.1, 23.0, '[)'), 'Oversize Long Trucks'::text),
    (numrange(23.0, 30.0, '[)'), 'Oversize Vehicles >23m'::text),
    (numrange(30.0, 40.0, '[]'), 'LCV'::text),
    (numrange(40.0, NULL, '()'), 'Unknown (>40m)'::text)
);

COMMENT ON TABLE traffic.mto_length_bin_classification
IS 'MTO 6 length bin classification guide.';

ALTER TABLE traffic.mto_length_bin_classification OWNER TO traffic_admins;
GRANT SELECT ON TABLE traffic.mto_length_bin_classification TO bdit_humans;
GRANT SELECT ON TABLE traffic.mto_length_bin_classification TO vds_bot;