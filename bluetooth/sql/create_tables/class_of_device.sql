-- Table: bluetooth.class_of_device

-- DROP TABLE bluetooth.class_of_device;

CREATE TABLE bluetooth.class_of_device
(
    cod_hex bytea NOT NULL,
    cod_binary bit varying(24) NOT NULL,
    device_type character varying(64) NOT NULL,
    device_example text,
    confirmed character varying(10),
    confirmed_example text,
    major_device_class text,
    cod bigint
)
WITH (
    oids = FALSE
);
ALTER TABLE bluetooth.class_of_device
OWNER TO bt_admins;
