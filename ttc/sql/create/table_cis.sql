CREATE TABLE ttc.cis (
    message_datetime timestamp not null,
    route INT NOT NULL,
    run INT NOT NULL,
    vehicle INT NOT NULL,
    latitude TEXT NOT NULL,
    longitude TEXT NOT NULL,
    position geometry(Point, 4326),
    PRIMARY KEY(vehicle, message_datetime)
);

CREATE TRIGGER insert_cis
  BEFORE INSERT
  ON ttc.cis
  FOR EACH ROW
  EXECUTE PROCEDURE ttc.cis_insert_trigger();

CREATE TABLE ttc.cis_2017 (
    CHECK (message_datetime >= '2017-01-01' AND message_datetime < ('2017-01-01'::DATE + INTERVAL '1 year'))
)INHERITS (ttc.cis);

CREATE TRIGGER cis_2017_mk_geom BEFORE INSERT ON ttc.cis_2017 FOR EACH ROW EXECUTE PROCEDURE ttc.trg_mk_position_geom();

CREATE TABLE ttc.cis_2018 (
    
    CHECK (message_datetime >= '2018-01-01' AND message_datetime < ('2018-01-01'::DATE + INTERVAL '1 year'))
)INHERITS (ttc.cis);

CREATE TRIGGER cis_2018_mk_geom BEFORE INSERT ON ttc.cis_2018 FOR EACH ROW EXECUTE PROCEDURE ttc.trg_mk_position_geom();



