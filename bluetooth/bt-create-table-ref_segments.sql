DROP TABLE IF EXISTS bluetooth.ref_segments;

CREATE TABLE bluetooth.ref_segments
(
  segment_id integer NOT NULL,
  analysis_id integer NOT NULL,
  segment_name character varying(10),
  startpointname character varying(10),
  endpointname character varying(10),
  direction character varying(10),
  start_road character varying(20),
  start_crossstreet character varying(40),
  end_road character varying(20),
  end_crossstreet character varying(40),
  orig_startpointname character varying(10),
  orig_endpointname character varying(10),
  CONSTRAINT segments_pkey PRIMARY KEY (segment_id)
);

ALTER TABLE bluetooth.ref_segments
  OWNER TO aharpal;
GRANT ALL ON TABLE bluetooth.ref_segments TO aharpal;
GRANT ALL ON TABLE bluetooth.ref_segments TO public;

