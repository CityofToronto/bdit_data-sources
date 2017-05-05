DROP TABLE IF EXISTS bluetooth.dl_data;
CREATE TABLE bluetooth.dl_data (
  measured_bin timestamp without time zone,
  startpoint_number smallint,
  startpoint_name character varying(8),
  endpoint_number smallint,
  endpoint_name character varying(8),
  minmeasuredtime smallint,
  maxmeasuredtime smallint,
  avgmeasuredtime smallint,
  medianmeasuredtime smallint,
  samplecount smallint,
  accuracylevel smallint,
  confidencelevel smallint);