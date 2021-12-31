--DROP TABLE IF EXISTS here.ta CASCADE;

CREATE TABLE here.ta
(
  link_dir text NOT NULL,
  tx timestamp without time zone NOT NULL,
  epoch_min integer NOT NULL,
  length int,
  mean numeric(4,1) NOT NULL,
  stddev numeric(4,1) NOT NULL,
  min_spd integer NOT NULL,
  max_spd integer NOT NULL,
  confidence integer NOT NULL,
  pct_5 int NOT NULL,
pct_10 int NOT NULL,
pct_15 int NOT NULL,
pct_20 int NOT NULL,
pct_25 int NOT NULL,
pct_30 int NOT NULL,
pct_35 int NOT NULL,
pct_40 int NOT NULL,
pct_45 int NOT NULL,
pct_50 int NOT NULL,
pct_55 int NOT NULL,
pct_60 int NOT NULL,
pct_65 int NOT NULL,
pct_70 int NOT NULL,
pct_75 int NOT NULL,
pct_80 int NOT NULL,
pct_85 int NOT NULL,
pct_90 int NOT NULL,
pct_95 int NOT NULL
)
WITH (
  OIDS=FALSE
);
ALTER TABLE here.ta
  OWNER TO here_admins;

	