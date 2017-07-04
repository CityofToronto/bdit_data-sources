/*DROP TABLE IF EXISTS here.ta CASCADE;

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

CREATE TABLE IF NOT EXISTS here.ta_staging (
LIKE here.ta)
;
ALTER TABLE here.ta_staging
OWNER TO here_admins;*/

/*Loops through the years and then months for which we currently have data in order to create a partitioned table for each month*/

DO $do$
DECLARE
	startdate DATE;
	yyyymm TEXT;
	basetablename TEXT := 'ta_';
	baserulename TEXT := 'here_ta_insert_';
	rulename TEXT;
	tablename TEXT;
BEGIN

	for yyyy IN 2012..2016 LOOP
		FOR mm IN 01..12 LOOP
			startdate:= to_date(yyyy||'-'||mm||'-01', 'YYYY-MM-DD');
			IF mm < 10 THEN
				yyyymm:= yyyy||'0'||mm;
			ELSE
				yyyymm:= yyyy||''||mm;
			END IF;
			tablename:= basetablename||yyyymm;
			EXECUTE format($$CREATE TABLE here.%I 
				(--CHECK (tx >= DATE '$$||startdate ||$$'AND tx < DATE '$$||startdate ||$$'+ INTERVAL '1 month')
				) INHERITS (here.ta)$$
				, tablename);
			EXECUTE format($$ALTER TABLE here.%I OWNER TO here_admins$$, tablename);
			rulename := baserulename||yyyymm;
			EXECUTE format($$CREATE OR REPLACE RULE %I AS
				    ON INSERT TO here.ta
				   WHERE new.tx >= %L::date AND new.tx < (%L::date + '1 mon'::interval) 
				   DO INSTEAD  INSERT INTO here.%I 
				  VALUES (new.*)
				  $$, rulename, startdate, startdate, tablename);
		END LOOP;
	END LOOP;
END;
$do$ LANGUAGE plpgsql
	