-- Parent table structure
CREATE TABLE IF NOT EXISTS here.ta_new
(
    link_dir text NOT NULL,
	dt date NOT NULL,
	tod time without time zone NOT NULL,
	tx timestamp without time zone NOT NULL, 
    length integer,
    mean numeric(4,1) NOT NULL,
    stddev numeric(4,1) NOT NULL,
    min_spd integer NOT NULL,
    max_spd integer NOT NULL,
    pct_50 integer NOT NULL,
    pct_85 integer NOT NULL,
    confidence integer NOT NULL,
	sample_size integer
) PARTITION BY RANGE (dt);

ALTER TABLE here.ta_new
  OWNER TO here_admins;

-- simple view structure
CREATE VIEW here.ta_view AS
 
select link_dir, tx, length, mean, stddev, min_spd, max_spd, pct_50, pct_85, confidence 

from here.ta_new

ALTER TABLE here.ta_view
  OWNER TO here_admins;
  
-- Instead of inserting to ta_view, 
-- insert to parent table trigger

CREATE TRIGGER transform_trigger
        INSTEAD OF INSERT
        ON here.ta_view
        FOR EACH ROW
        EXECUTE PROCEDURE here.here_insert_trigger();


