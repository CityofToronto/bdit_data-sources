CREATE OR REPLACE FUNCTION bluetooth.observations_insert_trigger()
  RETURNS trigger AS
$BODY$
BEGIN
    IF ( NEW.measured_timestamp > DATE '2014-01-01' AND
         NEW.measured_timestamp <= DATE '2014-02-01' ) THEN
        INSERT INTO bluetooth.observations_201401 VALUES (NEW.*);
    ELSIF ( NEW.measured_timestamp > DATE '2014-02-01' AND
         NEW.measured_timestamp <= DATE '2014-03-01' ) THEN
        INSERT INTO bluetooth.observations_201402 VALUES (NEW.*);
    ELSIF ( NEW.measured_timestamp > DATE '2014-03-01' AND
         NEW.measured_timestamp <= DATE '2014-04-01' ) THEN
        INSERT INTO bluetooth.observations_201403 VALUES (NEW.*);
    ELSIF ( NEW.measured_timestamp > DATE '2014-04-01' AND
         NEW.measured_timestamp <= DATE '2014-05-01' ) THEN
        INSERT INTO bluetooth.observations_201404 VALUES (NEW.*);
    ELSIF ( NEW.measured_timestamp > DATE '2014-05-01' AND
         NEW.measured_timestamp <= DATE '2014-06-01' ) THEN
        INSERT INTO bluetooth.observations_201405 VALUES (NEW.*);
    ELSIF ( NEW.measured_timestamp > DATE '2014-06-01' AND
         NEW.measured_timestamp <= DATE '2014-07-01' ) THEN
        INSERT INTO bluetooth.observations_201406 VALUES (NEW.*);
    ELSIF ( NEW.measured_timestamp > DATE '2014-07-01' AND
         NEW.measured_timestamp <= DATE '2014-08-01' ) THEN
        INSERT INTO bluetooth.observations_201407 VALUES (NEW.*);
    ELSIF ( NEW.measured_timestamp > DATE '2014-08-01' AND
         NEW.measured_timestamp <= DATE '2014-09-01' ) THEN
        INSERT INTO bluetooth.observations_201408 VALUES (NEW.*);
    ELSIF ( NEW.measured_timestamp > DATE '2014-09-01' AND
         NEW.measured_timestamp <= DATE '2014-10-01' ) THEN
        INSERT INTO bluetooth.observations_201409 VALUES (NEW.*);
    ELSIF ( NEW.measured_timestamp > DATE '2014-10-01' AND
         NEW.measured_timestamp <= DATE '2014-11-01' ) THEN
        INSERT INTO bluetooth.observations_201410 VALUES (NEW.*);
    ELSIF ( NEW.measured_timestamp > DATE '2014-11-01' AND
         NEW.measured_timestamp <= DATE '2014-12-01' ) THEN
        INSERT INTO bluetooth.observations_201411 VALUES (NEW.*);
    ELSIF ( NEW.measured_timestamp > DATE '2014-12-01' AND
         NEW.measured_timestamp <= DATE '2015-01-01' ) THEN
        INSERT INTO bluetooth.observations_201412 VALUES (NEW.*);
    ELSIF ( NEW.measured_timestamp > DATE '2015-01-01' AND
         NEW.measured_timestamp <= DATE '2015-02-01' ) THEN
        INSERT INTO bluetooth.observations_201501 VALUES (NEW.*);
    ELSIF ( NEW.measured_timestamp > DATE '2015-02-01' AND
         NEW.measured_timestamp <= DATE '2015-03-01' ) THEN
        INSERT INTO bluetooth.observations_201502 VALUES (NEW.*);
    ELSIF ( NEW.measured_timestamp > DATE '2015-03-01' AND
         NEW.measured_timestamp <= DATE '2015-04-01' ) THEN
        INSERT INTO bluetooth.observations_201503 VALUES (NEW.*);
    ELSIF ( NEW.measured_timestamp > DATE '2015-04-01' AND
         NEW.measured_timestamp <= DATE '2015-05-01' ) THEN
        INSERT INTO bluetooth.observations_201504 VALUES (NEW.*);
    ELSIF ( NEW.measured_timestamp > DATE '2015-05-01' AND
         NEW.measured_timestamp <= DATE '2015-06-01' ) THEN
        INSERT INTO bluetooth.observations_201505 VALUES (NEW.*);
    ELSIF ( NEW.measured_timestamp > DATE '2015-06-01' AND
         NEW.measured_timestamp <= DATE '2015-07-01' ) THEN
        INSERT INTO bluetooth.observations_201506 VALUES (NEW.*);
    ELSIF ( NEW.measured_timestamp > DATE '2015-07-01' AND
         NEW.measured_timestamp <= DATE '2015-08-01' ) THEN
        INSERT INTO bluetooth.observations_201507 VALUES (NEW.*);
    ELSIF ( NEW.measured_timestamp > DATE '2015-08-01' AND
         NEW.measured_timestamp <= DATE '2015-09-01' ) THEN
        INSERT INTO bluetooth.observations_201508 VALUES (NEW.*);
    ELSIF ( NEW.measured_timestamp > DATE '2015-09-01' AND
         NEW.measured_timestamp <= DATE '2015-10-01' ) THEN
        INSERT INTO bluetooth.observations_201509 VALUES (NEW.*);
    ELSIF ( NEW.measured_timestamp > DATE '2015-10-01' AND
         NEW.measured_timestamp <= DATE '2015-11-01' ) THEN
        INSERT INTO bluetooth.observations_201510 VALUES (NEW.*);
    ELSIF ( NEW.measured_timestamp > DATE '2015-11-01' AND
         NEW.measured_timestamp <= DATE '2015-12-01' ) THEN
        INSERT INTO bluetooth.observations_201511 VALUES (NEW.*);
    ELSIF ( NEW.measured_timestamp > DATE '2015-12-01' AND
         NEW.measured_timestamp <= DATE '2016-01-01' ) THEN
        INSERT INTO bluetooth.observations_201512 VALUES (NEW.*);
    ELSIF ( NEW.measured_timestamp > DATE '2016-01-01' AND
         NEW.measured_timestamp <= DATE '2016-02-01' ) THEN
        INSERT INTO bluetooth.observations_201601 VALUES (NEW.*);
    ELSIF ( NEW.measured_timestamp > DATE '2016-02-01' AND
         NEW.measured_timestamp <= DATE '2016-03-01' ) THEN
        INSERT INTO bluetooth.observations_201602 VALUES (NEW.*);
    ELSIF ( NEW.measured_timestamp > DATE '2016-03-01' AND
         NEW.measured_timestamp <= DATE '2016-04-01' ) THEN
        INSERT INTO bluetooth.observations_201603 VALUES (NEW.*);
    ELSIF ( NEW.measured_timestamp > DATE '2016-04-01' AND
         NEW.measured_timestamp <= DATE '2016-05-01' ) THEN
        INSERT INTO bluetooth.observations_201604 VALUES (NEW.*);
    ELSIF ( NEW.measured_timestamp > DATE '2016-05-01' AND
         NEW.measured_timestamp <= DATE '2016-06-01' ) THEN
        INSERT INTO bluetooth.observations_201605 VALUES (NEW.*);
    ELSIF ( NEW.measured_timestamp > DATE '2016-06-01' AND
         NEW.measured_timestamp <= DATE '2016-07-01' ) THEN
        INSERT INTO bluetooth.observations_201606 VALUES (NEW.*);
    ELSIF ( NEW.measured_timestamp > DATE '2016-07-01' AND
         NEW.measured_timestamp <= DATE '2016-08-01' ) THEN
        INSERT INTO bluetooth.observations_201607 VALUES (NEW.*);
    ELSIF ( NEW.measured_timestamp > DATE '2016-08-01' AND
         NEW.measured_timestamp <= DATE '2016-09-01' ) THEN
        INSERT INTO bluetooth.observations_201608 VALUES (NEW.*);
    ELSIF ( NEW.measured_timestamp > DATE '2016-09-01' AND
         NEW.measured_timestamp <= DATE '2016-10-01' ) THEN
        INSERT INTO bluetooth.observations_201609 VALUES (NEW.*);
    ELSIF ( NEW.measured_timestamp > DATE '2016-10-01' AND
         NEW.measured_timestamp <= DATE '2016-11-01' ) THEN
        INSERT INTO bluetooth.observations_201610 VALUES (NEW.*);
    ELSIF ( NEW.measured_timestamp > DATE '2016-11-01' AND
         NEW.measured_timestamp <= DATE '2016-12-01' ) THEN
        INSERT INTO bluetooth.observations_201611 VALUES (NEW.*);
    ELSIF ( NEW.measured_timestamp > DATE '2016-12-01' AND
         NEW.measured_timestamp <= DATE '2017-01-01' ) THEN
        INSERT INTO bluetooth.observations_201612 VALUES (NEW.*);
    ELSIF ( NEW.measured_timestamp > DATE '2017-01-01' AND
         NEW.measured_timestamp <= DATE '2017-02-01' ) THEN
        INSERT INTO bluetooth.observations_201701 VALUES (NEW.*);
    ELSIF ( NEW.measured_timestamp > DATE '2017-02-01' AND
         NEW.measured_timestamp <= DATE '2017-03-01' ) THEN
        INSERT INTO bluetooth.observations_201702 VALUES (NEW.*);
    ELSIF ( NEW.measured_timestamp > DATE '2017-03-01' AND
         NEW.measured_timestamp <= DATE '2017-04-01' ) THEN
        INSERT INTO bluetooth.observations_201703 VALUES (NEW.*);
    ELSIF ( NEW.measured_timestamp > DATE '2017-04-01' AND
         NEW.measured_timestamp <= DATE '2017-05-01' ) THEN
        INSERT INTO bluetooth.observations_201704 VALUES (NEW.*);
    ELSIF ( NEW.measured_timestamp > DATE '2017-05-01' AND
         NEW.measured_timestamp <= DATE '2017-06-01' ) THEN
        INSERT INTO bluetooth.observations_201705 VALUES (NEW.*);
    ELSIF ( NEW.measured_timestamp > DATE '2017-06-01' AND
         NEW.measured_timestamp <= DATE '2017-07-01' ) THEN
        INSERT INTO bluetooth.observations_201706 VALUES (NEW.*);
    ELSIF ( NEW.measured_timestamp > DATE '2017-07-01' AND
         NEW.measured_timestamp <= DATE '2017-08-01' ) THEN
        INSERT INTO bluetooth.observations_201707 VALUES (NEW.*);
    ELSIF ( NEW.measured_timestamp > DATE '2017-08-01' AND
         NEW.measured_timestamp <= DATE '2017-09-01' ) THEN
        INSERT INTO bluetooth.observations_201708 VALUES (NEW.*);
    ELSIF ( NEW.measured_timestamp > DATE '2017-09-01' AND
         NEW.measured_timestamp <= DATE '2017-10-01' ) THEN
        INSERT INTO bluetooth.observations_201709 VALUES (NEW.*);
    ELSIF ( NEW.measured_timestamp > DATE '2017-10-01' AND
         NEW.measured_timestamp <= DATE '2017-11-01' ) THEN
        INSERT INTO bluetooth.observations_201710 VALUES (NEW.*);
    ELSIF ( NEW.measured_timestamp > DATE '2017-11-01' AND
         NEW.measured_timestamp <= DATE '2017-12-01' ) THEN
        INSERT INTO bluetooth.observations_201711 VALUES (NEW.*);
    ELSIF ( NEW.measured_timestamp > DATE '2017-12-01' AND
         NEW.measured_timestamp <= DATE '2018-01-01' ) THEN
        INSERT INTO bluetooth.observations_201712 VALUES (NEW.*);
    ELSE
        RAISE EXCEPTION 'Date out of range.';
    END IF;
    RETURN NULL;
END;
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;
ALTER FUNCTION bluetooth.observations_insert_trigger()
  OWNER TO bt_admins;
