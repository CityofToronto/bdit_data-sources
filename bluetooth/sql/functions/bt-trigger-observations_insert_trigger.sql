CREATE OR REPLACE FUNCTION bluetooth.observations_insert_trigger()
  RETURNS trigger AS
$BODY$
BEGIN
    IF ( NEW.measured_timestamp > DATE '2020-01-01' AND
         NEW.measured_timestamp <= DATE '2020-02-01' ) THEN
        INSERT INTO bluetooth.observations_202001 VALUES (NEW.*) ON CONFLICT DO NOTHING;
    ELSIF ( NEW.measured_timestamp > DATE '2020-02-01' AND
         NEW.measured_timestamp <= DATE '2020-03-01' ) THEN
        INSERT INTO bluetooth.observations_202002 VALUES (NEW.*) ON CONFLICT DO NOTHING;
    ELSIF ( NEW.measured_timestamp > DATE '2020-03-01' AND
         NEW.measured_timestamp <= DATE '2020-04-01' ) THEN
        INSERT INTO bluetooth.observations_202003 VALUES (NEW.*) ON CONFLICT DO NOTHING;
    ELSIF ( NEW.measured_timestamp > DATE '2020-04-01' AND
         NEW.measured_timestamp <= DATE '2020-05-01' ) THEN
        INSERT INTO bluetooth.observations_202004 VALUES (NEW.*) ON CONFLICT DO NOTHING;
    ELSIF ( NEW.measured_timestamp > DATE '2020-05-01' AND
         NEW.measured_timestamp <= DATE '2020-06-01' ) THEN
        INSERT INTO bluetooth.observations_202005 VALUES (NEW.*) ON CONFLICT DO NOTHING;
    ELSIF ( NEW.measured_timestamp > DATE '2020-06-01' AND
         NEW.measured_timestamp <= DATE '2020-07-01' ) THEN
        INSERT INTO bluetooth.observations_202006 VALUES (NEW.*) ON CONFLICT DO NOTHING;
    ELSIF ( NEW.measured_timestamp > DATE '2020-07-01' AND
         NEW.measured_timestamp <= DATE '2020-08-01' ) THEN
        INSERT INTO bluetooth.observations_202007 VALUES (NEW.*) ON CONFLICT DO NOTHING;
    ELSIF ( NEW.measured_timestamp > DATE '2020-08-01' AND
         NEW.measured_timestamp <= DATE '2020-09-01' ) THEN
        INSERT INTO bluetooth.observations_202008 VALUES (NEW.*) ON CONFLICT DO NOTHING;
    ELSIF ( NEW.measured_timestamp > DATE '2020-09-01' AND
         NEW.measured_timestamp <= DATE '2020-10-01' ) THEN
        INSERT INTO bluetooth.observations_202009 VALUES (NEW.*) ON CONFLICT DO NOTHING;
    ELSIF ( NEW.measured_timestamp > DATE '2020-10-01' AND
         NEW.measured_timestamp <= DATE '2020-11-01' ) THEN
        INSERT INTO bluetooth.observations_202010 VALUES (NEW.*) ON CONFLICT DO NOTHING;
    ELSIF ( NEW.measured_timestamp > DATE '2020-11-01' AND
         NEW.measured_timestamp <= DATE '2020-12-01' ) THEN
        INSERT INTO bluetooth.observations_202011 VALUES (NEW.*) ON CONFLICT DO NOTHING;
    ELSIF ( NEW.measured_timestamp > DATE '2020-12-01' AND
         NEW.measured_timestamp <= DATE '2020-12-01' + INTERVAL '1 month' ) THEN
        INSERT INTO bluetooth.observations_202012 VALUES (NEW.*) ON CONFLICT DO NOTHING;
    ELSIF ( NEW.measured_timestamp > DATE '2014-01-01' AND
         NEW.measured_timestamp <= DATE '2014-02-01' ) THEN
        INSERT INTO bluetooth.observations_201401 VALUES (NEW.*) ON CONFLICT DO NOTHING;
    ELSIF ( NEW.measured_timestamp > DATE '2014-02-01' AND
         NEW.measured_timestamp <= DATE '2014-03-01' ) THEN
        INSERT INTO bluetooth.observations_201402 VALUES (NEW.*) ON CONFLICT DO NOTHING;
    ELSIF ( NEW.measured_timestamp > DATE '2014-03-01' AND
         NEW.measured_timestamp <= DATE '2014-04-01' ) THEN
        INSERT INTO bluetooth.observations_201403 VALUES (NEW.*) ON CONFLICT DO NOTHING;
    ELSIF ( NEW.measured_timestamp > DATE '2014-04-01' AND
         NEW.measured_timestamp <= DATE '2014-05-01' ) THEN
        INSERT INTO bluetooth.observations_201404 VALUES (NEW.*) ON CONFLICT DO NOTHING;
    ELSIF ( NEW.measured_timestamp > DATE '2014-05-01' AND
         NEW.measured_timestamp <= DATE '2014-06-01' ) THEN
        INSERT INTO bluetooth.observations_201405 VALUES (NEW.*) ON CONFLICT DO NOTHING;
    ELSIF ( NEW.measured_timestamp > DATE '2014-06-01' AND
         NEW.measured_timestamp <= DATE '2014-07-01' ) THEN
        INSERT INTO bluetooth.observations_201406 VALUES (NEW.*) ON CONFLICT DO NOTHING;
    ELSIF ( NEW.measured_timestamp > DATE '2014-07-01' AND
         NEW.measured_timestamp <= DATE '2014-08-01' ) THEN
        INSERT INTO bluetooth.observations_201407 VALUES (NEW.*) ON CONFLICT DO NOTHING;
    ELSIF ( NEW.measured_timestamp > DATE '2014-08-01' AND
         NEW.measured_timestamp <= DATE '2014-09-01' ) THEN
        INSERT INTO bluetooth.observations_201408 VALUES (NEW.*) ON CONFLICT DO NOTHING;
    ELSIF ( NEW.measured_timestamp > DATE '2014-09-01' AND
         NEW.measured_timestamp <= DATE '2014-10-01' ) THEN
        INSERT INTO bluetooth.observations_201409 VALUES (NEW.*) ON CONFLICT DO NOTHING;
    ELSIF ( NEW.measured_timestamp > DATE '2014-10-01' AND
         NEW.measured_timestamp <= DATE '2014-11-01' ) THEN
        INSERT INTO bluetooth.observations_201410 VALUES (NEW.*) ON CONFLICT DO NOTHING;
    ELSIF ( NEW.measured_timestamp > DATE '2014-11-01' AND
         NEW.measured_timestamp <= DATE '2014-12-01' ) THEN
        INSERT INTO bluetooth.observations_201411 VALUES (NEW.*) ON CONFLICT DO NOTHING;
    ELSIF ( NEW.measured_timestamp > DATE '2014-12-01' AND
         NEW.measured_timestamp <= DATE '2015-01-01' ) THEN
        INSERT INTO bluetooth.observations_201412 VALUES (NEW.*) ON CONFLICT DO NOTHING;
    ELSIF ( NEW.measured_timestamp > DATE '2015-01-01' AND
         NEW.measured_timestamp <= DATE '2015-02-01' ) THEN
        INSERT INTO bluetooth.observations_201501 VALUES (NEW.*) ON CONFLICT DO NOTHING;
    ELSIF ( NEW.measured_timestamp > DATE '2015-02-01' AND
         NEW.measured_timestamp <= DATE '2015-03-01' ) THEN
        INSERT INTO bluetooth.observations_201502 VALUES (NEW.*) ON CONFLICT DO NOTHING;
    ELSIF ( NEW.measured_timestamp > DATE '2015-03-01' AND
         NEW.measured_timestamp <= DATE '2015-04-01' ) THEN
        INSERT INTO bluetooth.observations_201503 VALUES (NEW.*) ON CONFLICT DO NOTHING;
    ELSIF ( NEW.measured_timestamp > DATE '2015-04-01' AND
         NEW.measured_timestamp <= DATE '2015-05-01' ) THEN
        INSERT INTO bluetooth.observations_201504 VALUES (NEW.*) ON CONFLICT DO NOTHING;
    ELSIF ( NEW.measured_timestamp > DATE '2015-05-01' AND
         NEW.measured_timestamp <= DATE '2015-06-01' ) THEN
        INSERT INTO bluetooth.observations_201505 VALUES (NEW.*) ON CONFLICT DO NOTHING;
    ELSIF ( NEW.measured_timestamp > DATE '2015-06-01' AND
         NEW.measured_timestamp <= DATE '2015-07-01' ) THEN
        INSERT INTO bluetooth.observations_201506 VALUES (NEW.*) ON CONFLICT DO NOTHING;
    ELSIF ( NEW.measured_timestamp > DATE '2015-07-01' AND
         NEW.measured_timestamp <= DATE '2015-08-01' ) THEN
        INSERT INTO bluetooth.observations_201507 VALUES (NEW.*) ON CONFLICT DO NOTHING;
    ELSIF ( NEW.measured_timestamp > DATE '2015-08-01' AND
         NEW.measured_timestamp <= DATE '2015-09-01' ) THEN
        INSERT INTO bluetooth.observations_201508 VALUES (NEW.*) ON CONFLICT DO NOTHING;
    ELSIF ( NEW.measured_timestamp > DATE '2015-09-01' AND
         NEW.measured_timestamp <= DATE '2015-10-01' ) THEN
        INSERT INTO bluetooth.observations_201509 VALUES (NEW.*) ON CONFLICT DO NOTHING;
    ELSIF ( NEW.measured_timestamp > DATE '2015-10-01' AND
         NEW.measured_timestamp <= DATE '2015-11-01' ) THEN
        INSERT INTO bluetooth.observations_201510 VALUES (NEW.*) ON CONFLICT DO NOTHING;
    ELSIF ( NEW.measured_timestamp > DATE '2015-11-01' AND
         NEW.measured_timestamp <= DATE '2015-12-01' ) THEN
        INSERT INTO bluetooth.observations_201511 VALUES (NEW.*) ON CONFLICT DO NOTHING;
    ELSIF ( NEW.measured_timestamp > DATE '2015-12-01' AND
         NEW.measured_timestamp <= DATE '2016-01-01' ) THEN
        INSERT INTO bluetooth.observations_201512 VALUES (NEW.*) ON CONFLICT DO NOTHING;
    ELSIF ( NEW.measured_timestamp > DATE '2016-01-01' AND
         NEW.measured_timestamp <= DATE '2016-02-01' ) THEN
        INSERT INTO bluetooth.observations_201601 VALUES (NEW.*) ON CONFLICT DO NOTHING;
    ELSIF ( NEW.measured_timestamp > DATE '2016-02-01' AND
         NEW.measured_timestamp <= DATE '2016-03-01' ) THEN
        INSERT INTO bluetooth.observations_201602 VALUES (NEW.*) ON CONFLICT DO NOTHING;
    ELSIF ( NEW.measured_timestamp > DATE '2016-03-01' AND
         NEW.measured_timestamp <= DATE '2016-04-01' ) THEN
        INSERT INTO bluetooth.observations_201603 VALUES (NEW.*) ON CONFLICT DO NOTHING;
    ELSIF ( NEW.measured_timestamp > DATE '2016-04-01' AND
         NEW.measured_timestamp <= DATE '2016-05-01' ) THEN
        INSERT INTO bluetooth.observations_201604 VALUES (NEW.*) ON CONFLICT DO NOTHING;
    ELSIF ( NEW.measured_timestamp > DATE '2016-05-01' AND
         NEW.measured_timestamp <= DATE '2016-06-01' ) THEN
        INSERT INTO bluetooth.observations_201605 VALUES (NEW.*) ON CONFLICT DO NOTHING;
    ELSIF ( NEW.measured_timestamp > DATE '2016-06-01' AND
         NEW.measured_timestamp <= DATE '2016-07-01' ) THEN
        INSERT INTO bluetooth.observations_201606 VALUES (NEW.*) ON CONFLICT DO NOTHING;
    ELSIF ( NEW.measured_timestamp > DATE '2016-07-01' AND
         NEW.measured_timestamp <= DATE '2016-08-01' ) THEN
        INSERT INTO bluetooth.observations_201607 VALUES (NEW.*) ON CONFLICT DO NOTHING;
    ELSIF ( NEW.measured_timestamp > DATE '2016-08-01' AND
         NEW.measured_timestamp <= DATE '2016-09-01' ) THEN
        INSERT INTO bluetooth.observations_201608 VALUES (NEW.*) ON CONFLICT DO NOTHING;
    ELSIF ( NEW.measured_timestamp > DATE '2016-09-01' AND
         NEW.measured_timestamp <= DATE '2016-10-01' ) THEN
        INSERT INTO bluetooth.observations_201609 VALUES (NEW.*) ON CONFLICT DO NOTHING;
    ELSIF ( NEW.measured_timestamp > DATE '2016-10-01' AND
         NEW.measured_timestamp <= DATE '2016-11-01' ) THEN
        INSERT INTO bluetooth.observations_201610 VALUES (NEW.*) ON CONFLICT DO NOTHING;
    ELSIF ( NEW.measured_timestamp > DATE '2016-11-01' AND
         NEW.measured_timestamp <= DATE '2016-12-01' ) THEN
        INSERT INTO bluetooth.observations_201611 VALUES (NEW.*) ON CONFLICT DO NOTHING;
    ELSIF ( NEW.measured_timestamp > DATE '2016-12-01' AND
         NEW.measured_timestamp <= DATE '2017-01-01' ) THEN
        INSERT INTO bluetooth.observations_201612 VALUES (NEW.*) ON CONFLICT DO NOTHING;
    ELSIF ( NEW.measured_timestamp > DATE '2017-01-01' AND
         NEW.measured_timestamp <= DATE '2017-02-01' ) THEN
        INSERT INTO bluetooth.observations_201701 VALUES (NEW.*) ON CONFLICT DO NOTHING;
    ELSIF ( NEW.measured_timestamp > DATE '2017-02-01' AND
         NEW.measured_timestamp <= DATE '2017-03-01' ) THEN
        INSERT INTO bluetooth.observations_201702 VALUES (NEW.*) ON CONFLICT DO NOTHING;
    ELSIF ( NEW.measured_timestamp > DATE '2017-03-01' AND
         NEW.measured_timestamp <= DATE '2017-04-01' ) THEN
        INSERT INTO bluetooth.observations_201703 VALUES (NEW.*) ON CONFLICT DO NOTHING;
    ELSIF ( NEW.measured_timestamp > DATE '2017-04-01' AND
         NEW.measured_timestamp <= DATE '2017-05-01' ) THEN
        INSERT INTO bluetooth.observations_201704 VALUES (NEW.*) ON CONFLICT DO NOTHING;
    ELSIF ( NEW.measured_timestamp > DATE '2017-05-01' AND
         NEW.measured_timestamp <= DATE '2017-06-01' ) THEN
        INSERT INTO bluetooth.observations_201705 VALUES (NEW.*) ON CONFLICT DO NOTHING;
    ELSIF ( NEW.measured_timestamp > DATE '2017-06-01' AND
         NEW.measured_timestamp <= DATE '2017-07-01' ) THEN
        INSERT INTO bluetooth.observations_201706 VALUES (NEW.*) ON CONFLICT DO NOTHING;
    ELSIF ( NEW.measured_timestamp > DATE '2017-07-01' AND
         NEW.measured_timestamp <= DATE '2017-08-01' ) THEN
        INSERT INTO bluetooth.observations_201707 VALUES (NEW.*) ON CONFLICT DO NOTHING;
    ELSIF ( NEW.measured_timestamp > DATE '2017-08-01' AND
         NEW.measured_timestamp <= DATE '2017-09-01' ) THEN
        INSERT INTO bluetooth.observations_201708 VALUES (NEW.*) ON CONFLICT DO NOTHING;
    ELSIF ( NEW.measured_timestamp > DATE '2017-09-01' AND
         NEW.measured_timestamp <= DATE '2017-10-01' ) THEN
        INSERT INTO bluetooth.observations_201709 VALUES (NEW.*) ON CONFLICT DO NOTHING;
    ELSIF ( NEW.measured_timestamp > DATE '2017-10-01' AND
         NEW.measured_timestamp <= DATE '2017-11-01' ) THEN
        INSERT INTO bluetooth.observations_201710 VALUES (NEW.*) ON CONFLICT DO NOTHING;
    ELSIF ( NEW.measured_timestamp > DATE '2017-11-01' AND
         NEW.measured_timestamp <= DATE '2017-12-01' ) THEN
        INSERT INTO bluetooth.observations_201711 VALUES (NEW.*) ON CONFLICT DO NOTHING;
    ELSIF ( NEW.measured_timestamp > DATE '2017-12-01' AND
         NEW.measured_timestamp <= DATE '2018-01-01' ) THEN
        INSERT INTO bluetooth.observations_201712 VALUES (NEW.*) ON CONFLICT DO NOTHING;
    ELSIF ( NEW.measured_timestamp > DATE '2018-01-01' AND
         NEW.measured_timestamp <= DATE '2018-02-01' ) THEN
        INSERT INTO bluetooth.observations_201801 VALUES (NEW.*) ON CONFLICT DO NOTHING;
    ELSIF ( NEW.measured_timestamp > DATE '2018-02-01' AND
         NEW.measured_timestamp <= DATE '2018-03-01' ) THEN
        INSERT INTO bluetooth.observations_201802 VALUES (NEW.*) ON CONFLICT DO NOTHING;
    ELSIF ( NEW.measured_timestamp > DATE '2018-03-01' AND NEW.measured_timestamp <= DATE '2018-03-01' + INTERVAL '1 month' ) THEN
        INSERT INTO bluetooth.observations_201803 VALUES (NEW.*) ON CONFLICT DO NOTHING;
    ELSIF ( NEW.measured_timestamp > DATE '2018-04-01' AND NEW.measured_timestamp <= DATE '2018-04-01' + INTERVAL '1 month' ) THEN
        INSERT INTO bluetooth.observations_201804 VALUES (NEW.*) ON CONFLICT DO NOTHING;
    ELSIF ( NEW.measured_timestamp > DATE '2018-04-01' AND NEW.measured_timestamp <= DATE '2018-04-01' + INTERVAL '1 month' ) THEN
        INSERT INTO bluetooth.observations_201804 VALUES (NEW.*) ON CONFLICT DO NOTHING;
    ELSIF ( NEW.measured_timestamp > DATE '2018-05-01' AND NEW.measured_timestamp <= DATE '2018-05-01' + INTERVAL '1 month' ) THEN
        INSERT INTO bluetooth.observations_201805 VALUES (NEW.*) ON CONFLICT DO NOTHING;
    ELSIF ( NEW.measured_timestamp > DATE '2018-06-01' AND NEW.measured_timestamp <= DATE '2018-06-01' + INTERVAL '1 month' ) THEN
        INSERT INTO bluetooth.observations_201806 VALUES (NEW.*) ON CONFLICT DO NOTHING;
    ELSIF ( NEW.measured_timestamp > DATE '2018-07-01' AND NEW.measured_timestamp <= DATE '2018-07-01' + INTERVAL '1 month' ) THEN
        INSERT INTO bluetooth.observations_201807 VALUES (NEW.*) ON CONFLICT DO NOTHING;
    ELSIF ( NEW.measured_timestamp > DATE '2018-08-01' AND NEW.measured_timestamp <= DATE '2018-08-01' + INTERVAL '1 month' ) THEN
        INSERT INTO bluetooth.observations_201808 VALUES (NEW.*) ON CONFLICT DO NOTHING;
    ELSIF ( NEW.measured_timestamp > DATE '2018-09-01' AND NEW.measured_timestamp <= DATE '2018-09-01' + INTERVAL '1 month' ) THEN
        INSERT INTO bluetooth.observations_201809 VALUES (NEW.*) ON CONFLICT DO NOTHING;
    ELSIF ( NEW.measured_timestamp > DATE '2018-10-01' AND NEW.measured_timestamp <= DATE '2018-10-01' + INTERVAL '1 month' ) THEN
        INSERT INTO bluetooth.observations_201810 VALUES (NEW.*) ON CONFLICT DO NOTHING;
    ELSIF ( NEW.measured_timestamp > DATE '2018-11-01' AND NEW.measured_timestamp <= DATE '2018-11-01' + INTERVAL '1 month' ) THEN
        INSERT INTO bluetooth.observations_201811 VALUES (NEW.*) ON CONFLICT DO NOTHING;
    ELSIF ( NEW.measured_timestamp > DATE '2018-12-01' AND NEW.measured_timestamp <= DATE '2018-12-01' + INTERVAL '1 month' ) THEN
        INSERT INTO bluetooth.observations_201812 VALUES (NEW.*) ON CONFLICT DO NOTHING;
     ELSIF ( NEW.measured_timestamp > DATE '2019-01-01' AND
         NEW.measured_timestamp <= DATE '2019-02-01' ) THEN
        INSERT INTO bluetooth.observations_201901 VALUES (NEW.*) ON CONFLICT DO NOTHING;
    ELSIF ( NEW.measured_timestamp > DATE '2019-02-01' AND
         NEW.measured_timestamp <= DATE '2019-03-01' ) THEN
        INSERT INTO bluetooth.observations_201902 VALUES (NEW.*) ON CONFLICT DO NOTHING;
    ELSIF ( NEW.measured_timestamp > DATE '2019-03-01' AND NEW.measured_timestamp <= DATE '2019-03-01' + INTERVAL '1 month' ) THEN
        INSERT INTO bluetooth.observations_201903 VALUES (NEW.*) ON CONFLICT DO NOTHING;
    ELSIF ( NEW.measured_timestamp > DATE '2019-04-01' AND NEW.measured_timestamp <= DATE '2019-04-01' + INTERVAL '1 month' ) THEN
        INSERT INTO bluetooth.observations_201904 VALUES (NEW.*) ON CONFLICT DO NOTHING;
    ELSIF ( NEW.measured_timestamp > DATE '2019-04-01' AND NEW.measured_timestamp <= DATE '2019-04-01' + INTERVAL '1 month' ) THEN
        INSERT INTO bluetooth.observations_201904 VALUES (NEW.*) ON CONFLICT DO NOTHING;
    ELSIF ( NEW.measured_timestamp > DATE '2019-05-01' AND NEW.measured_timestamp <= DATE '2019-05-01' + INTERVAL '1 month' ) THEN
        INSERT INTO bluetooth.observations_201905 VALUES (NEW.*) ON CONFLICT DO NOTHING;
    ELSIF ( NEW.measured_timestamp > DATE '2019-06-01' AND NEW.measured_timestamp <= DATE '2019-06-01' + INTERVAL '1 month' ) THEN
        INSERT INTO bluetooth.observations_201906 VALUES (NEW.*) ON CONFLICT DO NOTHING;
    ELSIF ( NEW.measured_timestamp > DATE '2019-07-01' AND NEW.measured_timestamp <= DATE '2019-07-01' + INTERVAL '1 month' ) THEN
        INSERT INTO bluetooth.observations_201907 VALUES (NEW.*) ON CONFLICT DO NOTHING;
    ELSIF ( NEW.measured_timestamp > DATE '2019-08-01' AND NEW.measured_timestamp <= DATE '2019-08-01' + INTERVAL '1 month' ) THEN
        INSERT INTO bluetooth.observations_201908 VALUES (NEW.*) ON CONFLICT DO NOTHING;
    ELSIF ( NEW.measured_timestamp > DATE '2019-09-01' AND NEW.measured_timestamp <= DATE '2019-09-01' + INTERVAL '1 month' ) THEN
        INSERT INTO bluetooth.observations_201909 VALUES (NEW.*) ON CONFLICT DO NOTHING;
    ELSIF ( NEW.measured_timestamp > DATE '2019-10-01' AND NEW.measured_timestamp <= DATE '2019-10-01' + INTERVAL '1 month' ) THEN
        INSERT INTO bluetooth.observations_201910 VALUES (NEW.*) ON CONFLICT DO NOTHING;
    ELSIF ( NEW.measured_timestamp > DATE '2019-11-01' AND NEW.measured_timestamp <= DATE '2019-11-01' + INTERVAL '1 month' ) THEN
        INSERT INTO bluetooth.observations_201911 VALUES (NEW.*) ON CONFLICT DO NOTHING;
    ELSIF ( NEW.measured_timestamp > DATE '2019-12-01' AND NEW.measured_timestamp <= DATE '2019-12-01' + INTERVAL '1 month' ) THEN
        INSERT INTO bluetooth.observations_201912 VALUES (NEW.*) ON CONFLICT DO NOTHING;
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
