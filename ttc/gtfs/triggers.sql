
create or replace function gtfs.calendar_dates_insert() returns trigger as
$BODY$
BEGIN
INSERT INTO gtfs.calendar_dates_imp
VALUES(NEW.service_id, to_date(NEW.date_::TEXT, 'YYYYMMDD'), NEW.exception_type, NEW.feed_id);
RETURN NULL;
END;
$BODY$
  LANGUAGE plpgsql VOLATILE SECURITY DEFINER
  COST 100;
ALTER FUNCTION gtfs.calendar_dates_insert()
  OWNER TO rdumas;

create trigger calendar_dates_insert BEFORE insert on gtfs.calendar_dates for each row execute procedure gtfs.calendar_dates_insert();

create or replace function gtfs.calendar_insert() returns trigger as
$BODY$
BEGIN
INSERT INTO gtfs.calendar_imp
VALUES (
NEW.service_id,
NEW.monday = 1,
NEW.tuesday = 1,
NEW.wednesday = 1,
NEW.thursday = 1,
NEW.friday = 1,
NEW.saturday = 1,
NEW.sunday = 1,
to_date(NEW.start_date::TEXT, 'YYYYMMDD'),
to_date(NEW.end_date::TEXT, 'YYYYMMDD'),
NEW.feed_id);
RETURN NULL;
END;
$BODY$
  LANGUAGE plpgsql VOLATILE SECURITY DEFINER
  COST 100;
ALTER FUNCTION gtfs.calendar_insert()
  OWNER TO rdumas;

  create trigger calendar_insert BEFORE insert on gtfs.calendar for each row execute procedure gtfs.calendar_insert();


CREATE OR REPLACE FUNCTION gtfs.trg_mk_geom()
  RETURNS trigger AS
$BODY$
    BEGIN

    NEW.geom := ST_GeomFromText('POINT('||NEW.stop_lon||' '||NEW.stop_lat||')', 4326);

    RETURN NEW;

END;
$BODY$
  LANGUAGE plpgsql VOLATILE SECURITY DEFINER
  COST 100;
ALTER FUNCTION gtfs.trg_mk_geom()
  OWNER TO rdumas;

  create trigger create_geom BEFORE INSERT on gtfs.stops for each row execute procedure gtfs.trg_mk_geom();
