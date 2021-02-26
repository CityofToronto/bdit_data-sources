-- FUNCTION: mohan.broken_readers(date)

CREATE OR REPLACE FUNCTION mohan.broken_readers(
	insert_dt date)
    RETURNS void
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE 
AS $BODY$	
	begin
		with x as (
select reader_id, last_active_date as active_date, active as active_two_days_before, dt as check_date
from mohan.reader_status_history
where dt = insert_dt - 1 and active IS TRUE
)
,y as (
select *
from mohan.reader_status_history
left join x using (reader_id)
where mohan.reader_status_history.active IS false and (mohan.reader_status_history.dt = insert_dt and last_active_date = insert_dt-2)

)
INSERT INTO mohan.broken_readers_log(reader_id, reader_name, check_date)
select reader_id, name, dt
from y
left join reader_locations using(reader_id)
where active_two_days_before IS TRUE and active IS false
;
end; $BODY$;

ALTER FUNCTION mohan.broken_readers(date)
    OWNER TO mohan;
