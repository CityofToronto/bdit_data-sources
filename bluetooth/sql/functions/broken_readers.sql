-- FUNCTION: bluetooth.broken_readers(date)

-- DROP FUNCTION bluetooth.broken_readers(date);

CREATE OR REPLACE FUNCTION bluetooth.broken_readers(
	insert_dt date)
    RETURNS TABLE(read_id integer, reader_name character varying) 
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE 
    ROWS 1000
AS $BODY$	
	begin
    return query 
    with x as(
select reader_id, last_active_date as yesterday, active as active_yesterday
	from bluetooth.reader_status_history
where last_active_date = insert_dt::date - 2
order by reader_id
) 
,y as (
select reader_id, last_active_date as today, active as active_today
	from bluetooth.reader_status_history
where last_active_date = insert_dt::date - 1 and active is false
order by reader_id
)
select distinct (reader_id), read_name from x 
left join y using(reader_id)
left join bluetooth.detectors_history_corrected using(reader_id)
where active_yesterday is true and active_today is false
;
end; $BODY$;

ALTER FUNCTION bluetooth.broken_readers(date)
    OWNER TO bluetooth;

GRANT EXECUTE ON FUNCTION bluetooth.broken_readers(date) TO PUBLIC;

GRANT EXECUTE ON FUNCTION bluetooth.broken_readers(date) TO bluetooth;
