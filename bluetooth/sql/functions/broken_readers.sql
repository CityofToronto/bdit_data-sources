-- FUNCTION: bluetooth.broken_readers(date)

-- DROP FUNCTION bluetooth.broken_readers(date);


CREATE OR REPLACE FUNCTION bluetooth.broken_readers(_dt date)
    RETURNS TABLE(read_id integer, reader_name character varying) 
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE 
    ROWS 1000

AS $BODY$	
	BEGIN
    RETURN query 


SELECT          reader_id, read_name 

FROM            bluetooth.reader_status_history
INNER join      bluetooth.detectors_history_corrected using(reader_id) -- needs to be changed to something else or another version
WHERE           active IS FALSE AND last_active_date = _dt - 2 AND dt = _dt
END; 

$BODY$;

ALTER FUNCTION bluetooth.broken_readers(date)
    OWNER TO bt_admins;

COMMENT ON FUNCTION bluetooth.broken_readers(date) IS 'This function identifies the readers that were not active yesterday but were active the day before as broken readers, and returns a list of broken readers.';

GRANT EXECUTE ON FUNCTION bluetooth.broken_readers(date) TO PUBLIC;

GRANT EXECUTE ON FUNCTION bluetooth.broken_readers(date) TO bt_bot;
