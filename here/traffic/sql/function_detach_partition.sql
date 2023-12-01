-- Detach partitioned tables in here.ta 
-- input yyyy text
CREATE OR REPLACE FUNCTION here.detach_partition_ta(
    yyyy text)
    RETURNS void
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE STRICT SECURITY DEFINER PARALLEL UNSAFE
AS $BODY$

DECLARE 
    startdate DATE;
    enddate DATE;
    yyyymm TEXT;
    basetablename TEXT := 'ta_';
    tablename TEXT;

BEGIN
    FOR mm IN 01..12 LOOP
        startdate:= to_date(yyyy||'-'||mm||'-01', 'YYYY-MM-DD');
        enddate:= startdate + INTERVAL '1 month';
        IF mm < 10 THEN
            yyyymm:= yyyy||'0'||mm;
        ELSE
            yyyymm:= yyyy||''||mm;
        END IF;
        tablename:= basetablename||yyyymm;
        EXECUTE format($$ALTER TABLE here.ta DETACH PARTITION here.%I ;
                       $$, tablename);
    END LOOP;
END;
$BODY$;

ALTER FUNCTION here.detach_partition_ta(text)
    OWNER TO here_admins;