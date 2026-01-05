CREATE OR REPLACE FUNCTION gwolofs.partition_yyyy(
    base_table text,
    year_ integer,
    partition_owner text,
    schema_ text
)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
COST 100
VOLATILE PARALLEL UNSAFE

AS $BODY$

DECLARE
    year_table TEXT := base_table || '_' || year_::text;
    startdate DATE := (year_::text || '-01-01')::date;

BEGIN

    EXECUTE FORMAT(
        $SQL$
            CREATE TABLE IF NOT EXISTS %1$I.%2$I
            PARTITION OF %1$I.%3$I
            FOR VALUES FROM (%4$L) TO (%4$L::date + interval '1 year');
            ALTER TABLE IF EXISTS %1$I.%2$I OWNER TO %5$I;
        $SQL$,
        schema_,
        year_table,
        base_table,
        startdate,
        partition_owner
    );

END;
$BODY$;

COMMENT ON FUNCTION gwolofs.partition_yyyy(text, integer, text, text) IS
'Create new partition by year under the parent table `base_table`.
Can be used accross schemas when partitioning by year. 
Example: SELECT gwolofs.partition_yyyy(base_table := ''congestion_raw_segments'', year_ := 2026, partition_owner := ''gwolofs'', schema_ := ''gwolofs'')';

ALTER FUNCTION gwolofs.partition_yyyy(text, integer, text, text) OWNER TO gwolofs;

GRANT EXECUTE ON FUNCTION gwolofs.partition_yyyy(text, integer, text, text) TO congestion_bot;