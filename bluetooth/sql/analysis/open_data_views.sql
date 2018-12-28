DO $do$
DECLARE
	yyyy TEXT;
	baseviewname TEXT := 'bluetooth_';
	viewname TEXT;
BEGIN

	for yyyy IN 2014..2019 LOOP
        viewname:= baseviewname||yyyy;
        EXECUTE format($$CREATE OR REPLACE VIEW open_data.%I AS
                        SELECT segment_name::varchar AS "resultId",
                            aggr_5min.tt::integer AS "timeInSeconds",
                            aggr_5min.obs AS count,
                            replace(to_char(timezone('America/Toronto'::text, aggr_5min.datetime_bin) + '00:05:00'::interval, 'YYYY-MM-DD HH24:MI:SSOF'::text), ' '::text, 'T'::text) AS updated
                        FROM bluetooth.aggr_5min
                        JOIN bluetooth.segments USING (analysis_id)
                        WHERE date_part('year'::text, aggr_5min.datetime_bin) = %L::double precision;
                        $$
                        , viewname, yyyy);
        EXECUTE format($$GRANT SELECT ON TABLE open_data.%I TO od_extract_svc;
                        GRANT SELECT ON TABLE open_data.%I TO bdit_humans;
                        $$, viewname, viewname, viewname);
        
	END LOOP;
END;
$do$ LANGUAGE plpgsql