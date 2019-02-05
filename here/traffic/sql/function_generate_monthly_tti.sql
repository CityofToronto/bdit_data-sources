-- FUNCTION: here.generate_monthly_tti(text)

 DROP FUNCTION here.generate_monthly_tti(text);

CREATE OR REPLACE FUNCTION here.generate_monthly_tti(
	yyyymm text)
    RETURNS VOID
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE STRICT 
AS $BODY$

DECLARE 
	_tablename TEXT := 'ta_' || yyyymm;

BEGIN
	DROP TABLE IF EXISTS avgs ;
	EXECUTE FORMAT($$
		
		CREATE TEMP TABLE avgs AS
		
		SELECT 		A.link_dir, 
				date_trunc('month', A.tx) AS mth,
				EXTRACT(HOUR from A.tx) as hh,
				3.6 * length * AVG(1.0/A.pct_50) AS tt_avg,
				3.6 * length / (PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY A.pct_50)) AS tt_med,
				COUNT(A.pct_50) AS obs
		
		FROM		here.%I A 
		LEFT JOIN	ref.holiday E ON A.tx >= e.dt AND A.tx < e.dt + INTERVAL '1 day'

		WHERE		EXTRACT(isodow FROM A.tx) < 6
				AND E.holiday IS NULL
		GROUP BY 	A.link_dir,
				mth,
				hh, 
				"length";
	$$, _tablename);
	CREATE INDEX ON avgs USING btree(link_dir);
	ANALYZE avgs;
	INSERT INTO here.monthly_hourly_tti

	SELECT link_dir, mth, hh, tt_avg, tt_med, tt_avg / free_flow_tt AS tti_avg, obs, free_flow_tt
	FROM avgs
	INNER JOIN (SELECT link_dir, percentile_cont(0.05) WITHIN GROUP (ORDER BY tt_avg) AS free_flow_tt FROM avgs GROUP BY link_dir) AS free_flow USING (link_dir);
		
	
		
		
	RETURN ;
END;

$BODY$;

ALTER FUNCTION here.generate_monthly_tti(text)
    OWNER TO rdumas;

GRANT EXECUTE ON FUNCTION here.generate_monthly_tti(text) TO rdumas;

GRANT EXECUTE ON FUNCTION here.generate_monthly_tti(text) TO here_admins;

