-- FUNCTION: here.generate_monthly_tti(text)

-- DROP FUNCTION here.generate_monthly_tti(text);

CREATE OR REPLACE FUNCTION here.generate_monthly_tti(
	yyyymm text)
    RETURNS integer
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE STRICT 
AS $BODY$

DECLARE 
	_tablename TEXT := 'ta_' || yyyymm;

BEGIN
	EXECUTE FORMAT($$
		WITH free_flow AS(SELECT link_dir, 3.6 * length / PERCENTILE_CONT(0.95) WITHIN GROUP(ORDER BY A.pct_50) AS free_flow_tt
						  FROM here.%I A
						  GROUP BY link_dir, "length"
						 )
				   
		INSERT INTO here.monthly_hourly_tti

		SELECT 		A.link_dir, 
				date_trunc('month', A.tx) AS mth,
				EXTRACT(HOUR from A.tx) as hh,
				3.6 * length * AVG(1.0/A.pct_50) AS tt_avg,
				3.6 * length / (PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY A.pct_50)) AS tt_med,
				3.6 * length * AVG(1.0/A.pct_50) / free_flow_tt  AS tti_avg,
				COUNT(A.pct_50) AS obs
		FROM		here.%I A 
		INNER JOIN free_flow USING (link_dir)
		LEFT JOIN	ref.holiday E ON A.tx >= e.dt AND A.tx < e.dt + INTERVAL '1 day'

		WHERE		EXTRACT(isodow FROM A.tx) < 6
				AND E.holiday IS NULL
		GROUP BY 	A.link_dir,
				mth,
				hh, free_flow_tt,
				"length"
		$$, _tablename, _tablename);
	RETURN 1;
END;

$BODY$;

ALTER FUNCTION here.generate_monthly_tti(text)
    OWNER TO rdumas;

GRANT EXECUTE ON FUNCTION here.generate_monthly_tti(text) TO rdumas;

GRANT EXECUTE ON FUNCTION here.generate_monthly_tti(text) TO here_admins;

