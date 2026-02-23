SELECT
    True AS "_check",
    COUNT(DISTINCT divisionid::text || issueid::text) AS issue_count
FROM public.issuedata
WHERE
    --there could be issues created right after we pulled them
    timestamputc < TIMEZONE('America/Toronto', '{{ ds }}'::date + 1) AT TIME ZONE 'UTC'
    AND divisionid IN (
        8048, --rodars new
        8014 --rodars (old)
    );
