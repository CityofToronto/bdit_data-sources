SELECT
    COUNT(DISTINCT divisionid::text || issueid::text)
    = {{ti.xcom_pull(key='return_value', task_ids='data_checks.check_src_issue_count')[1]}} AS "_check",
    'Bigdata count: ' || TO_CHAR(COUNT(DISTINCT divisionid::text || issueid::text), '999,999,999,999,999')
    || ', ITSC count: '
    || TO_CHAR({{ti.xcom_pull(key='return_value', task_ids='data_checks.check_src_issue_count')[1]}}, '999,999,999,999,999')
    AS description
FROM congestion_events.rodars_issues;
