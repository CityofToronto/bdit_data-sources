--WIP. May need to delete outdated versions of each issue.

WITH newest_timestamps AS (
    SELECT DISTINCT ON (divisionid, issueid)
        divisionid,
        issueid,
        timestamputc
    FROM congestion_events.itsc_issues
    ORDER BY
        divisionid,
        issueid,
        timestamputc DESC
)

SELECT *
FROM congestion_events.itsc_issues
LEFT JOIN newest_timestamps USING (divisionid, issueid, timestamputc)
WHERE newest_timestamps.timestamputc IS NULL
