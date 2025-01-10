WITH issues AS (
    --select the most recent version of each issue
    SELECT
        divisionid,
        issueid,
        MAX(timestamputc) AS timestamputc
    FROM public.issuedata
    WHERE
        divisionid IN (
            8048, --rodars new
            8014 --rodars (old)
        )
    GROUP BY
        divisionid,
        issueid
    HAVING
        MAX(timestamputc) >= {start}::date -- noqa: PRS, LT02
        AND MAX(timestamputc) < {start}::date + interval '1 day' -- noqa: PRS
)

SELECT
    i.divisionid,
    i.issueid,
    i.timestamputc,
    --Old rodars data doesn't have this value
    COALESCE(iln.locationindex, 0) AS locationindex,
    iln.mainroadname,
    iln.fromroadname,
    iln.toroadname,
    iln.direction AS direction_toplevel,
    iln.lanesaffected,
    iln.geometry,
    iln.streetnumber,
    iln.locationtype,
    iln.groupid::integer,
    iln.groupdescription
--Note there are multiple locations for each issue (unique locationindex)
FROM public.issuelocationnew AS iln
JOIN issues AS i USING (divisionid, issueid, timestamputc)
