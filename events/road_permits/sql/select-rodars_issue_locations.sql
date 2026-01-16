--this select query is used to select issue locations from ITSC database in rodars_pull pipeline.

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
        MAX(timestamputc) >= TIMEZONE('America/Toronto', {start}::date) AT TIME ZONE 'UTC' -- noqa: PRS, LT02
        AND MAX(timestamputc) < TIMEZONE('America/Toronto', {start}::date + 1) AT TIME ZONE 'UTC' -- noqa: PRS
)

SELECT
    i.divisionid,
    i.issueid,
    i.timestamputc,
    --Old rodars data doesn't have this value
    COALESCE(iln.locationindex, 0) AS locationindex,
    ilnt.mainroadname,
    ilnt.fromroadname,
    ilnt.toroadname,
    iln.direction AS direction_toplevel,
    iln.lanesaffected,
    iln.geometry,
    iln.streetnumber,
    iln.locationtype,
    iln.groupid::integer,
    ilnt.groupdescription
--Note there are multiple locations for each issue (unique locationindex)
FROM public.issuelocationnew AS iln
JOIN issues AS i USING (divisionid, issueid, timestamputc)
JOIN public.issuelocationnewtranslation AS ilnt USING (issueid, timestamputc, locationindex)