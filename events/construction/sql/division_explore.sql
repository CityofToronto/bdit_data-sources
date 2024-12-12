    SELECT DISTINCT ON (divisionid)
        divisionid,
		datadivision.shortname,
        issueid,
        timestamputc,
        issuetype,
        issuedata.description,
        priority,
        proposedstarttimestamputc,
        proposedendtimestamputc,
        earlyendtimestamputc,
        status,
        timeoption
    FROM public.issuedata
	JOIN public.datadivision USING (divisionid)
    /*WHERE
        divisionid IN (
            8048, --rodars new
            8014, --rodars (old)
            8023 --TMMS TM3 Planned Work
        )*/
    ORDER BY divisionid, timestamputc, issueid DESC
