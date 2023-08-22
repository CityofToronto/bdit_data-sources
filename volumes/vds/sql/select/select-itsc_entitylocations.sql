SELECT 
    divisionid,
    entitytype,
    entityid,
    location_timestamp AS start_timestamp,
	LEAD(location_timestamp, 1) OVER (
		PARTITION BY divisionid, entityid ORDER BY location_timestamp
	) AS end_timestamp,
    latitude,
    longitude,
    altitudemetersasl,
    headingdegrees,
    speedkmh,
    numsatellites,
    dilutionofprecision,
    mainroadid,
    crossroadid,
    secondcrossroadid,
    mainroadname,
    crossroadname,
    secondcrossroadname,
    streetnumber,
    offsetdistancemeters,
    offsetdirectiondegrees,
    locationsource,
    locationdescriptionoverwrite
FROM public.entitylocation,
	LATERAL (
        SELECT TIMEZONE('UTC', locationtimestamputc) AT TIME ZONE 'EST5EDT' AS location_timestamp
    ) AS ts
WHERE divisionid IN (2, 8001) --only these have data in 'vdsdata' table
ORDER BY
	divisionid,
    entityid,
    start_timestamp
