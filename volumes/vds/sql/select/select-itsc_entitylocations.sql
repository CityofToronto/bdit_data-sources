SELECT 
    e.divisionid,
    e.entitytype,
    e.entityid,
    ts.location_timestamp AS start_timestamp,
    LEAD(ts.location_timestamp, 1) OVER (
        PARTITION BY e.divisionid, e.entityid ORDER BY ts.location_timestamp
    ) AS end_timestamp,
    e.latitude,
    e.longitude,
    e.altitudemetersasl,
    e.headingdegrees,
    e.speedkmh,
    e.numsatellites,
    e.dilutionofprecision,
    e.mainroadid,
    e.crossroadid,
    e.secondcrossroadid,
    e.mainroadname,
    e.crossroadname,
    e.secondcrossroadname,
    e.streetnumber,
    e.offsetdistancemeters,
    e.offsetdirectiondegrees,
    e.locationsource,
    e.locationdescriptionoverwrite
FROM public.entitylocation AS e,
    LATERAL(
        SELECT TIMEZONE('UTC', e.locationtimestamputc) AT TIME ZONE 'EST5EDT' AS location_timestamp
    ) AS ts
WHERE e.divisionid IN (2, 8001) --only these have data in 'vdsdata' table
ORDER BY
    e.divisionid,
    e.entityid,
    start_timestamp
