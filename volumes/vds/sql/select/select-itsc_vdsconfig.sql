SELECT 
    divisionid,
    vdsid,
    UPPER(sourceid) AS detector_id, --match what we already have
    TIMEZONE('UTC', starttimestamputc) AT TIME ZONE 'EST5EDT' AS starttimestamp,
    TIMEZONE('UTC', endtimestamputc) AT TIME ZONE 'EST5EDT' AS endtimestamp,
    lanes,
    hasgpsunit,
    managementurl,
    description,
    fssdivisionid,
    fssid,
    rtmsfromzone,
    rtmstozone,
    detectortype,
    createdby,
    createdbystaffid,
    signalid,
    signaldivisionid,
    movement
FROM public.vdsconfig
WHERE divisionid IN (2, 8001) --only these have data in 'vdsdata' table