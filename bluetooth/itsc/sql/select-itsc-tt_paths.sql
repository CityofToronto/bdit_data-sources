SELECT
    divisionid,
    pathid,
    sourceid,
    algorithm,
    firstfeaturestartoffsetmeters,
    lastfeatureendoffsetmeters,
    firstfeatureforward,
    rawdatatypes,
    externaldataorigin,
    externaldatadestination,
    externaldatawaypoints,
    timeadjustfactor,
    timeadjustconstantseconds,
    queuemaxspeedkmh,
    minimaldelayspeedkmh,
    majordelayspeedkmh,
    severedelayspeedkmh,
    useminimumspeed,
    lengthmeters,
    timezone('UTC', starttimestamputc) AT TIME ZONE 'Canada/Eastern' AS starttimestamp,
    timezone('UTC', endtimestamputc) AT TIME ZONE 'Canada/Eastern' AS endtimestamp,
    featurespeeddivisionid,
    severedelayissuedivisionid,
    majordelayissuedivisionid,
    minordelayissuedivisionid,
    queueissuedivisionid,
    queuedetectionclearancespeedkmh,
    severedelayclearancespeedkmh,
    pathtype,
    pathdatatimeoutforissuecreationseconds,
    encodedpolyline
FROM public.traveltimepathconfig
WHERE
    divisionid IN (
        2, --RESCU (Stinson)
        8046, --miovision tt
        8026 --TPANA Bluetooth
    )
LIMIT 1000;