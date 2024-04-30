SELECT
    divisionid,
    deviceid,
    sourceid,
    TIMEZONE('UTC', starttimestamputc) AT TIME ZONE 'EST5EDT' AS starttimestamp,
    TIMEZONE('UTC', endtimestamputc) AT TIME ZONE 'EST5EDT' AS endtimestamp,
    hasgpsunit,
    devicetype,
    description
FROM public.commdeviceconfig
WHERE divisionid IN (2, 8001) --only these have data in 'vdsdata' table

/*columns excluded:
managementurl, diagnosticsaddress, diagnosticsport, password, username, manufacturerserialnumber,
networkserialnumber, phonenumber, lanmacaddress, lanipaddress, lansubnetmask, dhcpserverenabled, dmzaddress,
keepaliveaddress, createdby, createdbystaffid
*/