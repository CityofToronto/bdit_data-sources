DELETE FROM vds.volumes_daily
WHERE
    datetime_15min >= '{{ ds }} 00:00:00'::timestamp -- noqa: TMP
    AND datetime_15min < '{{ ds }} 00:00:00'::timestamp + interval '1 DAY'; -- noqa: TMP
