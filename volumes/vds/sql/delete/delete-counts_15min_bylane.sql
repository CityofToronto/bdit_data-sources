DELETE FROM vds.counts_15min_bylane
WHERE
    datetime_15min >= '{{ ds }} 00:00:00'::timestamp -- noqa: TMP
    AND datetime_15min < '{{ ds }} 00:00:00'::timestamp + interval '1 DAY' -- noqa: TMP