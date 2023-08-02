DELETE FROM vds.counts_15min
WHERE
    datetime_15min >= '{{ ds }} 00:00:00'::timestamp
    AND datetime_15min < '{{ ds }} 00:00:00'::timestamp + interval '1 DAY'