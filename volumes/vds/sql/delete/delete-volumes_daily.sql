DELETE FROM vds.volumes_daily
WHERE dt = '{{ ds }}'::date -- noqa: TMP
