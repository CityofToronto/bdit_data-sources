SELECT
    bt.hourly AS datetime_bin,
    bt.obs,
    mio.volume

FROM (

    SELECT
        intersection_uid,
        leg,
        dir,
        date_trunc('hour', datetime_bin) AS hourly,
        sum(volume) AS volume
    FROM miovision.volumes_15min
    WHERE (
        intersection_uid = 6 AND leg = 'W' AND dir = 'EB'
        AND datetime_bin::date IN (
            '2017-11-01', '2017-11-02', '2017-11-03', '2017-11-06', '2017-11-07'
        )
        AND classification_uid IN (1, 4, 5)
    )

    GROUP BY intersection_uid, hourly, leg, dir
) AS mio,

    (
        SELECT
            date_trunc('hour', datetime_bin) AS hourly,
            sum(obs) AS obs
        FROM bluetooth.aggr_15min
        WHERE
            analysis_id = 1454523
            AND datetime_bin::date IN (
                '2017-11-01', '2017-11-02', '2017-11-03', '2017-11-06', '2017-11-07'
            )
        GROUP BY hourly
    ) AS bt

WHERE mio.hourly = bt.hourly;