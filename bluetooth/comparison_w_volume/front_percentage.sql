SELECT datetime_bin::TIME, AVG(obs) AS obs, AVG(volume) AS volume FROM(
	SELECT bt.hourly AS datetime_bin, bt.obs, mio.volume  FROM (
			SELECT intersection_uid, date_trunc('hour', datetime_bin) as hourly, leg, dir, sum(volume) as volume 
			FROM miovision.volumes_15min
			WHERE (intersection_uid = 6  AND leg = 'W' AND dir = 'EB' 
			AND datetime_bin::date in ('2017-11-01', '2017-11-02', '2017-11-03','2017-11-06','2017-11-07') and classification_uid in (1,4,5))
			group by intersection_uid, hourly, leg, dir) mio, 

			(SELECT date_trunc('hour', datetime_bin) as hourly, sum(obs) as obs FROM bluetooth.aggr_15min
			WHERE analysis_id = 1454523
			AND datetime_bin::date in ('2017-11-01', '2017-11-02', '2017-11-03','2017-11-06','2017-11-07')
			GROUP BY hourly) bt

	WHERE mio.hourly = bt.hourly) final
	GROUP BY datetime_bin::time;