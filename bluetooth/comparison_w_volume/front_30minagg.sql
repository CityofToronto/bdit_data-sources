SELECT bt.thirtymins as datetime_bin, bt.obs, mio.volume  

FROM (
	SELECT intersection_uid, date_trunc('hour', datetime_bin)+ date_part('minute', datetime_bin)::int / 30 * interval '30 min'  as thirtymins, leg, dir, sum(volume) as volume 
	FROM miovision.volumes_15min
	WHERE (intersection_uid = 6  AND leg = 'W'AND dir = 'EB' 
	AND datetime_bin::date in ('2017-11-01', '2017-11-02', '2017-11-03','2017-11-06','2017-11-07') AND classification_uid in (1,4,5))
	GROUP BY intersection_uid, thirtymins, leg, dir) mio, 

	(SELECT date_trunc('hour', datetime_bin)+ date_part('minute', datetime_bin)::int / 30 * interval '30 min'  as thirtymins, sum(obs) as obs 
	FROM bluetooth.aggr_15min
	WHERE analysis_id = 1454523
	AND datetime_bin::date in ('2017-11-01', '2017-11-02', '2017-11-03','2017-11-06','2017-11-07')
	GROUP BY thirtymins) bt

WHERE mio.thirtymins = bt.thirtymins;