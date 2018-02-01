select bt.thirtymins as datetime_bin, bt.obs, mio.volume  from (
select intersection_uid, date_trunc('hour', datetime_bin)+ date_part('minute', datetime_bin)::int / 30 * interval '30 min'  as thirtymins, leg, dir, sum(volume) as volume from miovision.volumes_15min
where (intersection_uid = 6  and leg = 'W' and dir = 'EB' 
and datetime_bin::date >= '2017-11-01' and datetime_bin::date <= '2017-11-08' and classification_uid in (1,4,5))

group by intersection_uid, thirtymins, leg, dir) mio, 

(select date_trunc('hour', datetime_bin)+ date_part('minute', datetime_bin)::int / 30 * interval '30 min'  as thirtymins, sum(obs) as obs from bluetooth.aggr_15min
where analysis_id = 1454523
and datetime_bin::date >= '2017-11-01' and datetime_bin::date <= '2017-11-08'
group by thirtymins) bt

where mio.thirtymins = bt.thirtymins;