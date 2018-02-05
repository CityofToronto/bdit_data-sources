select datetime_bin::time, avg(obs) as obs, avg(volume) as volume from(
select bt.hourly as datetime_bin, bt.obs, mio.volume  from (
select intersection_uid, date_trunc('hour', datetime_bin) as hourly, leg, dir, sum(volume) as volume from miovision.volumes_15min
where (intersection_uid = 6  and leg = 'W' and dir = 'EB' 
and datetime_bin::date >= '2017-11-01' and datetime_bin::date <= '2017-11-08' and classification_uid in (1,4,5))

group by intersection_uid, hourly, leg, dir) mio, 

(select date_trunc('hour', datetime_bin) as hourly, sum(obs) as obs from bluetooth.aggr_15min
where analysis_id = 1454523
and datetime_bin::date >= '2017-11-01' and datetime_bin::date <= '2017-11-08'
group by hourly) bt

where mio.hourly = bt.hourly) final
group by datetime_bin::time;