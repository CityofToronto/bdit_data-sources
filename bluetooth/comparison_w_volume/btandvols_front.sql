select bt.datetime_bin, bt.obs, mio.v as volume from (
select intersection_uid, datetime_bin, leg, dir, sum(volume) as v from miovision.volumes_15min
where (intersection_uid = 6  and leg = 'W' and dir = 'EB' 
and datetime_bin::date in ('2017-11-01', '2017-11-02', '2017-11-03','2017-11-06','2017-11-07') and classification_uid in (1,4,5))

group by intersection_uid, datetime_bin, leg, dir
order by intersection_uid, datetime_bin) mio, 

(select datetime_bin, obs from bluetooth.aggr_15min
where analysis_id = 1454523
and datetime_bin::date in ('2017-11-01', '2017-11-02', '2017-11-03','2017-11-06','2017-11-07')) bt

where mio.datetime_bin = bt.datetime_bin;